import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { DuckDBInstance } from '@duckdb/node-api';
import { execute, detectSearchMode } from '../src/query/engine.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES_DIR = path.join(__dirname, '_fixtures');
const DB_PATH = path.join(FIXTURES_DIR, 'test_engine.duckdb');

let instance;
let conn;

const TENANT = 'test-tenant';
const TABLE_NAME = 'test_tenant';
const SEG1 = 'seg_file1';
const SEG2 = 'seg_file2';

async function createFixtures() {
  await mkdir(FIXTURES_DIR, { recursive: true });

  // Create parquet fixtures using a temp in-memory DuckDB
  const tmpInstance = await DuckDBInstance.create(':memory:');
  const tmpConn = await tmpInstance.connect();

  const file1 = path.join(FIXTURES_DIR, 'file1.parquet');
  const file2 = path.join(FIXTURES_DIR, 'file2.parquet');

  await tmpConn.run(`
    COPY (
      SELECT
        1705276800000 + (i * 1000) AS "timestamp",
        CASE WHEN i % 3 = 0 THEN 'auth' ELSE 'api' END AS service,
        CASE WHEN i % 5 = 0 THEN 'error' ELSE 'info' END AS level,
        'host-' || lpad(((i % 5) + 1)::VARCHAR, 3, '0') AS host,
        uuid()::VARCHAR AS trace_id,
        CASE
          WHEN i % 5 = 0 THEN 'Connection refused to database host=db-01 port=5432'
          WHEN i % 7 = 0 THEN 'Request timeout after 3000ms to upstream auth'
          ELSE 'Request completed successfully status=200 duration=' || (i % 500) || 'ms'
        END AS message,
        CASE WHEN i % 5 = 0 THEN 500 ELSE 200 END AS status_code,
        (i % 2000)::DOUBLE + 0.5 AS duration_ms,
        CASE WHEN i % 2 = 0 THEN '/api/users' ELSE '/api/orders' END AS request_path
      FROM generate_series(0, 999) AS t(i)
    ) TO '${file1}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500)
  `);

  await tmpConn.run(`
    COPY (
      SELECT
        1705280400000 + (i * 1000) AS "timestamp",
        'web' AS service,
        'info' AS level,
        'host-010' AS host,
        uuid()::VARCHAR AS trace_id,
        'Page rendered in ' || (i % 300) || 'ms for path /products/' || (i % 50) AS message,
        200 AS status_code,
        (i % 300)::DOUBLE + 1.0 AS duration_ms,
        '/api/products/search' AS request_path
      FROM generate_series(0, 499) AS t(i)
    ) TO '${file2}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500)
  `);

  tmpConn.closeSync();

  // Create the persistent test DB and ingest fixtures into a tenant table
  instance = await DuckDBInstance.create(DB_PATH);
  conn = await instance.connect();

  // Create tenant table from file1
  await conn.run(
    `CREATE TABLE ${TABLE_NAME} AS SELECT *, '${SEG1}' AS _segment FROM read_parquet('${file1}')`
  );

  // Append file2
  await conn.run(
    `INSERT INTO ${TABLE_NAME} SELECT *, '${SEG2}' AS _segment FROM read_parquet('${file2}')`
  );
}

describe('detectSearchMode', () => {
  it('should detect structured mode', () => {
    assert.equal(detectSearchMode({ service: 'auth' }, null), 'structured');
    assert.equal(detectSearchMode({ level: ['error', 'fatal'] }, null), 'structured');
    assert.equal(detectSearchMode({}, null), 'structured');
    assert.equal(detectSearchMode({}, undefined), 'structured');
  });

  it('should detect wildcard mode', () => {
    assert.equal(detectSearchMode({ request_path: '/api/users/*' }, null), 'wildcard');
    assert.equal(detectSearchMode({ host: 'host-0??' }, null), 'wildcard');
  });

  it('should detect basic search mode', () => {
    assert.equal(detectSearchMode({}, 'connection timeout'), 'basic');
    assert.equal(detectSearchMode({ service: 'auth' }, 'timeout'), 'basic');
  });
});

describe('execute', () => {
  before(async () => {
    await createFixtures();
  });

  after(async () => {
    if (conn) conn.closeSync();
    instance = null;
    await rm(FIXTURES_DIR, { recursive: true, force: true });
  });

  describe('structured queries', () => {
    it('should filter by equality', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: { service: 'auth' },
        limit: 50,
      });

      assert.equal(result.searchMode, 'structured');
      assert.ok(result.rows.length > 0);
      for (const row of result.rows) {
        assert.equal(row.service, 'auth');
      }
    });

    it('should filter by IN (array)', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: { level: ['error', 'info'] },
        limit: 50,
      });

      for (const row of result.rows) {
        assert.ok(['error', 'info'].includes(row.level));
      }
    });

    it('should filter by range', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: { duration_ms: { gt: 500 } },
        limit: 50,
      });

      assert.ok(result.rows.length > 0);
      for (const row of result.rows) {
        assert.ok(Number(row.duration_ms) > 500);
      }
    });

    it('should query across multiple segments', async () => {
      const result = await execute(conn, TENANT, [SEG1, SEG2], {
        filters: {},
        limit: 10,
      });

      assert.equal(result.filesScanned, 2);
      assert.equal(result.rows.length, 10);
    });

    it('should respect limit and offset', async () => {
      const result1 = await execute(conn, TENANT, [SEG1], { filters: {}, limit: 5, offset: 0 });
      const result2 = await execute(conn, TENANT, [SEG1], { filters: {}, limit: 5, offset: 5 });

      assert.equal(result1.rows.length, 5);
      assert.equal(result2.rows.length, 5);
      assert.notDeepEqual(result1.rows[0], result2.rows[0]);
    });
  });

  describe('wildcard queries', () => {
    it('should filter with GLOB pattern', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: { request_path: '/api/users*' },
        limit: 50,
      });

      assert.equal(result.searchMode, 'wildcard');
      assert.ok(result.rows.length > 0);
      for (const row of result.rows) {
        assert.ok(row.request_path.startsWith('/api/users'));
      }
    });

    it('should filter with ? single-char wildcard', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: { host: 'host-00?' },
        limit: 50,
      });

      assert.equal(result.searchMode, 'wildcard');
      assert.ok(result.rows.length > 0);
      for (const row of result.rows) {
        assert.ok(row.host.startsWith('host-00'), `Expected host-00x, got ${row.host}`);
        assert.equal(row.host.length, 8);
      }
    });
  });

  describe('basic search', () => {
    it('should return results matching search term', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: {},
        search: 'connection',
        limit: 20,
      });

      assert.equal(result.searchMode, 'basic');
      assert.ok(result.rows.length > 0);
      for (const row of result.rows) {
        assert.ok(row.message.toLowerCase().includes('connection'), 'Message should contain search term');
      }
    });

    it('should order results by timestamp desc', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: {},
        search: 'status',
        limit: 20,
      });

      const timestamps = result.rows.map((r) => Number(r.timestamp));
      for (let i = 1; i < timestamps.length; i++) {
        assert.ok(timestamps[i] <= timestamps[i - 1], `Timestamps not descending: ${timestamps[i]} > ${timestamps[i - 1]}`);
      }
    });

    it('should combine basic search with structured filters', async () => {
      const result = await execute(conn, TENANT, [SEG1], {
        filters: { service: 'auth' },
        search: 'connection',
        limit: 20,
      });

      assert.equal(result.searchMode, 'basic');
      for (const row of result.rows) {
        assert.equal(row.service, 'auth');
        assert.ok(row.message.toLowerCase().includes('connection'));
      }
    });
  });
});
