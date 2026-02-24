import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { DuckDBInstance } from '@duckdb/node-api';
import { buildFileBloom } from '../src/bloom/bloom.js';
import { ColdStorage } from '../src/storage/coldStorage.js';
import { DuckDbCache } from '../src/storage/cache.js';
import { QueryPlanner } from '../src/query/planner.js';
import { execute } from '../src/query/engine.js';
import { load as loadManifest, clearManifestCache } from '../src/manifest/manifest.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_COLD = path.join(__dirname, '_integration_cold');
const TEST_CACHE_DIR = path.join(__dirname, '_integration_cache');
const TEST_DB_PATH = path.join(TEST_CACHE_DIR, 'test.duckdb');

async function generateTestData() {
  await rm(TEST_COLD, { recursive: true, force: true });
  await rm(TEST_CACHE_DIR, { recursive: true, force: true });
  await mkdir(TEST_CACHE_DIR, { recursive: true });

  const tenantId = 'tenant-1';
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  const manifest = { tenantId, files: [] };
  const baseTs = new Date('2025-01-15T00:00:00Z').getTime();

  // Generate 4 hourly files
  for (let h = 0; h < 4; h++) {
    const startTs = baseTs + h * 3600000;
    const endTs = startTs + 3599999;
    const segment = `2025-01-15_${String(h).padStart(2, '0')}`;

    const relDir = path.join(tenantId, 'year=2025', 'month=01', 'day=15');
    const absDir = path.join(TEST_COLD, relDir);
    await mkdir(absDir, { recursive: true });

    const filename = `hour=${String(h).padStart(2, '0')}.parquet`;
    const relPath = path.join(relDir, filename);
    const absPath = path.join(TEST_COLD, relPath);

    const service = h < 2 ? `CASE WHEN i % 2 = 0 THEN 'auth' ELSE 'api' END` : `CASE WHEN i % 2 = 0 THEN 'web' ELSE 'billing' END`;
    const level = h < 2 ? `CASE WHEN i % 10 = 0 THEN 'error' ELSE 'info' END` : `'info'`;

    await conn.run(`
      COPY (
        SELECT
          ${startTs} + (i * 3600) AS "timestamp",
          ${service} AS service,
          ${level} AS level,
          'host-' || lpad(((i % 3) + 1)::VARCHAR, 3, '0') AS host,
          uuid()::VARCHAR AS trace_id,
          CASE
            WHEN i % 10 = 0 THEN 'Connection timeout to database server after 5000ms'
            WHEN i % 7 = 0 THEN 'Request timeout upstream service unavailable'
            ELSE 'Request completed status=200 duration=' || i || 'ms'
          END AS message,
          CASE WHEN i % 10 = 0 THEN 500 ELSE 200 END AS status_code,
          (i % 1000)::DOUBLE + 0.5 AS duration_ms,
          CASE WHEN i % 3 = 0 THEN '/api/users' WHEN i % 3 = 1 THEN '/api/orders' ELSE '/api/products/search' END AS request_path
        FROM generate_series(0, 999) AS t(i)
      ) TO '${absPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500)
    `);

    // Build blooms
    const services = h < 2 ? ['auth', 'api'] : ['web', 'billing'];
    const levels = h < 2 ? ['error', 'info'] : ['info'];
    const bloom = buildFileBloom({
      service: services,
      level: levels,
      host: ['host-001', 'host-002', 'host-003'],
      status_code: h < 2 ? ['200', '500'] : ['200'],
    });

    const bloomDir = path.join(TEST_COLD, tenantId, '_bloom');
    await mkdir(bloomDir, { recursive: true });
    await writeFile(path.join(bloomDir, `${segment}.bloom.json`), JSON.stringify(bloom));

    manifest.files.push({ path: relPath, segment, startTs, endTs, rowCount: 1000, sizeBytes: 50000 });
  }

  await writeFile(path.join(TEST_COLD, tenantId, 'manifest.json'), JSON.stringify(manifest, null, 2));
  conn.closeSync();
}

describe('Integration', () => {
  let dbCache;

  before(async () => {
    await generateTestData();
    clearManifestCache();
  });

  after(async () => {
    if (dbCache) await dbCache.close();
    await rm(TEST_COLD, { recursive: true, force: true });
    await rm(TEST_CACHE_DIR, { recursive: true, force: true });
  });

  async function runQuery({ filters = {}, search, limit = 50 } = {}) {
    const tenantId = 'tenant-1';
    const coldStorage = new ColdStorage(TEST_COLD);

    if (!dbCache) {
      dbCache = new DuckDbCache(TEST_DB_PATH, TEST_COLD);
      await dbCache.init();
    }

    const planner = new QueryPlanner(coldStorage, dbCache);

    const manifest = await loadManifest(TEST_COLD, tenantId);
    const baseTs = new Date('2025-01-15T00:00:00Z').getTime();
    const startTs = baseTs;
    const endTs = baseTs + 4 * 3600000;

    const plan = await planner.plan(tenantId, manifest, { startTs, endTs, filters });

    dbCache.resetCounters();
    await dbCache.ensureCached(tenantId, plan.files);

    const conn = dbCache.getConnection();
    const segments = plan.files.map((f) => f.segment);
    const result = await execute(conn, tenantId, segments, { filters, search, limit, offset: 0, startTs, endTs });
    const cacheStats = dbCache.stats();

    return { ...result, planStats: plan.stats, cacheStats };
  }

  it('should execute a full structured query pipeline', async () => {
    const result = await runQuery({ filters: { service: 'auth', level: 'error' } });

    assert.equal(result.searchMode, 'structured');
    assert.ok(result.rows.length > 0, 'Should have results');
    for (const row of result.rows) {
      assert.equal(row.service, 'auth');
      assert.equal(row.level, 'error');
    }
  });

  it('should prune files via bloom filters', async () => {
    const result = await runQuery({ filters: { service: 'auth' } });

    // Hours 2-3 don't have 'auth' — bloom should prune them
    assert.equal(result.planStats.filesAfterTimeFilter, 4);
    assert.equal(result.planStats.filesAfterBloom, 2, 'Bloom should prune 2 of 4 files');
    assert.equal(result.filesScanned, 2);
  });

  it('should execute wildcard query', async () => {
    const result = await runQuery({ filters: { request_path: '/api/users*' } });

    assert.equal(result.searchMode, 'wildcard');
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.ok(row.request_path.startsWith('/api/users'));
    }
  });

  it('should execute basic search', async () => {
    const result = await runQuery({ search: 'connection' });

    assert.equal(result.searchMode, 'basic');
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.ok(row.message.toLowerCase().includes('connection'));
    }
  });

  it('should combine basic search with bloom-pruned structured filters', async () => {
    const result = await runQuery({
      filters: { service: 'auth' },
      search: 'timeout',
    });

    assert.equal(result.searchMode, 'basic');
    // Bloom should prune web/billing hours
    assert.equal(result.planStats.filesAfterBloom, 2);
    for (const row of result.rows) {
      assert.equal(row.service, 'auth');
      assert.ok(row.message.toLowerCase().includes('timeout'));
    }
  });

  it('should handle cache hits on second query', async () => {
    // First query — all misses (already cached from earlier tests, so actually hits)
    await runQuery({ filters: { service: 'api' } });
    // Second query — same files should be cached
    const result = await runQuery({ filters: { service: 'api' } });

    assert.ok(result.cacheStats.hits > 0, 'Should have cache hits on second query');
    assert.equal(result.cacheStats.misses, 0, 'Should have no misses on second query');
  });
});
