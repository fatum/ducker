import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { storeBloomToDb, loadBloomFromDb } from '../bloom/bloom.js';

function tenantTableName(tenantId) {
  return tenantId.replace(/[^a-zA-Z0-9]/g, '_');
}

function escapeSql(str) {
  return str.replace(/'/g, "''");
}

export class DuckDbCache {
  constructor(dbPath, coldStorageRoot) {
    this.dbPath = dbPath;
    this.coldStorageRoot = coldStorageRoot;
    this.instance = null;
    this.connection = null;
    this.hits = 0;
    this.misses = 0;
    this._knownTables = new Set();
  }

  async init() {
    this.instance = await DuckDBInstance.create(this.dbPath);
    this.connection = await this.instance.connect();

    await this.connection.run(`
      CREATE TABLE IF NOT EXISTS _cache_segments (
        tenant VARCHAR,
        segment VARCHAR,
        cached_at BIGINT,
        last_accessed BIGINT,
        fts_indexed_at BIGINT,
        next_row_id BIGINT,
        row_count INTEGER,
        PRIMARY KEY (tenant, segment)
      )
    `);

    await this.connection.run(`
      CREATE TABLE IF NOT EXISTS _bloom_cache (
        tenant VARCHAR,
        segment VARCHAR,
        column_name VARCHAR,
        filter_size INTEGER,
        hash_count INTEGER,
        bits BLOB,
        PRIMARY KEY (tenant, segment, column_name)
      )
    `);
  }

  getConnection() {
    return this.connection;
  }

  async close() {
    if (this.connection) {
      this.connection.closeSync();
      this.connection = null;
    }
    this.instance = null;
  }

  async ensureCached(tenant, files) {
    const tableName = tenantTableName(tenant);

    for (const file of files) {
      if (await this._isSegmentCached(tenant, file.segment)) {
        this.hits++;
        continue;
      }

      this.misses++;
      const coldPath = path.join(this.coldStorageRoot, file.path);

      const nextRowId = await this._getNextRowId(tenant);

      if (!this._knownTables.has(tableName)) {
        await this.connection.run(
          `CREATE TABLE IF NOT EXISTS ${tableName} AS SELECT *, 0::BIGINT AS _row_id, ''::VARCHAR AS _segment FROM read_parquet('${escapeSql(coldPath)}') WHERE false`
        );
        this._knownTables.add(tableName);
      }

      await this.connection.run(
        `INSERT INTO ${tableName} SELECT *, ${nextRowId} + row_number() OVER () AS _row_id, '${escapeSql(file.segment)}' AS _segment FROM read_parquet('${escapeSql(coldPath)}')`
      );

      const countResult = await this.connection.runAndReadAll(
        `SELECT count(*) AS cnt FROM read_parquet('${escapeSql(coldPath)}')`
      );
      const rowCount = Number(countResult.getRowObjectsJson()[0].cnt);

      const now = Date.now();
      await this.connection.run(
        `INSERT INTO _cache_segments VALUES ('${escapeSql(tenant)}', '${escapeSql(file.segment)}', ${now}, ${now}, NULL, ${nextRowId + rowCount}, ${rowCount})`
      );

      await this._cacheBloom(tenant, file.segment);
    }

    // Update last_accessed for all touched segments
    if (files.length > 0) {
      const segList = files.map((f) => `'${escapeSql(f.segment)}'`).join(', ');
      await this.connection.run(
        `UPDATE _cache_segments SET last_accessed = ${Date.now()} WHERE tenant = '${escapeSql(tenant)}' AND segment IN (${segList})`
      );
    }
  }

  async _isSegmentCached(tenant, segment) {
    const result = await this.connection.runAndReadAll(
      `SELECT 1 FROM _cache_segments WHERE tenant = '${escapeSql(tenant)}' AND segment = '${escapeSql(segment)}' LIMIT 1`
    );
    return result.getRowObjectsJson().length > 0;
  }

  async _getNextRowId(tenant) {
    const tableName = tenantTableName(tenant);
    try {
      const result = await this.connection.runAndReadAll(
        `SELECT COALESCE(MAX(_row_id), 0) AS max_id FROM ${tableName}`
      );
      return Number(result.getRowObjectsJson()[0].max_id);
    } catch {
      return 0;
    }
  }

  async _cacheBloom(tenant, segment) {
    try {
      const bloomPath = path.join(this.coldStorageRoot, tenant, '_bloom', `${segment}.bloom.json`);
      const bloomData = JSON.parse(await readFile(bloomPath, 'utf-8'));
      await storeBloomToDb(this.connection, tenant, segment, bloomData);
    } catch {
      // No bloom file in cold storage — skip
    }
  }

  async getBloomData(tenant, segment) {
    return loadBloomFromDb(this.connection, tenant, segment);
  }

  async evict(maxRows = 1_000_000) {
    const totalResult = await this.connection.runAndReadAll(
      `SELECT SUM(row_count) AS total FROM _cache_segments`
    );
    const total = Number(totalResult.getRowObjectsJson()[0].total || 0);
    if (total <= maxRows * 0.8) return 0;

    // Find oldest-accessed segments
    const segments = await this.connection.runAndReadAll(
      `SELECT tenant, segment, row_count FROM _cache_segments ORDER BY last_accessed ASC`
    );
    const rows = segments.getRowObjectsJson();

    let remaining = total;
    const target = maxRows * 0.6;
    let evicted = 0;

    for (const row of rows) {
      if (remaining <= target) break;

      const tableName = tenantTableName(row.tenant);
      await this.connection.run(
        `DELETE FROM ${tableName} WHERE _segment = '${escapeSql(row.segment)}'`
      );
      await this.connection.run(
        `DELETE FROM _bloom_cache WHERE tenant = '${escapeSql(row.tenant)}' AND segment = '${escapeSql(row.segment)}'`
      );
      await this.connection.run(
        `DELETE FROM _cache_segments WHERE tenant = '${escapeSql(row.tenant)}' AND segment = '${escapeSql(row.segment)}'`
      );

      remaining -= Number(row.row_count);
      evicted++;
    }

    return evicted;
  }

  resetCounters() {
    this.hits = 0;
    this.misses = 0;
  }

  stats() {
    return {
      hits: this.hits,
      misses: this.misses,
      hitRate: this.hits + this.misses > 0 ? this.hits / (this.hits + this.misses) : 0,
    };
  }
}
