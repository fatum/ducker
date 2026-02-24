#!/usr/bin/env node
/**
 * Generate test parquet data using pure DuckDB SQL.
 * Usage: node scripts/generate-simple.js [--tenants N] [--days N] [--rows-per-hour N]
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { mkdir, writeFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { BloomFilter } from '../src/bloom/bloom.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const NODE_ROOT = path.resolve(__dirname, '..');
const PROJECT_ROOT = path.resolve(NODE_ROOT, '..');
const COLD_STORAGE = path.join(PROJECT_ROOT, 'cold-storage');

// Parse CLI args
const args = process.argv.slice(2);
function getArg(name, defaultVal) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? parseInt(args[idx + 1], 10) : defaultVal;
}

const NUM_TENANTS = getArg('tenants', 1);
const NUM_DAYS = getArg('days', 2);
const ROWS_PER_HOUR = getArg('rows-per-hour', 1000);

const BLOOM_COLUMNS = ['service', 'level', 'host', 'status_code'];

async function generateHourFile(conn, tenantId, year, month, day, hour, rowCount) {
  const monthStr = String(month).padStart(2, '0');
  const dayStr = String(day).padStart(2, '0');
  const hourStr = String(hour).padStart(2, '0');
  const segment = `${year}-${monthStr}-${dayStr}_${hourStr}`;

  const relDir = path.join(tenantId, `year=${year}`, `month=${monthStr}`, `day=${dayStr}`);
  const absDir = path.join(COLD_STORAGE, relDir);
  await mkdir(absDir, { recursive: true });

  const filename = `hour=${hourStr}.parquet`;
  const relPath = path.join(relDir, filename);
  const absPath = path.join(COLD_STORAGE, relPath);

  // Calculate hour boundaries in epoch ms
  const hourStart = Date.UTC(year, month - 1, day, hour, 0, 0);
  const hourEnd = hourStart + 3600000 - 1;

  // Generate data entirely in DuckDB SQL using array indexing for proper randomization
  await conn.run(`
    COPY (
      SELECT
        ${hourStart} + floor(random() * 3600000)::BIGINT AS "timestamp",
        (['api', 'auth', 'web', 'worker', 'scheduler', 'billing', 'notifications'])[floor(random() * 7)::INTEGER + 1] AS service,
        (['debug', 'info', 'info', 'info', 'warn', 'error'])[floor(random() * 6)::INTEGER + 1] AS level,
        'host-' || lpad((floor(random() * 20) + 1)::VARCHAR, 3, '0') AS host,
        uuid()::VARCHAR AS trace_id,
        'Request processed id=' || (floor(random() * 10000)::INTEGER)::VARCHAR || ' duration=' || (floor(random() * 2000)::INTEGER)::VARCHAR || 'ms' AS message,
        ([200, 200, 200, 201, 400, 401, 404, 500])[floor(random() * 8)::INTEGER + 1] AS status_code,
        round(random() * 2000, 1) AS duration_ms,
        (['/api/users', '/api/orders', '/api/products', '/api/auth/login', '/api/health'])[floor(random() * 5)::INTEGER + 1] AS request_path
      FROM generate_series(1, ${rowCount})
      ORDER BY "timestamp"
    ) TO '${absPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
  `);

  // Extract distinct values for bloom filters
  const bloomValues = {};
  for (const col of BLOOM_COLUMNS) {
    const result = await conn.runAndReadAll(`SELECT DISTINCT ${col}::VARCHAR AS val FROM read_parquet('${absPath}')`);
    const values = new Set();
    for (const row of result.getRows()) {
      values.add(String(row[0]));
    }
    bloomValues[col] = values;
  }

  // Build bloom filters
  const bloomDir = path.join(COLD_STORAGE, tenantId, '_bloom');
  await mkdir(bloomDir, { recursive: true });

  const bloomColumns = {};
  for (const col of BLOOM_COLUMNS) {
    const bf = BloomFilter.build([...bloomValues[col]]);
    bloomColumns[col] = bf.serialize();
  }

  await writeFile(
    path.join(bloomDir, `${segment}.bloom.json`),
    JSON.stringify({ columns: bloomColumns })
  );

  const fileStat = await stat(absPath);

  return {
    path: relPath,
    segment,
    startTs: hourStart,
    endTs: hourEnd,
    rowCount,
    sizeBytes: fileStat.size,
  };
}

async function main() {
  console.log(`Generating: ${NUM_TENANTS} tenant(s), ${NUM_DAYS} day(s), ${ROWS_PER_HOUR} rows/hour`);
  console.log(`Output: ${COLD_STORAGE}`);

  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  const baseDate = new Date('2025-01-15T00:00:00Z');

  for (let t = 1; t <= NUM_TENANTS; t++) {
    const tenantId = `tenant-${t}`;
    console.log(`\nGenerating ${tenantId}...`);

    const manifest = { tenantId, files: [], createdAt: new Date().toISOString() };

    for (let d = 0; d < NUM_DAYS; d++) {
      const date = new Date(baseDate.getTime() + d * 86400000);
      const year = date.getUTCFullYear();
      const month = date.getUTCMonth() + 1;
      const day = date.getUTCDate();
      const dateStr = date.toISOString().slice(0, 10);
      process.stdout.write(`  ${dateStr}: `);

      for (let h = 0; h < 24; h++) {
        const fileInfo = await generateHourFile(conn, tenantId, year, month, day, h, ROWS_PER_HOUR);
        manifest.files.push(fileInfo);
        process.stdout.write('.');
      }
      console.log(' done');
    }

    const manifestPath = path.join(COLD_STORAGE, tenantId, 'manifest.json');
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2));
    console.log(`  Manifest: ${manifest.files.length} files`);
  }

  conn.closeSync();
  console.log('\nDone!');
}

main().catch((err) => {
  console.error('Generation failed:', err);
  process.exit(1);
});
