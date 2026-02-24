/**
 * Generate test parquet data for Ducker PoC.
 * Usage: node scripts/generate.js [--tenants N] [--days N] [--rows-per-hour N]
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

const NUM_TENANTS = getArg('tenants', 2);
const NUM_DAYS = getArg('days', 3);
const ROWS_PER_HOUR = getArg('rows-per-hour', 10000);

const SERVICES = ['api', 'auth', 'web', 'worker', 'scheduler', 'billing', 'notifications'];
const LEVELS = ['debug', 'info', 'warn', 'error', 'fatal'];
const LEVEL_WEIGHTS = [10, 60, 15, 12, 3]; // weighted distribution
const STATUS_CODES = [200, 200, 200, 201, 400, 401, 403, 404, 500, 502, 503];
const PATHS = [
  '/api/users', '/api/users/profile', '/api/orders', '/api/orders/checkout',
  '/api/products', '/api/products/search', '/api/auth/login', '/api/auth/refresh',
  '/api/billing/invoices', '/api/notifications/send', '/api/health', '/api/metrics',
];

const MESSAGE_TEMPLATES = {
  debug: [
    'Processing request for user_id={uid}',
    'Cache lookup for key={key} ttl={ttl}ms',
    'Database query executed in {dur}ms rows={rows}',
    'Serializing response payload size={size} bytes',
  ],
  info: [
    'Request completed successfully status={status} duration={dur}ms',
    'User {uid} authenticated via {method}',
    'Order {oid} created total={total} currency=USD',
    'Scheduled job {job} started at {time}',
    'Connection pool stats: active={active} idle={idle} waiting={waiting}',
  ],
  warn: [
    'Slow query detected duration={dur}ms threshold=1000ms query={query}',
    'Rate limit approaching for tenant={tid} current={current}/1000',
    'Retry attempt {attempt}/3 for external service {svc}',
    'Memory usage high: {mem}MB / 512MB',
    'Deprecated API version v1 called by user_id={uid}',
  ],
  error: [
    'Connection refused to database host={host} port=5432',
    'Request timeout after {dur}ms to upstream {svc}',
    'Authentication failed for user_id={uid} reason={reason}',
    'Out of memory allocating {size}MB for request {rid}',
    'Unhandled exception in handler: {err}',
    'Connection timeout reaching redis host={host}',
    'Database connection pool exhausted active=50 max=50',
  ],
  fatal: [
    'Process crash: segmentation fault at address {addr}',
    'Unrecoverable state: data corruption detected in table {table}',
    'OOM killer invoked: process using {mem}GB exceeds limit',
  ],
};

const BLOOM_COLUMNS = ['service', 'level', 'host', 'status_code'];

function pickWeighted(items, weights) {
  const total = weights.reduce((a, b) => a + b, 0);
  let r = Math.random() * total;
  for (let i = 0; i < items.length; i++) {
    r -= weights[i];
    if (r <= 0) return items[i];
  }
  return items[items.length - 1];
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function generateMessage(level) {
  const templates = MESSAGE_TEMPLATES[level];
  const tpl = templates[randomInt(0, templates.length - 1)];
  return tpl
    .replace('{uid}', randomInt(1000, 9999))
    .replace('{key}', `cache:user:${randomInt(1, 500)}`)
    .replace('{ttl}', randomInt(100, 5000))
    .replace('{dur}', randomInt(1, 5000))
    .replace('{rows}', randomInt(0, 1000))
    .replace('{size}', randomInt(100, 50000))
    .replace('{status}', STATUS_CODES[randomInt(0, STATUS_CODES.length - 1)])
    .replace('{method}', ['password', 'oauth', 'api_key', 'sso'][randomInt(0, 3)])
    .replace('{oid}', `ORD-${randomInt(10000, 99999)}`)
    .replace('{total}', (Math.random() * 500).toFixed(2))
    .replace('{job}', ['cleanup', 'sync', 'report', 'backup'][randomInt(0, 3)])
    .replace('{time}', new Date().toISOString())
    .replace('{query}', 'SELECT * FROM orders WHERE ...')
    .replace('{tid}', `tenant-${randomInt(1, 10)}`)
    .replace('{current}', randomInt(800, 999))
    .replace('{attempt}', randomInt(1, 3))
    .replace('{svc}', SERVICES[randomInt(0, SERVICES.length - 1)])
    .replace('{mem}', randomInt(200, 500))
    .replace('{reason}', ['invalid_password', 'expired_token', 'ip_blocked'][randomInt(0, 2)])
    .replace('{rid}', uuid().slice(0, 8))
    .replace('{err}', ['TypeError', 'RangeError', 'ConnectionError'][randomInt(0, 2)])
    .replace('{host}', `host-${String(randomInt(1, 20)).padStart(3, '0')}`)
    .replace('{addr}', `0x${randomInt(0, 0xffffff).toString(16)}`)
    .replace('{table}', ['users', 'orders', 'sessions'][randomInt(0, 2)])
    .replace('{active}', randomInt(10, 50))
    .replace('{idle}', randomInt(0, 20))
    .replace('{waiting}', randomInt(0, 10));
}

async function generateHourFile(conn, tenantId, date, hour, rowCount) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hourStr = String(hour).padStart(2, '0');
  const segment = `${year}-${month}-${day}_${hourStr}`;

  const relDir = path.join(tenantId, `year=${year}`, `month=${month}`, `day=${day}`);
  const absDir = path.join(COLD_STORAGE, relDir);
  await mkdir(absDir, { recursive: true });

  const filename = `hour=${hourStr}.parquet`;
  const relPath = path.join(relDir, filename);
  const absPath = path.join(COLD_STORAGE, relPath);

  const hourStart = new Date(Date.UTC(year, date.getUTCMonth(), date.getUTCDate(), hour, 0, 0));
  const startTs = hourStart.getTime();
  const endTs = startTs + 3600000 - 1;

  // Generate data via DuckDB SQL
  const tableName = `gen_${tenantId.replace('-', '_')}_${segment.replace(/-/g, '_')}`;

  // Create a temporary table with the schema
  await conn.run(`
    CREATE OR REPLACE TEMP TABLE ${tableName} (
      "timestamp" BIGINT,
      service VARCHAR,
      level VARCHAR,
      host VARCHAR,
      trace_id VARCHAR,
      message VARCHAR,
      status_code INTEGER,
      duration_ms DOUBLE,
      request_path VARCHAR
    )
  `);

  // Generate rows in batches
  const BATCH_SIZE = 5000;
  const bloomValues = { service: new Set(), level: new Set(), host: new Set(), status_code: new Set() };

  for (let offset = 0; offset < rowCount; offset += BATCH_SIZE) {
    const batchRows = Math.min(BATCH_SIZE, rowCount - offset);
    const values = [];

    for (let i = 0; i < batchRows; i++) {
      const ts = startTs + randomInt(0, 3599999);
      const service = SERVICES[randomInt(0, SERVICES.length - 1)];
      const level = pickWeighted(LEVELS, LEVEL_WEIGHTS);
      const hostNum = randomInt(1, 20);
      const host = `host-${String(hostNum).padStart(3, '0')}`;
      const traceId = uuid();
      const message = generateMessage(level);
      const statusCode = STATUS_CODES[randomInt(0, STATUS_CODES.length - 1)];
      const durationMs = level === 'error' ? randomInt(500, 10000) : randomInt(1, 2000);
      const requestPath = PATHS[randomInt(0, PATHS.length - 1)];

      bloomValues.service.add(service);
      bloomValues.level.add(level);
      bloomValues.host.add(host);
      bloomValues.status_code.add(String(statusCode));

      values.push(
        `(${ts}, '${service}', '${level}', '${host}', '${traceId}', '${message.replace(/'/g, "''")}', ${statusCode}, ${durationMs.toFixed(1)}, '${requestPath}')`
      );
    }

    await conn.run(`INSERT INTO ${tableName} VALUES ${values.join(',\n')}`);
  }

  // Write to parquet
  await conn.run(
    `COPY (SELECT * FROM ${tableName} ORDER BY "timestamp") TO '${absPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)`
  );

  await conn.run(`DROP TABLE ${tableName}`);

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

  // Get file size
  const fileStat = await stat(absPath);

  return {
    path: relPath,
    segment,
    startTs,
    endTs,
    rowCount,
    sizeBytes: fileStat.size,
  };
}

async function main() {
  console.log(`Generating test data: ${NUM_TENANTS} tenants, ${NUM_DAYS} days, ${ROWS_PER_HOUR} rows/hour`);
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
      const dateStr = date.toISOString().slice(0, 10);
      process.stdout.write(`  ${dateStr}: `);

      for (let h = 0; h < 24; h++) {
        const fileInfo = await generateHourFile(conn, tenantId, date, h, ROWS_PER_HOUR);
        manifest.files.push(fileInfo);
        process.stdout.write('.');
      }
      console.log(' done');
    }

    // Write manifest
    const manifestPath = path.join(COLD_STORAGE, tenantId, 'manifest.json');
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2));
    console.log(`  Manifest written: ${manifest.files.length} files`);
  }

  conn.closeSync();
  console.log('\nDone!');
}

main().catch((err) => {
  console.error('Generation failed:', err);
  process.exit(1);
});
