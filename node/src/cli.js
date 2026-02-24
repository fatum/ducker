/**
 * CLI helper for quick queries.
 * Usage: node src/cli.js --tenant tenant-1 --service auth --level error --last 24h
 *        node src/cli.js --tenant tenant-1 --search "connection timeout" --last 48h
 *        node src/cli.js --tenant tenant-1 --request_path "/api/users/*" --last 24h
 */

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ColdStorage } from './storage/coldStorage.js';
import { DuckDbCache } from './storage/cache.js';
import { QueryPlanner } from './query/planner.js';
import { execute } from './query/engine.js';
import { load as loadManifest } from './manifest/manifest.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const NODE_ROOT = path.resolve(__dirname, '..');
const PROJECT_ROOT = path.resolve(NODE_ROOT, '..');
const COLD_STORAGE_DIR = process.env.COLD_STORAGE_DIR || path.join(PROJECT_ROOT, 'cold-storage');
const CACHE_DIR = process.env.CACHE_DIR || path.join(NODE_ROOT, 'cache');
const DB_PATH = process.env.DUCKDB_PATH || path.join(CACHE_DIR, 'ducker.duckdb');

const args = process.argv.slice(2);

function getFlag(name) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : null;
}

const FILTER_FIELDS = ['service', 'level', 'host', 'status_code', 'request_path', 'trace_id'];

async function main() {
  const tenant = getFlag('tenant');
  if (!tenant) {
    console.error('Usage: node src/cli.js --tenant <id> [--service X] [--level X] [--search "text"] [--last 24h]');
    process.exit(1);
  }

  // Parse time range
  const lastStr = getFlag('last') || '24h';
  const lastMatch = lastStr.match(/^(\d+)(h|d)$/);
  if (!lastMatch) {
    console.error('Invalid --last format. Use e.g. 24h, 7d');
    process.exit(1);
  }
  const amount = parseInt(lastMatch[1], 10);
  const unit = lastMatch[2];
  const ms = unit === 'h' ? amount * 3600000 : amount * 86400000;

  // Use the end of the data range from manifest
  const manifest = await loadManifest(COLD_STORAGE_DIR, tenant);
  const maxTs = Math.max(...manifest.files.map((f) => f.endTs));
  const endTs = maxTs;
  const startTs = endTs - ms;

  // Build filters from CLI args
  const filters = {};
  for (const field of FILTER_FIELDS) {
    const val = getFlag(field);
    if (val) filters[field] = val;
  }

  const search = getFlag('search');
  const limit = parseInt(getFlag('limit') || '20', 10);

  console.log(`\nQuery: tenant=${tenant} range=${new Date(startTs).toISOString()} → ${new Date(endTs).toISOString()}`);
  if (Object.keys(filters).length > 0) console.log(`Filters: ${JSON.stringify(filters)}`);
  if (search) console.log(`Search: "${search}"`);
  console.log();

  const coldStorage = new ColdStorage(COLD_STORAGE_DIR);
  const dbCache = new DuckDbCache(DB_PATH, COLD_STORAGE_DIR);
  await dbCache.init();

  const planner = new QueryPlanner(coldStorage, dbCache);

  // Plan
  const plan = await planner.plan(tenant, manifest, { startTs, endTs, filters });

  // Cache segments into DuckDB
  dbCache.resetCounters();
  await dbCache.ensureCached(tenant, plan.files);

  // Execute
  const conn = dbCache.getConnection();
  const segments = plan.files.map((f) => f.segment);
  const result = await execute(conn, tenant, segments, { filters, search, limit, offset: 0, startTs, endTs });

  const cacheStats = dbCache.stats();

  // Print stats
  console.log('--- Stats ---');
  console.log(`  Total files:          ${plan.stats.totalFiles}`);
  console.log(`  After time filter:    ${plan.stats.filesAfterTimeFilter}`);
  console.log(`  After bloom filter:   ${plan.stats.filesAfterBloom}`);
  console.log(`  Files scanned:        ${result.filesScanned}`);
  console.log(`  Rows matched:         ${result.rowsMatched}`);
  console.log(`  Query time:           ${result.queryTimeMs}ms`);
  console.log(`  Search mode:          ${result.searchMode}`);
  console.log(`  Cache hits/misses:    ${cacheStats.hits}/${cacheStats.misses}`);
  console.log();

  // Print results
  console.log('--- Results ---');
  if (result.rows.length === 0) {
    console.log('  (no results)');
  } else {
    for (const row of result.rows) {
      const ts = new Date(Number(row.timestamp)).toISOString();
      const score = row.score !== undefined ? ` score=${Number(row.score).toFixed(3)}` : '';
      console.log(`  [${ts}] [${row.level}] [${row.service}] ${row.message}${score}`);
    }
  }

  await dbCache.close();
}

main().catch((err) => {
  console.error('CLI error:', err);
  process.exit(1);
});
