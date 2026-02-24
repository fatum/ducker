import Fastify from 'fastify';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ColdStorage } from './storage/coldStorage.js';
import { DuckDbCache } from './storage/cache.js';
import { QueryPlanner } from './query/planner.js';
import { execute, detectSearchMode } from './query/engine.js';
import { load as loadManifest } from './manifest/manifest.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const NODE_ROOT = path.resolve(__dirname, '..');
const PROJECT_ROOT = path.resolve(NODE_ROOT, '..');

const COLD_STORAGE_DIR = process.env.COLD_STORAGE_DIR || path.join(PROJECT_ROOT, 'cold-storage');
const CACHE_DIR = process.env.CACHE_DIR || path.join(NODE_ROOT, 'cache');
const DB_PATH = process.env.DUCKDB_PATH || path.join(CACHE_DIR, 'ducker.duckdb');
const PORT = parseInt(process.env.PORT || '3000', 10);

const coldStorage = new ColdStorage(COLD_STORAGE_DIR);
const dbCache = new DuckDbCache(DB_PATH, COLD_STORAGE_DIR);
const planner = new QueryPlanner(coldStorage, dbCache);

const app = Fastify({ logger: true });

app.addHook('onReady', async () => {
  await dbCache.init();
});

app.addHook('onClose', async () => {
  await dbCache.close();
});

app.post('/query', async (request, reply) => {
  const { tenant, from, to, filters = {}, search, limit = 100, offset = 0 } = request.body || {};

  if (!tenant) return reply.code(400).send({ error: 'tenant is required' });
  if (!from || !to) return reply.code(400).send({ error: 'from and to are required' });

  const startTs = new Date(from).getTime();
  const endTs = new Date(to).getTime();

  // 1. Load manifest
  const manifest = await loadManifest(COLD_STORAGE_DIR, tenant);

  // 2. Plan (time + bloom pruning)
  const plan = await planner.plan(tenant, manifest, { startTs, endTs, filters });

  // 3. Cache segments into DuckDB
  dbCache.resetCounters();
  await dbCache.ensureCached(tenant, plan.files);

  // 4. Execute query on cached tenant table
  const conn = dbCache.getConnection();
  const segments = plan.files.map((f) => f.segment);
  const result = await execute(conn, tenant, segments, { filters, search, limit, offset, startTs, endTs });

  // 5. Evict if needed
  await dbCache.evict();

  const cacheStats = dbCache.stats();

  return {
    rows: result.rows,
    stats: {
      ...plan.stats,
      filesScanned: result.filesScanned,
      rowsMatched: result.rowsMatched,
      queryTimeMs: result.queryTimeMs,
      searchMode: result.searchMode,
      cacheHits: cacheStats.hits,
      cacheMisses: cacheStats.misses,
    },
  };
});

app.get('/tenants', async () => {
  const tenants = await coldStorage.listTenants();
  return { tenants };
});

app.get('/stats', async () => {
  return {
    cache: dbCache.stats(),
    coldStorageDir: COLD_STORAGE_DIR,
    cacheDir: CACHE_DIR,
    dbPath: DB_PATH,
  };
});

export { app, dbCache };

// Start server if run directly
const isMain = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isMain) {
  app.listen({ port: PORT, host: '0.0.0.0' }, (err, address) => {
    if (err) {
      console.error(err);
      process.exit(1);
    }
    console.log(`Ducker server listening on ${address}`);
  });
}
