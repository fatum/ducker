/**
 * Benchmark: 1B rows across segments with query execution
 * Usage: node scripts/benchmark.js [--rows N] [--segments N] [--force] [--memory N] [--mmap true|false]
 * 
 * Options:
 *   --rows N       Number of rows to generate (default: 1_000_000_000)
 *   --segments N   Number of segments (default: 1000)
 *   --force        Force recreation of data (default: false - reuse existing)
 *   --memory N     DuckDB memory limit (default: 2GB)
 *   --mmap true|false  Enable memory mapping (default: false - uses less memory)
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const NODE_ROOT = path.resolve(__dirname, '..');
const PROJECT_ROOT = path.resolve(NODE_ROOT, '..');
const BENCHMARK_DIR = path.join(PROJECT_ROOT, 'benchmark-data');

const args = process.argv.slice(2);
function getArg(name, defaultVal) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : defaultVal;
}

const TOTAL_ROWS = parseInt(getArg('rows', '1000000000'), 10);
const NUM_SEGMENTS = parseInt(getArg('segments', '1000'), 10);
const FORCE_RECREATE = args.includes('--force');
const MEMORY_LIMIT = getArg('memory', '2GB');
const USE_MMAP = args.includes('--mmap') ? args[args.indexOf('--mmap') + 1] !== 'false' : false;

function formatDuration(ms) {
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60000).toFixed(2)}min`;
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`;
}

function getMemoryUsage() {
  const mem = process.memoryUsage();
  return {
    heapUsed: mem.heapUsed,
    heapTotal: mem.heapTotal,
    rss: mem.rss,
    external: mem.external,
  };
}

async function runBenchmark() {
  console.log('='.repeat(60));
  console.log('Ducker Benchmark: 1B Rows Query Performance');
  console.log('='.repeat(60));
  console.log(`Total Rows: ${TOTAL_ROWS.toLocaleString()}`);
  console.log(`Segments: ${NUM_SEGMENTS}`);
  console.log(`Memory Limit: ${MEMORY_LIMIT}`);
  console.log('='.repeat(60));

  await mkdir(BENCHMARK_DIR, { recursive: true });
  const dbPath = path.join(BENCHMARK_DIR, 'benchmark.duckdb');

  const fs = await import('node:fs/promises');
  let dataExists = false;
  try {
    await fs.access(dbPath);
    dataExists = true;
  } catch {
    // No existing file
  }

  if (dataExists && !FORCE_RECREATE) {
    console.log('  Using existing benchmark database (use --force to recreate)');
  } else {
    if (dataExists) {
      await fs.unlink(dbPath);
      console.log('  Removed existing benchmark database (--force)');
    }
  }

  console.log('\n[1/4] Creating DuckDB instance...');
  const instance = await DuckDBInstance.create(dbPath);
  const conn = await instance.connect();
  await conn.run('INSTALL fts; LOAD fts;');
  await conn.run(`SET memory_limit='${MEMORY_LIMIT}'`);
  console.log(`  Memory limit: ${MEMORY_LIMIT}`);

  let actualCount = 0;
  if (dataExists && !FORCE_RECREATE) {
    try {
      const countResult = await conn.runAndReadAll(`SELECT count(*) as cnt FROM benchmark_logs`);
      actualCount = Number(countResult.getRowObjectsJson()[0].cnt);
      console.log(`  Found existing data: ${actualCount.toLocaleString()} rows`);
    } catch {
      // Table doesn't exist, will generate
    }
  }

  let genTime = 0;
  let ftsTime = 0;
  const tableName = 'benchmark_logs';

  if (actualCount === 0) {
    console.log(`[2/4] Generating ${TOTAL_ROWS.toLocaleString()} rows across ${NUM_SEGMENTS} segments...`);
    genTime = performance.now();

    await conn.run(`
      CREATE TABLE ${tableName} (
        "timestamp" BIGINT,
        service VARCHAR,
        level VARCHAR,
        host VARCHAR,
        trace_id VARCHAR,
        message VARCHAR,
        status_code INTEGER,
        duration_ms DOUBLE,
        request_path VARCHAR,
        _segment VARCHAR,
        _row_id BIGINT
      )
    `);

    const rowsPerSegment = Math.ceil(TOTAL_ROWS / NUM_SEGMENTS);
    console.log(`  Using DuckDB generate_series for fast data generation...`);

    for (let batchStart = 0; batchStart < NUM_SEGMENTS; batchStart += 10) {
      const batchEnd = Math.min(batchStart + 10, NUM_SEGMENTS);
      const segmentRanges = [];
      
      for (let seg = batchStart; seg < batchEnd; seg++) {
        const segStart = seg * rowsPerSegment;
        const segEnd = Math.min(segStart + rowsPerSegment, TOTAL_ROWS);
        segmentRanges.push({ seg, segStart, segEnd, count: segEnd - segStart });
      }

      const batchSql = segmentRanges.map(({ seg, segStart, count }) => {
        const segmentId = `seg_${String(seg).padStart(5, '0')}`;
        const baseTs = 1704067200000 + seg * 3600000;
        
        return `
          SELECT 
            ${baseTs} + (i % 3600000) as "timestamp",
            ['api', 'auth', 'web', 'worker', 'scheduler', 'billing', 'notifications'][1 + (i % 7)] as service,
            CASE i % 100 
              WHEN 0 THEN 'debug'
              WHEN 1 THEN 'debug'
              WHEN 2 THEN 'debug'
              WHEN 3 THEN 'debug'
              WHEN 4 THEN 'debug'
              WHEN 5 THEN 'debug'
              WHEN 6 THEN 'debug'
              WHEN 7 THEN 'debug'
              WHEN 8 THEN 'debug'
              WHEN 9 THEN 'debug'
              WHEN 10 THEN 'error'
              WHEN 11 THEN 'error'
              WHEN 12 THEN 'error'
              WHEN 13 THEN 'fatal'
              ELSE 'info'
            END as level,
            'host-' || lpad(((i % 20) + 1)::VARCHAR, 3, '0') as host,
            substr(md5(i::VARCHAR), 1, 8) || '-' || substr(md5((i+1)::VARCHAR), 1, 4) || '-' || substr(md5((i+2)::VARCHAR), 1, 4) as trace_id,
            'Request completed in ' || (i % 5000) || 'ms' as message,
            CASE i % 20 
              WHEN 0 THEN 400 
              WHEN 1 THEN 401 
              WHEN 2 THEN 403 
              WHEN 3 THEN 404 
              WHEN 4 THEN 500 
              WHEN 5 THEN 502 
              WHEN 6 THEN 503 
              ELSE 200 
            END as status_code,
            (i % 2000)::DOUBLE + 1.0 as duration_ms,
            ['/api/users', '/api/users/profile', '/api/orders', '/api/products', '/api/auth/login', '/api/health'][1 + (i % 6)] as request_path,
            '${segmentId}' as _segment,
            ${segStart} + i as _row_id
          FROM generate_series(0, ${count - 1}) as t(i)
        `;
      }).join('\n      UNION ALL\n      ');

      await conn.run(`INSERT INTO ${tableName} ${batchSql}`);

      if ((batchEnd) % 50 === 0 || batchEnd === NUM_SEGMENTS) {
        const progress = ((batchEnd) / NUM_SEGMENTS * 100).toFixed(1);
        console.log(`  Progress: ${progress}%`);
      }
    }

    genTime = performance.now() - genTime;
    console.log(`  Generation complete in ${formatDuration(genTime)}`);

    const countResult = await conn.runAndReadAll(`SELECT count(*) as cnt FROM ${tableName}`);
    actualCount = Number(countResult.getRowObjectsJson()[0].cnt);
    console.log(`  Actual row count: ${actualCount.toLocaleString()}`);

    console.log('[3/4] Building FTS index...');
    ftsTime = performance.now();
    await conn.run(
      `PRAGMA create_fts_index('${tableName}', '_row_id', 'message', stemmer='porter', stopwords='english', overwrite=1)`
    );
    ftsTime = performance.now() - ftsTime;
    console.log(`  FTS index built in ${formatDuration(ftsTime)}`);
  } else {
    console.log('[2/4] Skipping data generation (using existing data)');
    console.log('[3/4] Skipping FTS index (using existing index)');
  }

  console.log('[4/4] Running query benchmarks...\n');

  const initialMemory = getMemoryUsage();
  console.log(`  Initial Memory: heap ${formatBytes(initialMemory.heapUsed)} / ${formatBytes(initialMemory.heapTotal)}, RSS ${formatBytes(initialMemory.rss)}`);
  console.log('');

  const queries = [
    {
      name: 'Full table scan (count)',
      sql: `SELECT count(*) as cnt FROM ${tableName}`,
    },
    {
      name: 'Filter by service = api',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE service = 'api'`,
    },
    {
      name: 'Filter by level IN (error, fatal)',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE level IN ('error', 'fatal')`,
    },
    {
      name: 'Filter by status_code >= 500',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE status_code >= 500`,
    },
    {
      name: 'Filter by duration_ms > 1000',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE duration_ms > 1000`,
    },
    {
      name: 'Filter by host (specific)',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE host = 'host-005'`,
    },
    {
      name: 'Complex filter (service + level)',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE service = 'api' AND level = 'error'`,
    },
    {
      name: 'Time range filter',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE "timestamp" BETWEEN 1704067200000 AND 1704153600000`,
    },
    {
      name: 'Group by service (aggregation)',
      sql: `SELECT service, count(*) as cnt FROM ${tableName} GROUP BY service ORDER BY cnt DESC LIMIT 10`,
    },
    {
      name: 'Group by level (aggregation)',
      sql: `SELECT level, count(*) as cnt FROM ${tableName} GROUP BY level ORDER BY cnt DESC`,
    },
    {
      name: 'Group by status_code (aggregation)',
      sql: `SELECT status_code, count(*) as cnt FROM ${tableName} GROUP BY status_code ORDER BY cnt DESC LIMIT 10`,
    },
    {
      name: 'Average duration by service',
      sql: `SELECT service, avg(duration_ms) as avg_duration FROM ${tableName} GROUP BY service ORDER BY avg_duration DESC`,
    },
    {
      name: 'LIMIT query',
      sql: `SELECT * FROM ${tableName} LIMIT 100`,
    },
    {
      name: 'Segment filter (single segment)',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE _segment = 'seg_00000'`,
    },
    {
      name: 'Segment filter (10 segments)',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE _segment IN ('seg_00000', 'seg_00001', 'seg_00002', 'seg_00003', 'seg_00004', 'seg_00005', 'seg_00006', 'seg_00007', 'seg_00008', 'seg_00009')`,
    },
    {
      name: 'COUNT with GROUP BY segment',
      sql: `SELECT _segment, count(*) as cnt FROM ${tableName} GROUP BY _segment ORDER BY _segment LIMIT 10`,
    },
    {
      name: 'Wildcard: request_path LIKE /api/users%',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE request_path LIKE '/api/users%'`,
    },
    {
      name: 'Wildcard: message LIKE %completed%',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE message LIKE '%completed%'`,
    },
    {
      name: 'Wildcard: host LIKE host-00?',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE host LIKE 'host-00_'`,
    },
    {
      name: 'FTS: contains(message, "completed")',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE contains(message, 'completed')`,
    },
    {
      name: 'FTS: contains(message, "Request")',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE contains(message, 'Request')`,
    },
    {
      name: 'FTS: contains(message, "completed ms")',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE contains(message, 'completed ms')`,
    },
    {
      name: 'FTS + filter: contains(message, "completed") AND service=api',
      sql: `SELECT count(*) as cnt FROM ${tableName} WHERE contains(message, 'completed') AND service = 'api'`,
    },
    {
      name: 'FTS with LIMIT (bm25)',
      sql: `SELECT _row_id, message FROM ${tableName} WHERE contains(message, 'completed') LIMIT 100`,
    },
  ];

  const results = [];

  for (const query of queries) {
    const memBefore = getMemoryUsage();
    const start = performance.now();
    try {
      const result = await conn.runAndReadAll(query.sql);
      const rows = result.getRowObjectsJson();
      const time = performance.now() - start;
      const memAfter = getMemoryUsage();
      const rowCount = rows.length;
      const memDelta = memAfter.heapUsed - memBefore.heapUsed;
      results.push({ name: query.name, time, rowCount, success: true, memDelta });
      console.log(`  ✓ ${query.name}`);
      console.log(`      Time: ${formatDuration(time)}, Rows: ${rowCount}, Mem: ${formatBytes(Math.abs(memDelta))}${memDelta >= 0 ? ' ↑' : ' ↓'}`);
    } catch (err) {
      const time = performance.now() - start;
      results.push({ name: query.name, time, rowCount: 0, success: false, error: err.message });
      console.log(`  ✗ ${query.name}: ${err.message}`);
    }
  }

  console.log('\n' + '='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  console.log(`Total Rows: ${actualCount.toLocaleString()}`);
  console.log(`Segments: ${NUM_SEGMENTS}`);
  console.log(`Data Generation: ${formatDuration(genTime)}`);
  console.log(`FTS Index Build: ${formatDuration(ftsTime)}`);
  console.log('\nQuery Results (sorted by time):');
  console.log('-'.repeat(60));
  
  const sortedResults = [...results].sort((a, b) => a.time - b.time);
  for (const r of sortedResults) {
    const status = r.success ? '✓' : '✗';
    console.log(`${status} ${r.name.padEnd(40)} ${formatDuration(r.time).padStart(12)}`);
  }

  const totalQueryTime = results.reduce((sum, r) => sum + r.time, 0);
  const avgMemDelta = results.reduce((sum, r) => sum + (r.memDelta || 0), 0) / results.length;
  console.log('-'.repeat(60));
  console.log(`Total Query Time: ${formatDuration(totalQueryTime)}`);
  console.log(`Average Query Time: ${formatDuration(totalQueryTime / results.length)}`);

  const finalMemory = getMemoryUsage();
  console.log('\nMemory Usage:');
  console.log(`  Initial: heap ${formatBytes(initialMemory.heapUsed)} / ${formatBytes(initialMemory.heapTotal)}, RSS ${formatBytes(initialMemory.rss)}`);
  console.log(`  Final:   heap ${formatBytes(finalMemory.heapUsed)} / ${formatBytes(finalMemory.heapTotal)}, RSS ${formatBytes(finalMemory.rss)}`);
  console.log(`  Delta:   heap ${formatBytes(finalMemory.heapUsed - initialMemory.heapUsed)}, RSS ${formatBytes(finalMemory.rss - initialMemory.rss)}`);
  console.log(`  Avg per query: ${formatBytes(Math.abs(avgMemDelta))}`);

  conn.closeSync();
  console.log('\nBenchmark complete!');
  console.log(`Database saved to: ${dbPath}`);
}

runBenchmark().catch((err) => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
