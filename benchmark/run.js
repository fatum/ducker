#!/usr/bin/env node
/**
 * HTTP API Benchmark for Ducker (Go and Node.js implementations)
 *
 * Usage:
 *   node benchmark/run.js [options]
 *
 * Options:
 *   --target <go|node>     Target server (default: node)
 *   --host <url>           Server URL (default: http://localhost:3000)
 *   --generate             Generate test data before benchmarking
 *   --tenants <N>          Number of tenants to generate (default: 1)
 *   --days <N>             Days of data to generate (default: 3)
 *   --rows-per-hour <N>    Rows per hour (default: 5000)
 *   --iterations <N>       Iterations per query (default: 3)
 *   --warmup <N>           Warmup iterations (default: 1)
 */

import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { mkdir, rm } from 'node:fs/promises';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = path.resolve(__dirname, '..');

// Parse CLI args
const args = process.argv.slice(2);
function getArg(name, defaultVal) {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1) return defaultVal;
  if (typeof defaultVal === 'boolean') return true;
  return args[idx + 1] ?? defaultVal;
}

const TARGET = getArg('target', 'node');
const HOST = getArg('host', 'http://localhost:3000');
const GENERATE = args.includes('--generate');
const NUM_TENANTS = parseInt(getArg('tenants', '1'), 10);
const NUM_DAYS = parseInt(getArg('days', '3'), 10);
const ROWS_PER_HOUR = parseInt(getArg('rows-per-hour', '5000'), 10);
const ITERATIONS = parseInt(getArg('iterations', '3'), 10);
const WARMUP = parseInt(getArg('warmup', '1'), 10);

// Test data date range (matches generate.js base date)
const BASE_DATE = '2025-01-15T00:00:00Z';
const END_DATE = new Date(new Date(BASE_DATE).getTime() + NUM_DAYS * 86400000).toISOString();

function formatDuration(ms) {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}μs`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function formatNumber(n) {
  return n.toLocaleString();
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function httpPost(url, body) {
  const start = performance.now();
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const elapsed = performance.now() - start;
  const data = await res.json();
  return { elapsed, status: res.status, data };
}

async function httpGet(url) {
  const start = performance.now();
  const res = await fetch(url);
  const elapsed = performance.now() - start;
  const data = await res.json();
  return { elapsed, status: res.status, data };
}

async function waitForServer(maxWait = 30000) {
  const start = Date.now();
  while (Date.now() - start < maxWait) {
    try {
      const res = await fetch(`${HOST}/tenants`);
      if (res.ok) return true;
    } catch {
      // Server not ready
    }
    await sleep(500);
  }
  return false;
}

async function generateData() {
  console.log(`\nGenerating test data: ${NUM_TENANTS} tenant(s), ${NUM_DAYS} day(s), ${ROWS_PER_HOUR} rows/hour`);

  // Clean existing data
  await rm(path.join(PROJECT_ROOT, 'cold-storage'), { recursive: true, force: true });

  const generateScript = path.join(PROJECT_ROOT, 'node', 'scripts', 'generate.js');

  return new Promise((resolve, reject) => {
    const proc = spawn('node', [
      generateScript,
      '--tenants', String(NUM_TENANTS),
      '--days', String(NUM_DAYS),
      '--rows-per-hour', String(ROWS_PER_HOUR),
    ], {
      cwd: path.join(PROJECT_ROOT, 'node'),
      stdio: 'inherit',
    });

    proc.on('close', code => {
      if (code === 0) resolve();
      else reject(new Error(`Generate failed with code ${code}`));
    });
  });
}

function startNodeServer() {
  // Clean cache
  const cacheDir = path.join(PROJECT_ROOT, 'node', 'cache');

  return new Promise(async (resolve, reject) => {
    await mkdir(cacheDir, { recursive: true });
    await rm(path.join(cacheDir, 'ducker.duckdb'), { force: true });
    await rm(path.join(cacheDir, 'ducker.duckdb.wal'), { force: true });

    const proc = spawn('node', ['src/server.js'], {
      cwd: path.join(PROJECT_ROOT, 'node'),
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, PORT: '3000' },
    });

    proc.stdout.on('data', () => {});
    proc.stderr.on('data', () => {});

    proc.on('error', reject);
    resolve(proc);
  });
}

function startGoServer() {
  const cacheDir = path.join(PROJECT_ROOT, 'go', 'cache');

  return new Promise(async (resolve, reject) => {
    await mkdir(cacheDir, { recursive: true });
    await rm(path.join(cacheDir, 'ducker.duckdb'), { force: true });
    await rm(path.join(cacheDir, 'ducker.duckdb.wal'), { force: true });

    const proc = spawn('go', ['run', './cmd/ducker-server'], {
      cwd: path.join(PROJECT_ROOT, 'go'),
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, PORT: '3000', COLD_STORAGE_DIR: path.join(PROJECT_ROOT, 'cold-storage') },
    });

    proc.stdout.on('data', () => {});
    proc.stderr.on('data', () => {});

    proc.on('error', reject);
    resolve(proc);
  });
}

// Define benchmark queries
function getBenchmarkQueries(tenant, fromDate, toDate) {
  return [
    // Basic queries
    {
      name: 'Simple query (no filters)',
      body: { tenant, from: fromDate, to: toDate, limit: 100 },
    },
    {
      name: 'Single day range',
      body: { tenant, from: fromDate, to: new Date(new Date(fromDate).getTime() + 86400000).toISOString(), limit: 100 },
    },
    {
      name: 'Single hour range',
      body: { tenant, from: fromDate, to: new Date(new Date(fromDate).getTime() + 3600000).toISOString(), limit: 100 },
    },

    // Service filters
    {
      name: 'Filter: service=api',
      body: { tenant, from: fromDate, to: toDate, filters: { service: 'api' }, limit: 100 },
    },
    {
      name: 'Filter: service=auth',
      body: { tenant, from: fromDate, to: toDate, filters: { service: 'auth' }, limit: 100 },
    },

    // Level filters
    {
      name: 'Filter: level=error',
      body: { tenant, from: fromDate, to: toDate, filters: { level: 'error' }, limit: 100 },
    },
    {
      name: 'Filter: level=info',
      body: { tenant, from: fromDate, to: toDate, filters: { level: 'info' }, limit: 100 },
    },

    // Status code filters
    {
      name: 'Filter: status_code=500',
      body: { tenant, from: fromDate, to: toDate, filters: { status_code: 500 }, limit: 100 },
    },
    {
      name: 'Filter: status_code=200',
      body: { tenant, from: fromDate, to: toDate, filters: { status_code: 200 }, limit: 100 },
    },

    // Combined filters
    {
      name: 'Combined: service=api + level=error',
      body: { tenant, from: fromDate, to: toDate, filters: { service: 'api', level: 'error' }, limit: 100 },
    },
    {
      name: 'Combined: service=web + status=500',
      body: { tenant, from: fromDate, to: toDate, filters: { service: 'web', status_code: 500 }, limit: 100 },
    },

    // Wildcard patterns
    {
      name: 'Wildcard: request_path=/api/*',
      body: { tenant, from: fromDate, to: toDate, filters: { request_path: '/api/*' }, limit: 100 },
    },
    {
      name: 'Wildcard: host=host-00*',
      body: { tenant, from: fromDate, to: toDate, filters: { host: 'host-00*' }, limit: 100 },
    },

    // Full-text search
    {
      name: 'FTS: search="processed"',
      body: { tenant, from: fromDate, to: toDate, search: 'processed', limit: 100 },
    },
    {
      name: 'FTS: search="duration"',
      body: { tenant, from: fromDate, to: toDate, search: 'duration', limit: 100 },
    },

    // FTS + filters
    {
      name: 'FTS+Filter: search + service=api',
      body: { tenant, from: fromDate, to: toDate, search: 'processed', filters: { service: 'api' }, limit: 100 },
    },

    // Pagination
    {
      name: 'Pagination: limit=10',
      body: { tenant, from: fromDate, to: toDate, limit: 10 },
    },
    {
      name: 'Pagination: limit=500',
      body: { tenant, from: fromDate, to: toDate, limit: 500 },
    },
    {
      name: 'Pagination: offset=100, limit=100',
      body: { tenant, from: fromDate, to: toDate, limit: 100, offset: 100 },
    },
  ];
}

async function runBenchmark() {
  console.log('='.repeat(70));
  console.log('Ducker HTTP API Benchmark');
  console.log('='.repeat(70));
  console.log(`Target: ${TARGET}`);
  console.log(`Host: ${HOST}`);
  console.log(`Iterations: ${ITERATIONS} (warmup: ${WARMUP})`);
  console.log(`Date range: ${BASE_DATE} to ${END_DATE}`);
  console.log('='.repeat(70));

  // Generate data if requested
  if (GENERATE) {
    await generateData();
  }

  // Start server
  console.log(`\nStarting ${TARGET} server...`);
  let serverProc;
  try {
    serverProc = TARGET === 'go' ? await startGoServer() : await startNodeServer();
  } catch (err) {
    console.error(`Failed to start server: ${err.message}`);
    process.exit(1);
  }

  // Wait for server
  console.log('Waiting for server to be ready...');
  const ready = await waitForServer();
  if (!ready) {
    console.error('Server failed to start within timeout');
    serverProc.kill();
    process.exit(1);
  }
  console.log('Server ready!\n');

  // Get tenants
  const { data: tenantsData } = await httpGet(`${HOST}/tenants`);
  const tenants = tenantsData.tenants || [];
  if (tenants.length === 0) {
    console.error('No tenants found. Run with --generate to create test data.');
    serverProc.kill();
    process.exit(1);
  }
  console.log(`Found tenants: ${tenants.join(', ')}\n`);

  const tenant = tenants[0];
  const queries = getBenchmarkQueries(tenant, BASE_DATE, END_DATE);
  const results = [];

  // Run benchmarks
  console.log('Running benchmarks...\n');

  for (const query of queries) {
    const times = [];
    let lastResult = null;

    // Warmup
    for (let i = 0; i < WARMUP; i++) {
      await httpPost(`${HOST}/query`, query.body);
    }

    // Actual runs
    for (let i = 0; i < ITERATIONS; i++) {
      const { elapsed, data } = await httpPost(`${HOST}/query`, query.body);
      times.push(elapsed);
      lastResult = data;
    }

    const avgTime = times.reduce((a, b) => a + b, 0) / times.length;
    const minTime = Math.min(...times);
    const maxTime = Math.max(...times);
    const stats = lastResult?.stats || {};

    results.push({
      name: query.name,
      avgTime,
      minTime,
      maxTime,
      rowsMatched: stats.rowsMatched || 0,
      filesScanned: stats.filesScanned || 0,
      cacheHits: stats.cacheHits || 0,
      cacheMisses: stats.cacheMisses || 0,
      searchMode: stats.searchMode || 'structured',
    });

    console.log(`  ${query.name}`);
    console.log(`    avg: ${formatDuration(avgTime)}, min: ${formatDuration(minTime)}, max: ${formatDuration(maxTime)}`);
    console.log(`    rows: ${stats.rowsMatched}, files: ${stats.filesScanned}, cache: ${stats.cacheHits}/${stats.cacheMisses + stats.cacheHits}`);
  }

  // Summary
  console.log('\n' + '='.repeat(70));
  console.log('SUMMARY');
  console.log('='.repeat(70));

  const sorted = [...results].sort((a, b) => a.avgTime - b.avgTime);
  console.log('\nBy Response Time (fastest to slowest):');
  console.log('-'.repeat(70));

  for (const r of sorted) {
    const time = formatDuration(r.avgTime).padStart(10);
    const name = r.name.padEnd(45);
    console.log(`${time}  ${name}  rows=${formatNumber(r.rowsMatched)}`);
  }

  const totalTime = results.reduce((sum, r) => sum + r.avgTime, 0);
  const avgTime = totalTime / results.length;

  console.log('-'.repeat(70));
  console.log(`Total: ${formatDuration(totalTime)}`);
  console.log(`Average: ${formatDuration(avgTime)}`);
  console.log(`Queries: ${results.length}`);

  // Cleanup
  console.log('\nShutting down server...');
  serverProc.kill();

  console.log('Benchmark complete!');
}

runBenchmark().catch(err => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
