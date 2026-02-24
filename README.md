# Ducker

A DuckDB-powered stateless log search engine proof-of-concept.

Ducker demonstrates how to build a scalable log search system using DuckDB as an embedded query engine with cold storage in Parquet format. It features bloom filter pruning, basic text search, and an LRU caching layer for efficient query execution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Query Flow                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐   │
│   │  Client  │───▶│ Query Planner│───▶│ DuckDB Cache│───▶│ Query Engine │   │
│   └──────────┘    └──────────────┘    └─────────────┘    └──────────────┘   │
│                          │                   │                  │            │
│                          ▼                   ▼                  ▼            │
│                   ┌──────────────┐    ┌─────────────┐    ┌──────────────┐   │
│                   │ Bloom Filters│    │Cold Storage │    │    Search    │   │
│                   │  (pruning)   │    │  (Parquet)  │    │   (ILIKE)    │   │
│                   └──────────────┘    └─────────────┘    └──────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features

- **Multi-tenant** - Isolated data storage per tenant
- **Time-range filtering** - Efficiently skips segments outside query window
- **Bloom filter pruning** - Eliminates files that definitely don't contain matches
- **Basic text search** - Case-insensitive substring search on message column
- **Wildcard patterns** - `*` and `?` glob-style matching on any column
- **Range filters** - Support for `gt`, `gte`, `lt`, `lte` operators
- **IN filters** - Match any value from a list
- **LRU cache eviction** - Automatic eviction when cache exceeds threshold
- **Stateless design** - Cold storage as source of truth, cache is ephemeral

## Implementations

This project includes two implementations:

| Implementation | Directory | Description |
|----------------|-----------|-------------|
| **Go** | [`go/`](go/) | Production-ready implementation with HTTP server and CLI |
| **Node.js** | [`node/`](node/) | Reference implementation with data generation scripts |

See the README in each directory for language-specific instructions.

## Quick Start

### 1. Generate Test Data

The data generation script is in the Node.js implementation:

```bash
cd node
npm install
npm run generate
# Or with options:
npm run generate -- --tenants 3 --days 7 --rows-per-hour 20000
```

This creates shared data in the project root:
- `cold-storage/tenant-N/` - Parquet files organized by date
- `cold-storage/tenant-N/manifest.json` - Segment metadata
- `cold-storage/tenant-N/_bloom/` - Bloom filter files

### 2. Run the Server

**Go:**
```bash
cd go
make run-server
```

**Node.js:**
```bash
cd node
npm start
```

### 3. Query Logs

```bash
curl -X POST http://localhost:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "tenant": "tenant-1",
    "from": "2025-01-15T00:00:00Z",
    "to": "2025-01-17T23:59:59Z",
    "filters": {
      "service": "auth",
      "level": "error"
    },
    "limit": 50
  }'
```

## API Reference

### POST /query

Query logs with filtering and text search.

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tenant` | string | Yes | Tenant identifier |
| `from` | string | Yes | Start timestamp (ISO 8601) |
| `to` | string | Yes | End timestamp (ISO 8601) |
| `filters` | object | No | Column filters (see below) |
| `search` | string | No | Text search in message column |
| `limit` | number | No | Max results (default: 100) |
| `offset` | number | No | Pagination offset (default: 0) |

**Filter Types:**

```json
{
  "filters": {
    "service": "auth",
    "level": ["error", "fatal"],
    "request_path": "/api/users/*",
    "status_code": { "gte": 400 },
    "duration_ms": { "gt": 1000, "lte": 5000 }
  }
}
```

### GET /tenants

List all available tenants.

### GET /stats

Get cache statistics and configuration.

## Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | BIGINT | Unix timestamp in milliseconds |
| `service` | VARCHAR | Service name (api, auth, web, worker, etc.) |
| `level` | VARCHAR | Log level (debug, info, warn, error, fatal) |
| `host` | VARCHAR | Host identifier (host-001 to host-020) |
| `trace_id` | VARCHAR | Distributed trace ID |
| `message` | VARCHAR | Log message (searchable) |
| `status_code` | INTEGER | HTTP status code |
| `duration_ms` | DOUBLE | Request duration |
| `request_path` | VARCHAR | API endpoint path |

## Configuration

Environment variables (work with both implementations):

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3000 | HTTP server port |
| `COLD_STORAGE_DIR` | `../cold-storage` | Path to Parquet files |
| `CACHE_DIR` | `./cache` | Path to DuckDB cache |
| `DUCKDB_PATH` | `./cache/ducker.duckdb` | DuckDB database file |

## Project Structure

```
ducker/
├── go/                     # Go implementation
│   ├── cmd/                # CLI and server entrypoints
│   ├── internal/           # Core packages
│   ├── go.mod
│   └── Makefile
├── node/                   # Node.js implementation
│   ├── src/                # Server and CLI
│   ├── scripts/            # Data generation and benchmarks
│   ├── test/               # Tests
│   └── package.json
├── cold-storage/           # Shared Parquet data (gitignored)
├── docs/                   # Design documents
└── README.md               # This file
```

## License

MIT
