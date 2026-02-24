# Ducker - Node.js Implementation

Node.js reference implementation of the Ducker log search engine.

## Requirements

- Node.js >= 22.0.0

## Installation

```bash
npm install
```

## Usage

### Generate Test Data

```bash
npm run generate
# Or with options:
npm run generate -- --tenants 3 --days 7 --rows-per-hour 20000
```

This creates shared data in the project root:
- `../cold-storage/tenant-N/` - Parquet files organized by date
- `../cold-storage/tenant-N/manifest.json` - Segment metadata
- `../cold-storage/tenant-N/_bloom/` - Bloom filter files

### Run the Server

```bash
npm start
# Or with custom config:
PORT=8080 COLD_STORAGE_DIR=/path/to/logs npm start
```

### Run the CLI

```bash
# Filter by service and level
npm run cli -- --tenant tenant-1 --service auth --level error --last 24h

# Text search
npm run cli -- --tenant tenant-1 --search "connection timeout" --last 48h

# Wildcard matching
npm run cli -- --tenant tenant-1 --request_path "/api/users/*" --last 24h
```

## Testing

```bash
npm test
```

## Project Structure

```
node/
├── src/
│   ├── bloom/              # Bloom filter implementation
│   ├── manifest/           # Manifest loading
│   ├── query/              # Query engine + planner
│   ├── storage/            # Cold storage and cache
│   ├── cli.js              # CLI tool
│   └── server.js           # HTTP server
├── scripts/
│   ├── generate.js         # Test data generator
│   └── benchmark.js        # Performance benchmarks
├── test/                   # Tests
└── package.json
```
