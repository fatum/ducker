# Ducker - Go Implementation

Go implementation of the Ducker log search engine.

## Requirements

- Go >= 1.25

## Installation

```bash
make tidy
```

## Usage

### Run the Server

```bash
make run-server
# Or with custom config:
PORT=8080 COLD_STORAGE_DIR=/path/to/logs make run-server
```

### Run the CLI

```bash
# Filter by service and level
make run-cli -- --tenant tenant-1 --service auth --level error --last 24h

# Text search
make run-cli -- --tenant tenant-1 --search "connection timeout" --last 48h

# Wildcard matching
make run-cli -- --tenant tenant-1 --request_path "/api/users/*" --last 24h
```

## Testing

```bash
make test
```

## Building

```bash
make build
# Outputs binaries to bin/
```

## Project Structure

```
go/
├── cmd/
│   ├── ducker-cli/         # CLI tool
│   └── ducker-server/      # HTTP server
├── internal/
│   ├── api/                # HTTP handlers
│   ├── bloom/              # Bloom filter probing
│   ├── cache/              # DuckDB caching layer
│   ├── config/             # Environment config
│   ├── manifest/           # Manifest loading
│   ├── query/              # Query engine + planner
│   ├── storage/            # Cold storage access
│   └── tenant/             # Tenant table naming
├── tests/                  # Integration tests
├── go.mod
├── go.sum
└── Makefile
```
