.PHONY: tidy fmt test build run-server run-cli run-benchmark

tidy:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go mod tidy

fmt:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache gofmt -w cmd internal

test:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go test ./cmd/... ./internal/...

build:
	mkdir -p bin
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go build -o bin/ducker-server ./cmd/ducker-server
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go build -o bin/ducker-cli ./cmd/ducker-cli
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go build -o bin/ducker-benchmark ./cmd/ducker-benchmark

run-server:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go run ./cmd/ducker-server

run-cli:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go run ./cmd/ducker-cli

run-benchmark:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go run ./cmd/ducker-benchmark
