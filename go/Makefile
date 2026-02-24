.PHONY: tidy fmt test build run-server run-cli

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

run-server:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go run ./cmd/ducker-server

run-cli:
	GOCACHE=$(PWD)/.gocache GOMODCACHE=$(PWD)/.gomodcache go run ./cmd/ducker-cli
