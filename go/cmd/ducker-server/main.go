package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"ducker/internal/api"
	"ducker/internal/cache"
	"ducker/internal/config"
	"ducker/internal/query"
	"ducker/internal/storage"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("get cwd: %v", err)
	}
	cfg := config.Load(cwd)
	if err := os.MkdirAll(filepath.Dir(cfg.DuckDBPath), 0o755); err != nil {
		log.Fatalf("mkdir cache dir: %v", err)
	}

	coldStorage := storage.NewColdStorage(cfg.ColdStorageDir)
	dbCache := cache.New(cfg.DuckDBPath, cfg.ColdStorageDir)
	if err := dbCache.Init(); err != nil {
		log.Fatalf("init cache: %v", err)
	}
	defer func() {
		if err := dbCache.Close(); err != nil {
			log.Printf("cache close: %v", err)
		}
	}()

	planner := query.NewPlanner(coldStorage, dbCache)
	server := api.NewServer(cfg.ColdStorageDir, coldStorage, dbCache, planner)
	addr := ":" + strconv.Itoa(cfg.Port)
	log.Printf("Ducker server listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Router()); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
