package config

import (
	"os"
	"path/filepath"
	"strconv"
)

type Config struct {
	Port           int
	ColdStorageDir string
	CacheDir       string
	DuckDBPath     string
}

func Load(root string) Config {
	projectRoot := filepath.Dir(root) // go up one level from go/ to project root
	cold := getenv("COLD_STORAGE_DIR", filepath.Join(projectRoot, "cold-storage"))
	cacheDir := getenv("CACHE_DIR", filepath.Join(root, "cache"))
	dbPath := getenv("DUCKDB_PATH", filepath.Join(cacheDir, "ducker.duckdb"))
	port, err := strconv.Atoi(getenv("PORT", "3000"))
	if err != nil || port <= 0 {
		port = 3000
	}
	return Config{Port: port, ColdStorageDir: cold, CacheDir: cacheDir, DuckDBPath: dbPath}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
