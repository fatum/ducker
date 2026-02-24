package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{"PORT", "COLD_STORAGE_DIR", "CACHE_DIR", "DUCKDB_PATH"} {
		_ = os.Unsetenv(k)
	}
	root := "/tmp/ducker-project/go"
	projectRoot := filepath.Dir(root)
	cfg := Load(root)
	if cfg.Port != 3000 {
		t.Fatalf("expected default port 3000, got %d", cfg.Port)
	}
	if cfg.ColdStorageDir != filepath.Join(projectRoot, "cold-storage") {
		t.Fatalf("unexpected cold dir: %s", cfg.ColdStorageDir)
	}
	if cfg.CacheDir != filepath.Join(root, "cache") {
		t.Fatalf("unexpected cache dir: %s", cfg.CacheDir)
	}
	if cfg.DuckDBPath != filepath.Join(root, "cache", "ducker.duckdb") {
		t.Fatalf("unexpected db path: %s", cfg.DuckDBPath)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("PORT", "8081")
	t.Setenv("COLD_STORAGE_DIR", "/x/cold")
	t.Setenv("CACHE_DIR", "/x/cache")
	t.Setenv("DUCKDB_PATH", "/x/cache/custom.duckdb")
	cfg := Load("/ignored")
	if cfg.Port != 8081 {
		t.Fatalf("expected port 8081, got %d", cfg.Port)
	}
	if cfg.ColdStorageDir != "/x/cold" || cfg.CacheDir != "/x/cache" || cfg.DuckDBPath != "/x/cache/custom.duckdb" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
