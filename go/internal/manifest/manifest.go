package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"ducker/internal/model"
)

var (
	mu    sync.RWMutex
	cache = map[string]model.Manifest{}
)

func cacheKey(root, tenantID string) string {
	return root + "::" + tenantID
}

func Clear() {
	mu.Lock()
	defer mu.Unlock()
	cache = map[string]model.Manifest{}
}

func Load(coldStorageRoot, tenantID string) (model.Manifest, error) {
	key := cacheKey(coldStorageRoot, tenantID)
	mu.RLock()
	if m, ok := cache[key]; ok {
		mu.RUnlock()
		return m, nil
	}
	mu.RUnlock()

	manifestPath := filepath.Join(coldStorageRoot, tenantID, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return model.Manifest{}, err
	}

	var m model.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return model.Manifest{}, err
	}

	mu.Lock()
	cache[key] = m
	mu.Unlock()
	return m, nil
}

func FilesInRange(m model.Manifest, startTS, endTS int64) []model.SegmentFile {
	out := make([]model.SegmentFile, 0, len(m.Files))
	for _, f := range m.Files {
		if f.EndTS >= startTS && f.StartTS <= endTS {
			out = append(out, f)
		}
	}
	return out
}
