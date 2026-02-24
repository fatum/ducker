package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"ducker/internal/model"
)

type ColdStorage struct {
	RootDir string
}

func NewColdStorage(rootDir string) *ColdStorage {
	return &ColdStorage{RootDir: rootDir}
}

func (c *ColdStorage) GetManifest(tenantID string) (model.Manifest, error) {
	p := filepath.Join(c.RootDir, tenantID, "manifest.json")
	b, err := os.ReadFile(p)
	if err != nil {
		return model.Manifest{}, err
	}
	var m model.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return model.Manifest{}, err
	}
	return m, nil
}

func (c *ColdStorage) GetBloom(tenantID, segment string) (map[string]any, error) {
	p := filepath.Join(c.RootDir, tenantID, "_bloom", segment+".bloom.json")
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var d map[string]any
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}
	return d, nil
}

func (c *ColdStorage) ListTenants() ([]string, error) {
	entries, err := os.ReadDir(c.RootDir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "tenant-") {
			out = append(out, e.Name())
		}
	}
	return out, nil
}
