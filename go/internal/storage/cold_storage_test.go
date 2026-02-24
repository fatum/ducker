package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestColdStorageReadAndList(t *testing.T) {
	root := t.TempDir()
	tenant := filepath.Join(root, "tenant-1")
	if err := os.MkdirAll(filepath.Join(tenant, "_bloom"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tenant, "manifest.json"), []byte(`{"tenantId":"tenant-1","files":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tenant, "_bloom", "seg-1.bloom.json"), []byte(`{"columns":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "not-a-tenant"), 0o755); err != nil {
		t.Fatal(err)
	}

	cs := NewColdStorage(root)
	m, err := cs.GetManifest("tenant-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.TenantID != "tenant-1" {
		t.Fatalf("unexpected manifest tenant: %s", m.TenantID)
	}
	if _, err := cs.GetBloom("tenant-1", "seg-1"); err != nil {
		t.Fatal(err)
	}
	list, err := cs.ListTenants()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0] != "tenant-1" {
		t.Fatalf("unexpected tenants: %+v", list)
	}
}
