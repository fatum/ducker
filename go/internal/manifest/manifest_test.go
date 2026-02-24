package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCacheKeyIncludesRoot(t *testing.T) {
	Clear()
	tmp := t.TempDir()
	rootA := filepath.Join(tmp, "a")
	rootB := filepath.Join(tmp, "b")
	tenant := "tenant-1"

	if err := os.MkdirAll(filepath.Join(rootA, tenant), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rootB, tenant), 0o755); err != nil {
		t.Fatal(err)
	}

	dataA := `{"tenantId":"tenant-1","files":[{"path":"a.parquet","segment":"a","startTs":1,"endTs":2,"rowCount":1,"sizeBytes":1}]}`
	dataB := `{"tenantId":"tenant-1","files":[{"path":"b.parquet","segment":"b","startTs":3,"endTs":4,"rowCount":1,"sizeBytes":1}]}`
	if err := os.WriteFile(filepath.Join(rootA, tenant, "manifest.json"), []byte(dataA), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootB, tenant, "manifest.json"), []byte(dataB), 0o644); err != nil {
		t.Fatal(err)
	}

	mA, err := Load(rootA, tenant)
	if err != nil {
		t.Fatal(err)
	}
	mB, err := Load(rootB, tenant)
	if err != nil {
		t.Fatal(err)
	}

	if len(mA.Files) != 1 || mA.Files[0].Segment != "a" {
		t.Fatalf("unexpected rootA manifest: %+v", mA.Files)
	}
	if len(mB.Files) != 1 || mB.Files[0].Segment != "b" {
		t.Fatalf("unexpected rootB manifest: %+v", mB.Files)
	}
}
