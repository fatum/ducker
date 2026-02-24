package query

import (
	"database/sql"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

func TestExecuteEmptySegmentsReturnsEmptyResult(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	res, err := Execute(db, "tenant-1", []string{}, map[string]any{}, "", 100, 0, 1, 2)
	if err != nil {
		t.Fatalf("execute returned error: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(res.Rows))
	}
	if res.FilesScanned != 0 {
		t.Fatalf("expected files scanned=0, got %d", res.FilesScanned)
	}
}
