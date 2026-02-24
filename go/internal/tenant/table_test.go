package tenant

import "testing"

func TestTableNameDeterministicAndCollisionSafe(t *testing.T) {
	a := TableName("tenant-a")
	b := TableName("tenant_a")
	if a == b {
		t.Fatalf("expected different table names, got same: %s", a)
	}
	if TableName("tenant-a") != a {
		t.Fatalf("expected deterministic output")
	}
}
