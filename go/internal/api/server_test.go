package api

import (
	"net/http/httptest"
	"testing"

	"ducker/internal/model"
)

func TestValidateQueryRequest(t *testing.T) {
	w := httptest.NewRecorder()
	_, _, _, _, ok := validateQueryRequest(w, model.QueryRequest{})
	if ok {
		t.Fatalf("expected invalid empty request")
	}

	w = httptest.NewRecorder()
	limit := 50
	offset := 2
	start, end, l, o, ok := validateQueryRequest(w, model.QueryRequest{
		Tenant: "tenant-1",
		From:   "2025-01-15T00:00:00Z",
		To:     "2025-01-15T01:00:00Z",
		Limit:  &limit,
		Offset: &offset,
	})
	if !ok {
		t.Fatalf("expected valid request")
	}
	if start <= 0 || end <= start || l != 50 || o != 2 {
		t.Fatalf("unexpected parsed values: start=%d end=%d limit=%d offset=%d", start, end, l, o)
	}
}
