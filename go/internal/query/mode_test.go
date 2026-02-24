package query

import "testing"

func TestDetectSearchMode(t *testing.T) {
	if got := DetectSearchMode(map[string]any{"service": "auth"}, ""); got != "structured" {
		t.Fatalf("expected structured, got %s", got)
	}
	if got := DetectSearchMode(map[string]any{"request_path": "/api/*"}, ""); got != "wildcard" {
		t.Fatalf("expected wildcard, got %s", got)
	}
	if got := DetectSearchMode(map[string]any{"service": "auth"}, "timeout"); got != "basic" {
		t.Fatalf("expected basic, got %s", got)
	}
}
