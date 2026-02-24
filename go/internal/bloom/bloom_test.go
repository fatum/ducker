package bloom

import (
	"encoding/base64"
	"testing"
)

func TestFilterAddProbe(t *testing.T) {
	f := NewFilter(2048, 4)
	f.Add("auth")
	f.Add("api")

	if !f.Probe("auth") {
		t.Fatalf("expected auth to be present")
	}
	if !f.Probe("api") {
		t.Fatalf("expected api to be present")
	}
}

func TestParseAndProbeFileBloom(t *testing.T) {
	f := NewFilter(2048, 4)
	f.Add("auth")
	f.Add("api")

	raw := map[string]any{
		"columns": map[string]any{
			"service": map[string]any{
				"size":      float64(f.Size),
				"hashCount": float64(f.HashCount),
				"bits":      base64.StdEncoding.EncodeToString(f.Bits),
			},
		},
	}

	parsed, err := ParseFileBloom(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if !ProbeFileBloom(parsed, map[string]any{"service": "auth"}) {
		t.Fatalf("expected bloom probe to pass for auth")
	}
	if ProbeFileBloom(parsed, map[string]any{"service": "definitely_missing_value_xyz"}) {
		t.Fatalf("expected bloom probe to fail for definitely missing value")
	}
}
