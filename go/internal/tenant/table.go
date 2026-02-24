package tenant

import (
	"fmt"
	"hash/fnv"
	"regexp"
)

var identifierSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func TableName(tenantID string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	suffix := fmt.Sprintf("%x", h.Sum64())[:12]
	cleaned := identifierSanitizer.ReplaceAllString(tenantID, "_")
	if len(cleaned) > 32 {
		cleaned = cleaned[:32]
	}
	return "tenant_" + cleaned + "_" + suffix
}
