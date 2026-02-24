package model

type SegmentFile struct {
	Path      string `json:"path"`
	Segment   string `json:"segment"`
	StartTS   int64  `json:"startTs"`
	EndTS     int64  `json:"endTs"`
	RowCount  int64  `json:"rowCount"`
	SizeBytes int64  `json:"sizeBytes"`
}

type Manifest struct {
	TenantID  string        `json:"tenantId"`
	CreatedAt string        `json:"createdAt,omitempty"`
	Files     []SegmentFile `json:"files"`
}

type PlanStats struct {
	TotalFiles           int `json:"totalFiles"`
	FilesAfterTimeFilter int `json:"filesAfterTimeFilter"`
	FilesAfterBloom      int `json:"filesAfterBloom"`
}

type QueryPlan struct {
	Files []SegmentFile `json:"files"`
	Stats PlanStats     `json:"stats"`
}

type QueryResult struct {
	Rows         []map[string]any `json:"rows"`
	QueryTimeMS  int64            `json:"queryTimeMs"`
	SearchMode   string           `json:"searchMode"`
	FilesScanned int              `json:"filesScanned"`
	RowsMatched  int              `json:"rowsMatched"`
}

type CacheStats struct {
	Hits    int     `json:"hits"`
	Misses  int     `json:"misses"`
	HitRate float64 `json:"hitRate"`
}

type QueryRequest struct {
	Tenant  string         `json:"tenant"`
	From    string         `json:"from"`
	To      string         `json:"to"`
	Filters map[string]any `json:"filters"`
	Search  string         `json:"search"`
	Limit   *int           `json:"limit,omitempty"`
	Offset  *int           `json:"offset,omitempty"`
}
