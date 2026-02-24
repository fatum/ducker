package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"ducker/internal/cache"
	"ducker/internal/manifest"
	"ducker/internal/model"
	"ducker/internal/query"
	"ducker/internal/storage"
)

type Server struct {
	coldStorageDir string
	coldStorage    *storage.ColdStorage
	dbCache        *cache.Cache
	planner        *query.Planner
}

func NewServer(coldStorageDir string, coldStorage *storage.ColdStorage, dbCache *cache.Cache, planner *query.Planner) *Server {
	return &Server{coldStorageDir: coldStorageDir, coldStorage: coldStorage, dbCache: dbCache, planner: planner}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Post("/query", s.handleQuery)
	r.Get("/tenants", s.handleTenants)
	r.Get("/stats", s.handleStats)
	return r
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req model.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	startTS, endTS, limit, offset, ok := validateQueryRequest(w, req)
	if !ok {
		return
	}
	if req.Filters == nil {
		req.Filters = map[string]any{}
	}

	m, err := manifest.Load(s.coldStorageDir, req.Tenant)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to load tenant manifest")
		return
	}

	plan, err := s.planner.Plan(req.Tenant, m, startTS, endTS, req.Filters)
	if err != nil {
		log.Printf("planner error: %v", err)
		writeError(w, http.StatusInternalServerError, "planner failed")
		return
	}

	s.dbCache.ResetCounters()
	if err := s.dbCache.EnsureCached(req.Tenant, plan.Files); err != nil {
		log.Printf("cache ensure error: %v", err)
		writeError(w, http.StatusInternalServerError, "cache ingestion failed")
		return
	}

	segments := make([]string, 0, len(plan.Files))
	for _, f := range plan.Files {
		segments = append(segments, f.Segment)
	}
	result, err := query.Execute(s.dbCache.DB(), req.Tenant, segments, req.Filters, req.Search, limit, offset, startTS, endTS)
	if err != nil {
		log.Printf("query execute error: %v", err)
		writeError(w, http.StatusInternalServerError, "query execution failed")
		return
	}

	if _, err := s.dbCache.Evict(1_000_000); err != nil {
		log.Printf("evict error: %v", err)
	}

	cacheStats := s.dbCache.Stats()
	resp := map[string]any{
		"rows": result.Rows,
		"stats": map[string]any{
			"totalFiles":           plan.Stats.TotalFiles,
			"filesAfterTimeFilter": plan.Stats.FilesAfterTimeFilter,
			"filesAfterBloom":      plan.Stats.FilesAfterBloom,
			"filesScanned":         result.FilesScanned,
			"rowsMatched":          result.RowsMatched,
			"queryTimeMs":          result.QueryTimeMS,
			"searchMode":           result.SearchMode,
			"cacheHits":            cacheStats.Hits,
			"cacheMisses":          cacheStats.Misses,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleTenants(w http.ResponseWriter, _ *http.Request) {
	tenants, err := s.coldStorage.ListTenants()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed listing tenants")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"cache": s.dbCache.Stats(), "coldStorageDir": s.coldStorageDir})
}

func validateQueryRequest(w http.ResponseWriter, req model.QueryRequest) (int64, int64, int, int, bool) {
	if req.Tenant == "" {
		writeError(w, http.StatusBadRequest, "tenant is required")
		return 0, 0, 0, 0, false
	}
	if req.From == "" || req.To == "" {
		writeError(w, http.StatusBadRequest, "from and to are required")
		return 0, 0, 0, 0, false
	}
	from, err := time.Parse(time.RFC3339, req.From)
	if err != nil {
		writeError(w, http.StatusBadRequest, "from must be RFC3339")
		return 0, 0, 0, 0, false
	}
	to, err := time.Parse(time.RFC3339, req.To)
	if err != nil {
		writeError(w, http.StatusBadRequest, "to must be RFC3339")
		return 0, 0, 0, 0, false
	}
	if from.After(to) {
		writeError(w, http.StatusBadRequest, "from must be <= to")
		return 0, 0, 0, 0, false
	}
	limit := 100
	if req.Limit != nil {
		limit = *req.Limit
	}
	offset := 0
	if req.Offset != nil {
		offset = *req.Offset
	}
	if limit <= 0 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "limit must be 1..1000")
		return 0, 0, 0, 0, false
	}
	if offset < 0 {
		writeError(w, http.StatusBadRequest, "offset must be >= 0")
		return 0, 0, 0, 0, false
	}
	return from.UnixMilli(), to.UnixMilli(), limit, offset, true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}
