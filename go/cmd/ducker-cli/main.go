package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"ducker/internal/cache"
	"ducker/internal/config"
	"ducker/internal/manifest"
	"ducker/internal/query"
	"ducker/internal/storage"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("get cwd: %v", err)
	}
	cfg := config.Load(cwd)

	tenant := flag.String("tenant", "", "tenant id")
	service := flag.String("service", "", "service filter")
	level := flag.String("level", "", "level filter")
	host := flag.String("host", "", "host filter")
	statusCode := flag.String("status_code", "", "status_code filter")
	requestPath := flag.String("request_path", "", "request_path filter")
	traceID := flag.String("trace_id", "", "trace_id filter")
	search := flag.String("search", "", "search in message")
	last := flag.String("last", "24h", "time window e.g. 24h, 7d")
	limit := flag.Int("limit", 20, "limit")
	flag.Parse()

	if *tenant == "" {
		log.Fatalf("usage: ducker-cli --tenant <id> [--service X] [--level X] [--search text] [--last 24h]")
	}
	if *limit <= 0 || *limit > 1000 {
		log.Fatalf("invalid --limit, expected 1..1000")
	}

	dur, err := parseLast(*last)
	if err != nil {
		log.Fatalf("invalid --last: %v", err)
	}

	m, err := manifest.Load(cfg.ColdStorageDir, *tenant)
	if err != nil {
		log.Fatalf("load manifest: %v", err)
	}
	if len(m.Files) == 0 {
		log.Fatalf("tenant %s has empty manifest", *tenant)
	}

	maxTS := m.Files[0].EndTS
	for _, f := range m.Files {
		if f.EndTS > maxTS {
			maxTS = f.EndTS
		}
	}
	endTS := maxTS
	startTS := endTS - dur.Milliseconds()

	filters := map[string]any{}
	setIf(filters, "service", *service)
	setIf(filters, "level", *level)
	setIf(filters, "host", *host)
	setIf(filters, "status_code", *statusCode)
	setIf(filters, "request_path", *requestPath)
	setIf(filters, "trace_id", *traceID)

	coldStorage := storage.NewColdStorage(cfg.ColdStorageDir)
	dbCache := cache.New(cfg.DuckDBPath, cfg.ColdStorageDir)
	if err := dbCache.Init(); err != nil {
		log.Fatalf("init cache: %v", err)
	}
	defer func() {
		if err := dbCache.Close(); err != nil {
			log.Printf("close cache: %v", err)
		}
	}()

	planner := query.NewPlanner(coldStorage, dbCache)
	plan, err := planner.Plan(*tenant, m, startTS, endTS, filters)
	if err != nil {
		log.Fatalf("plan: %v", err)
	}

	dbCache.ResetCounters()
	if err := dbCache.EnsureCached(*tenant, plan.Files); err != nil {
		log.Fatalf("ensure cache: %v", err)
	}
	segments := make([]string, 0, len(plan.Files))
	for _, f := range plan.Files {
		segments = append(segments, f.Segment)
	}
	result, err := query.Execute(dbCache.DB(), *tenant, segments, filters, *search, *limit, 0, startTS, endTS)
	if err != nil {
		log.Fatalf("execute: %v", err)
	}
	cacheStats := dbCache.Stats()

	fmt.Printf("\nQuery: tenant=%s range=%s -> %s\n", *tenant, msToISO(startTS), msToISO(endTS))
	if len(filters) > 0 {
		fmt.Printf("Filters: %#v\n", filters)
	}
	if *search != "" {
		fmt.Printf("Search: %q\n", *search)
	}

	fmt.Println("\n--- Stats ---")
	fmt.Printf("  Total files:          %d\n", plan.Stats.TotalFiles)
	fmt.Printf("  After time filter:    %d\n", plan.Stats.FilesAfterTimeFilter)
	fmt.Printf("  After bloom filter:   %d\n", plan.Stats.FilesAfterBloom)
	fmt.Printf("  Files scanned:        %d\n", result.FilesScanned)
	fmt.Printf("  Rows matched:         %d\n", result.RowsMatched)
	fmt.Printf("  Query time:           %dms\n", result.QueryTimeMS)
	fmt.Printf("  Search mode:          %s\n", result.SearchMode)
	fmt.Printf("  Cache hits/misses:    %d/%d\n", cacheStats.Hits, cacheStats.Misses)

	fmt.Println("\n--- Results ---")
	if len(result.Rows) == 0 {
		fmt.Println("  (no results)")
		return
	}
	for _, row := range result.Rows {
		ts := fmt.Sprintf("%v", row["timestamp"])
		tsVal, _ := strconv.ParseInt(ts, 10, 64)
		score := ""
		if v, ok := row["score"]; ok {
			score = fmt.Sprintf(" score=%.3f", toFloat(v))
		}
		fmt.Printf("  [%s] [%v] [%v] %v%s\n", msToISO(tsVal), row["level"], row["service"], row["message"], score)
	}
}

func parseLast(v string) (time.Duration, error) {
	if strings.HasSuffix(v, "h") {
		n, err := strconv.Atoi(strings.TrimSuffix(v, "h"))
		if err != nil {
			return 0, err
		}
		return time.Duration(n) * time.Hour, nil
	}
	if strings.HasSuffix(v, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(v, "d"))
		if err != nil {
			return 0, err
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return 0, fmt.Errorf("expected <n>h or <n>d")
}

func setIf(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}

func msToISO(ms int64) string {
	if ms <= 0 {
		return "invalid"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	default:
		return 0
	}
}
