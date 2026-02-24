package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

type benchmarkQuery struct {
	Name string
	SQL  string
}

func main() {
	rows := flag.Int64("rows", 1_000_000_000, "Number of rows to generate")
	segments := flag.Int("segments", 1000, "Number of segments")
	force := flag.Bool("force", false, "Force recreation of data")
	memory := flag.String("memory", "2GB", "DuckDB memory limit")
	mmap := flag.Bool("mmap", false, "Enable mmap (accepted for parity; currently informational)")
	flag.Parse()

	if *rows <= 0 {
		log.Fatal("--rows must be > 0")
	}
	if *segments <= 0 {
		log.Fatal("--segments must be > 0")
	}

	projectRoot, err := os.Getwd()
	if err != nil {
		log.Fatalf("get cwd: %v", err)
	}
	benchmarkDir := filepath.Join(projectRoot, "benchmark-data")
	if err := os.MkdirAll(benchmarkDir, 0o755); err != nil {
		log.Fatalf("mkdir benchmark dir: %v", err)
	}
	dbPath := filepath.Join(benchmarkDir, "benchmark.duckdb")

	printHeader(*rows, *segments, *memory, *mmap)

	if *force {
		if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
			log.Fatalf("remove existing db: %v", err)
		}
		fmt.Println("Removed existing benchmark database (--force)")
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	mustExec(db, "INSTALL fts;")
	mustExec(db, "LOAD fts;")
	mustExec(db, fmt.Sprintf("SET memory_limit='%s'", escapeSingleQuoted(*memory)))
	if *mmap {
		fmt.Println("mmap flag accepted for parity; no explicit DuckDB mmap toggle is set by this benchmark")
	}

	const tableName = "benchmark_logs"
	count, hasTable := tableCount(db, tableName)

	var genTime, ftsTime time.Duration
	if !hasTable || count == 0 {
		fmt.Printf("\n[1/3] Generating %s rows across %d segments...\n", formatInt(*rows), *segments)
		start := time.Now()
		generateData(db, tableName, *rows, *segments)
		genTime = time.Since(start)
		fmt.Printf("Generation complete in %s\n", formatDuration(genTime))

		count, _ = tableCount(db, tableName)
		fmt.Printf("Actual row count: %s\n", formatInt(count))

		fmt.Println("[2/3] Building FTS index...")
		ftsStart := time.Now()
		mustExec(db, fmt.Sprintf("PRAGMA create_fts_index('%s', '_row_id', 'message', stemmer='porter', stopwords='english', overwrite=1)", tableName))
		ftsTime = time.Since(ftsStart)
		fmt.Printf("FTS index built in %s\n", formatDuration(ftsTime))
	} else {
		fmt.Printf("\n[1/3] Reusing existing table with %s rows\n", formatInt(count))
		fmt.Println("[2/3] Skipping data generation and FTS build")
	}

	fmt.Println("[3/3] Running query benchmarks...")
	printMem()

	queries := benchmarkQueries(tableName)
	for i, q := range queries {
		start := time.Now()
		rowCount := runQueryCount(db, q.SQL)
		d := time.Since(start)
		fmt.Printf("%2d. %-45s %10s  rows=%s\n", i+1, q.Name, formatDuration(d), formatInt(rowCount))
	}

	fmt.Println("\nSummary")
	fmt.Printf("Rows: %s\n", formatInt(count))
	fmt.Printf("Data generation: %s\n", formatDuration(genTime))
	fmt.Printf("FTS build: %s\n", formatDuration(ftsTime))
}

func printHeader(rows int64, segments int, memory string, mmap bool) {
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Ducker Go Benchmark")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total Rows: %s\n", formatInt(rows))
	fmt.Printf("Segments: %d\n", segments)
	fmt.Printf("Memory Limit: %s\n", memory)
	fmt.Printf("Mmap: %v\n", mmap)
	fmt.Println(strings.Repeat("=", 60))
}

func generateData(db *sql.DB, table string, totalRows int64, segments int) {
	rowsPerSegment := int64(math.Ceil(float64(totalRows) / float64(segments)))
	mustExec(db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	mustExec(db, fmt.Sprintf(`
CREATE TABLE %s AS
WITH base AS (
  SELECT
    i,
    CAST(floor(i / %d) AS BIGINT) AS seg_idx
  FROM generate_series(0, %d) AS t(i)
)
SELECT
  1704067200000 + (seg_idx * 3600000) + (i %% 3600000) AS "timestamp",
  ['api','auth','web','worker','scheduler','billing','notifications'][1 + (i %% 7)] AS service,
  CASE i %% 100
    WHEN 0 THEN 'debug'
    WHEN 1 THEN 'debug'
    WHEN 2 THEN 'debug'
    WHEN 3 THEN 'debug'
    WHEN 4 THEN 'debug'
    WHEN 5 THEN 'debug'
    WHEN 6 THEN 'debug'
    WHEN 7 THEN 'debug'
    WHEN 8 THEN 'debug'
    WHEN 9 THEN 'debug'
    WHEN 10 THEN 'error'
    WHEN 11 THEN 'error'
    WHEN 12 THEN 'error'
    WHEN 13 THEN 'fatal'
    ELSE 'info'
  END AS level,
  'host-' || lpad(((i %% 20) + 1)::VARCHAR, 3, '0') AS host,
  substr(md5(i::VARCHAR), 1, 8) || '-' || substr(md5((i+1)::VARCHAR), 1, 4) || '-' || substr(md5((i+2)::VARCHAR), 1, 4) AS trace_id,
  'Request completed in ' || (i %% 5000) || 'ms' AS message,
  CASE i %% 20
    WHEN 0 THEN 400
    WHEN 1 THEN 401
    WHEN 2 THEN 403
    WHEN 3 THEN 404
    WHEN 4 THEN 500
    WHEN 5 THEN 502
    WHEN 6 THEN 503
    ELSE 200
  END AS status_code,
  (i %% 2000)::DOUBLE + 1.0 AS duration_ms,
  ['/api/users', '/api/users/profile', '/api/orders', '/api/products', '/api/auth/login', '/api/health'][1 + (i %% 6)] AS request_path,
  'seg_' || lpad(seg_idx::VARCHAR, 5, '0') AS _segment,
  i AS _row_id
FROM base
`, table, rowsPerSegment, totalRows-1))
}

func benchmarkQueries(table string) []benchmarkQuery {
	return []benchmarkQuery{
		{Name: "Full table scan (count)", SQL: fmt.Sprintf("SELECT count(*) FROM %s", table)},
		{Name: "Filter by service = api", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE service = 'api'", table)},
		{Name: "Filter by level IN (error,fatal)", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE level IN ('error', 'fatal')", table)},
		{Name: "Filter by status_code >= 500", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE status_code >= 500", table)},
		{Name: "Filter by duration_ms > 1000", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE duration_ms > 1000", table)},
		{Name: "Filter by host specific", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE host = 'host-005'", table)},
		{Name: "Complex filter service+level", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE service = 'api' AND level = 'error'", table)},
		{Name: "Time range filter", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE \"timestamp\" BETWEEN 1704067200000 AND 1704153600000", table)},
		{Name: "Group by service", SQL: fmt.Sprintf("SELECT count(*) FROM (SELECT service, count(*) FROM %s GROUP BY service)", table)},
		{Name: "LIMIT query", SQL: fmt.Sprintf("SELECT count(*) FROM (SELECT * FROM %s LIMIT 100)", table)},
		{Name: "Segment single", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE _segment = 'seg_00000'", table)},
		{Name: "Segment 10", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE _segment IN ('seg_00000','seg_00001','seg_00002','seg_00003','seg_00004','seg_00005','seg_00006','seg_00007','seg_00008','seg_00009')", table)},
		{Name: "Wildcard request_path", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE request_path LIKE '/api/users%%'", table)},
		{Name: "Wildcard message", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE message LIKE '%%completed%%'", table)},
		{Name: "Wildcard host", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE host LIKE 'host-00_'", table)},
		{Name: "contains completed", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE contains(message, 'completed')", table)},
		{Name: "contains request", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE contains(message, 'Request')", table)},
		{Name: "contains + service", SQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE contains(message, 'completed') AND service = 'api'", table)},
	}
}

func tableCount(db *sql.DB, table string) (int64, bool) {
	var count int64
	err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count)
	if err != nil {
		return 0, false
	}
	return count, true
}

func runQueryCount(db *sql.DB, sqlText string) int64 {
	var count int64
	if err := db.QueryRow(sqlText).Scan(&count); err != nil {
		log.Fatalf("query failed: %v\nSQL: %s", err, sqlText)
	}
	return count
}

func mustExec(db *sql.DB, stmt string) {
	if _, err := db.Exec(stmt); err != nil {
		log.Fatalf("exec failed: %v\nSQL: %s", err, stmt)
	}
}

func printMem() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Initial Memory: heap=%s alloc=%s\n", formatBytes(m.HeapSys), formatBytes(m.Alloc))
}

func formatDuration(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	if ms < 60000 {
		return fmt.Sprintf("%.2fs", float64(ms)/1000.0)
	}
	return fmt.Sprintf("%.2fmin", float64(ms)/60000.0)
}

func formatBytes(v uint64) string {
	if v < 1024 {
		return fmt.Sprintf("%dB", v)
	}
	if v < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(v)/1024.0)
	}
	if v < 1024*1024*1024 {
		return fmt.Sprintf("%.1fMB", float64(v)/1024.0/1024.0)
	}
	return fmt.Sprintf("%.2fGB", float64(v)/1024.0/1024.0/1024.0)
}

func formatInt(v int64) string {
	s := strconv.FormatInt(v, 10)
	n := len(s)
	if n <= 3 {
		return s
	}
	var b strings.Builder
	mod := n % 3
	if mod > 0 {
		b.WriteString(s[:mod])
		if n > mod {
			b.WriteByte(',')
		}
	}
	for i := mod; i < n; i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < n {
			b.WriteByte(',')
		}
	}
	return b.String()
}

func escapeSingleQuoted(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
