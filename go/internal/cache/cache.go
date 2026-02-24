package cache

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"ducker/internal/bloom"
	"ducker/internal/model"
	tenantpkg "ducker/internal/tenant"
)

type Cache struct {
	dbPath          string
	coldStorageRoot string
	db              *sql.DB
	hits            int
	misses          int
	knownTables     map[string]bool
	knownTablesMu   sync.Mutex
	segmentLocks    map[string]*sync.Mutex
	segmentLocksMu  sync.Mutex
	countersMu      sync.Mutex
}

func New(dbPath, coldStorageRoot string) *Cache {
	return &Cache{
		dbPath:          dbPath,
		coldStorageRoot: coldStorageRoot,
		knownTables:     map[string]bool{},
		segmentLocks:    map[string]*sync.Mutex{},
	}
}

func (c *Cache) Init() error {
	if err := os.MkdirAll(filepath.Dir(c.dbPath), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("duckdb", c.dbPath)
	if err != nil {
		return err
	}
	c.db = db

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS _cache_segments (
			tenant VARCHAR,
			segment VARCHAR,
			cached_at BIGINT,
			last_accessed BIGINT,
			row_count INTEGER,
			PRIMARY KEY (tenant, segment)
		)`,
		`CREATE TABLE IF NOT EXISTS _bloom_cache (
			tenant VARCHAR,
			segment VARCHAR,
			column_name VARCHAR,
			filter_size INTEGER,
			hash_count INTEGER,
			bits BLOB,
			PRIMARY KEY (tenant, segment, column_name)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := c.db.Exec(stmt); err != nil {
			_ = c.db.Close()
			return err
		}
	}
	return nil
}

func (c *Cache) Close() error {
	if c.db == nil {
		return nil
	}
	return c.db.Close()
}

func (c *Cache) DB() *sql.DB {
	return c.db
}

func (c *Cache) segmentLock(tenant, segment string) *sync.Mutex {
	key := tenant + "::" + segment
	c.segmentLocksMu.Lock()
	defer c.segmentLocksMu.Unlock()
	if m, ok := c.segmentLocks[key]; ok {
		return m
	}
	m := &sync.Mutex{}
	c.segmentLocks[key] = m
	return m
}

func (c *Cache) EnsureCached(tenant string, files []model.SegmentFile) error {
	table := tenantpkg.TableName(tenant)
	for _, file := range files {
		lock := c.segmentLock(tenant, file.Segment)
		lock.Lock()

		cached, err := c.isSegmentCached(tenant, file.Segment)
		if err != nil {
			lock.Unlock()
			return err
		}
		if cached {
			c.incrementHits()
			lock.Unlock()
			continue
		}

		if err := c.createTenantTableIfNeeded(table, file.Path); err != nil {
			lock.Unlock()
			return err
		}

		coldPath := filepath.Join(c.coldStorageRoot, file.Path)
		insertSQL := fmt.Sprintf(
			`INSERT INTO %s SELECT *, '%s' AS _segment FROM read_parquet('%s')`,
			table, escapeSQL(file.Segment), escapeSQL(coldPath),
		)
		if _, err := c.db.Exec(insertSQL); err != nil {
			lock.Unlock()
			return err
		}

		var rowCount int64
		countSQL := fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQL(coldPath))
		if err := c.db.QueryRow(countSQL).Scan(&rowCount); err != nil {
			lock.Unlock()
			return err
		}

		now := time.Now().UnixMilli()
		_, err = c.db.Exec(
			`INSERT OR REPLACE INTO _cache_segments VALUES (?, ?, ?, ?, ?)`,
			tenant, file.Segment, now, now, rowCount,
		)
		if err != nil {
			lock.Unlock()
			return err
		}

		if err := c.cacheBloom(tenant, file.Segment); err != nil {
			lock.Unlock()
			return err
		}

		c.incrementMisses()
		lock.Unlock()
	}

	if len(files) > 0 {
		now := time.Now().UnixMilli()
		for _, f := range files {
			if _, err := c.db.Exec(`UPDATE _cache_segments SET last_accessed = ? WHERE tenant = ? AND segment = ?`, now, tenant, f.Segment); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Cache) isSegmentCached(tenant, segment string) (bool, error) {
	var one int
	err := c.db.QueryRow(`SELECT 1 FROM _cache_segments WHERE tenant = ? AND segment = ? LIMIT 1`, tenant, segment).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Cache) createTenantTableIfNeeded(table, samplePath string) error {
	c.knownTablesMu.Lock()
	known := c.knownTables[table]
	c.knownTablesMu.Unlock()
	if known {
		return nil
	}

	coldPath := filepath.Join(c.coldStorageRoot, samplePath)
	stmt := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s AS SELECT *, ''::VARCHAR AS _segment FROM read_parquet('%s') WHERE false`,
		table, escapeSQL(coldPath),
	)
	if _, err := c.db.Exec(stmt); err != nil {
		return err
	}

	c.knownTablesMu.Lock()
	c.knownTables[table] = true
	c.knownTablesMu.Unlock()
	return nil
}

func (c *Cache) cacheBloom(tenant, segment string) error {
	p := filepath.Join(c.coldStorageRoot, tenant, "_bloom", segment+".bloom.json")
	b, err := os.ReadFile(p)
	if err != nil {
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil
	}
	parsed, err := bloom.ParseFileBloom(raw)
	if err != nil {
		return nil
	}
	return bloom.StoreToDB(c.db, tenant, segment, parsed)
}

func (c *Cache) GetBloomData(tenantID, segment string) (bloom.FileBloom, bool, error) {
	return bloom.LoadFromDB(c.db, tenantID, segment)
}

func (c *Cache) Evict(maxRows int64) (int, error) {
	if maxRows <= 0 {
		maxRows = 1_000_000
	}
	var total sql.NullInt64
	if err := c.db.QueryRow(`SELECT SUM(row_count) AS total FROM _cache_segments`).Scan(&total); err != nil {
		return 0, err
	}
	curr := int64(0)
	if total.Valid {
		curr = total.Int64
	}
	if curr <= int64(float64(maxRows)*0.8) {
		return 0, nil
	}

	rows, err := c.db.Query(`SELECT tenant, segment, row_count FROM _cache_segments ORDER BY last_accessed ASC`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	target := int64(float64(maxRows) * 0.6)
	remaining := curr
	evicted := 0
	for rows.Next() {
		if remaining <= target {
			break
		}
		var tenantID, segment string
		var rowCount int64
		if err := rows.Scan(&tenantID, &segment, &rowCount); err != nil {
			return evicted, err
		}
		table := tenantpkg.TableName(tenantID)
		if _, err := c.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE _segment = '%s'`, table, escapeSQL(segment))); err != nil {
			return evicted, err
		}
		if _, err := c.db.Exec(`DELETE FROM _bloom_cache WHERE tenant = ? AND segment = ?`, tenantID, segment); err != nil {
			return evicted, err
		}
		if _, err := c.db.Exec(`DELETE FROM _cache_segments WHERE tenant = ? AND segment = ?`, tenantID, segment); err != nil {
			return evicted, err
		}
		remaining -= rowCount
		evicted++
	}
	if err := rows.Err(); err != nil {
		return evicted, err
	}
	return evicted, nil
}

func (c *Cache) ResetCounters() {
	c.countersMu.Lock()
	defer c.countersMu.Unlock()
	c.hits = 0
	c.misses = 0
}

func (c *Cache) Stats() model.CacheStats {
	c.countersMu.Lock()
	defer c.countersMu.Unlock()
	den := c.hits + c.misses
	hitRate := 0.0
	if den > 0 {
		hitRate = float64(c.hits) / float64(den)
	}
	return model.CacheStats{Hits: c.hits, Misses: c.misses, HitRate: hitRate}
}

func (c *Cache) incrementHits() {
	c.countersMu.Lock()
	c.hits++
	c.countersMu.Unlock()
}

func (c *Cache) incrementMisses() {
	c.countersMu.Lock()
	c.misses++
	c.countersMu.Unlock()
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
