package query

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ducker/internal/model"
	"ducker/internal/tenant"
)

func DetectSearchMode(filters map[string]any, search string) string {
	if search != "" {
		return "basic"
	}
	for _, value := range filters {
		if s, ok := value.(string); ok {
			if strings.Contains(s, "*") || strings.Contains(s, "?") {
				return "wildcard"
			}
		}
	}
	return "structured"
}

func Execute(conn *sql.DB, tenantID string, segments []string, filters map[string]any, search string, limit, offset int, startTS, endTS int64) (model.QueryResult, error) {
	start := time.Now()
	searchMode := DetectSearchMode(filters, search)
	if len(segments) == 0 {
		return model.QueryResult{Rows: []map[string]any{}, QueryTimeMS: 0, SearchMode: searchMode, FilesScanned: 0, RowsMatched: 0}, nil
	}

	table := tenant.TableName(tenantID)
	var (
		rows []map[string]any
		err  error
	)
	if searchMode == "basic" {
		rows, err = executeBasicSearchQuery(conn, table, segments, filters, search, limit, offset, startTS, endTS)
	} else {
		rows, err = executeDirectQuery(conn, table, segments, filters, limit, offset, startTS, endTS)
	}
	if err != nil {
		return model.QueryResult{}, err
	}

	return model.QueryResult{
		Rows:         rows,
		QueryTimeMS:  int64(time.Since(start) / time.Millisecond),
		SearchMode:   searchMode,
		FilesScanned: len(segments),
		RowsMatched:  len(rows),
	}, nil
}

func executeDirectQuery(conn *sql.DB, table string, segments []string, filters map[string]any, limit, offset int, startTS, endTS int64) ([]map[string]any, error) {
	clauses := buildWhereClauses(filters)
	clauses = append(clauses, fmt.Sprintf(`"_segment" IN (%s)`, segmentListSQL(segments)))
	if startTS > 0 {
		clauses = append(clauses, fmt.Sprintf(`"timestamp" >= %d`, startTS))
	}
	if endTS > 0 {
		clauses = append(clauses, fmt.Sprintf(`"timestamp" <= %d`, endTS))
	}
	where := "WHERE " + strings.Join(clauses, " AND ")
	sqlText := fmt.Sprintf(`SELECT * EXCLUDE (_row_id, _segment) FROM %s %s ORDER BY "timestamp" DESC LIMIT %d OFFSET %d`, table, where, limit, offset)
	return runRowMapQuery(conn, sqlText)
}

func executeBasicSearchQuery(conn *sql.DB, table string, segments []string, filters map[string]any, search string, limit, offset int, startTS, endTS int64) ([]map[string]any, error) {
	clauses := buildWhereClauses(filters)
	clauses = append(clauses, fmt.Sprintf(`"_segment" IN (%s)`, segmentListSQL(segments)))
	if startTS > 0 {
		clauses = append(clauses, fmt.Sprintf(`"timestamp" >= %d`, startTS))
	}
	if endTS > 0 {
		clauses = append(clauses, fmt.Sprintf(`"timestamp" <= %d`, endTS))
	}
	searchEscaped := escapeSQL(search)
	clauses = append(clauses, fmt.Sprintf(`"message" ILIKE '%%%s%%'`, searchEscaped))
	where := "WHERE " + strings.Join(clauses, " AND ")
	sqlText := fmt.Sprintf(`SELECT * EXCLUDE (_row_id, _segment) FROM %s %s ORDER BY "timestamp" DESC LIMIT %d OFFSET %d`, table, where, limit, offset)
	return runRowMapQuery(conn, sqlText)
}

func buildWhereClauses(filters map[string]any) []string {
	clauses := make([]string, 0)
	for key, value := range filters {
		col := sanitizeColumnName(key)
		switch v := value.(type) {
		case string:
			if strings.Contains(v, "*") || strings.Contains(v, "?") {
				likePattern := strings.ReplaceAll(strings.ReplaceAll(v, "*", "%"), "?", "_")
				clauses = append(clauses, fmt.Sprintf("%s LIKE '%s'", col, escapeSQL(likePattern)))
			} else {
				clauses = append(clauses, fmt.Sprintf("%s = '%s'", col, escapeSQL(v)))
			}
		case []any:
			vals := make([]string, 0, len(v))
			for _, item := range v {
				vals = append(vals, fmt.Sprintf("'%s'", escapeSQL(fmt.Sprintf("%v", item))))
			}
			clauses = append(clauses, fmt.Sprintf("%s IN (%s)", col, strings.Join(vals, ", ")))
		case map[string]any:
			appendRangeClauses(&clauses, col, v)
		}
	}
	return clauses
}

func appendRangeClauses(clauses *[]string, col string, v map[string]any) {
	if val, ok := v["gt"]; ok {
		*clauses = append(*clauses, fmt.Sprintf("%s > %s", col, numericLiteral(val)))
	}
	if val, ok := v["gte"]; ok {
		*clauses = append(*clauses, fmt.Sprintf("%s >= %s", col, numericLiteral(val)))
	}
	if val, ok := v["lt"]; ok {
		*clauses = append(*clauses, fmt.Sprintf("%s < %s", col, numericLiteral(val)))
	}
	if val, ok := v["lte"]; ok {
		*clauses = append(*clauses, fmt.Sprintf("%s <= %s", col, numericLiteral(val)))
	}
}

func numericLiteral(v any) string {
	switch n := v.(type) {
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(n), 'f', -1, 64)
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.FormatInt(n, 10)
	default:
		return "0"
	}
}

func runRowMapQuery(conn *sql.DB, sqlText string) ([]map[string]any, error) {
	rows, err := conn.Query(sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0)

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rowMap := map[string]any{}
		for i, c := range cols {
			rowMap[c] = normalizeDBValue(values[i])
		}
		result = append(result, rowMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func normalizeDBValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}

func sanitizeColumnName(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func TenantTableNameForCache(tenantID string) string {
	return tenant.TableName(tenantID)
}

func segmentListSQL(segments []string) string {
	vals := make([]string, 0, len(segments))
	for _, s := range segments {
		vals = append(vals, fmt.Sprintf("'%s'", escapeSQL(s)))
	}
	return strings.Join(vals, ", ")
}
