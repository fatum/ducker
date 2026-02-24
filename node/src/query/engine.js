/**
 * Detect the search mode from a query.
 * Returns "fts" | "wildcard" | "structured"
 */
export function detectSearchMode(filters, search) {
  if (search) return 'basic';
  for (const value of Object.values(filters || {})) {
    if (typeof value === 'string' && (value.includes('*') || value.includes('?'))) {
      return 'wildcard';
    }
  }
  return 'structured';
}

/**
 * Build WHERE clause fragments from filters.
 * Returns an array of SQL condition strings.
 */
function buildWhereClauses(filters) {
  const clauses = [];

  for (const [key, value] of Object.entries(filters || {})) {
    const col = sanitizeColumnName(key);

    if (typeof value === 'string') {
      if (value.includes('*') || value.includes('?')) {
        const likePattern = value.replace(/\*/g, '%').replace(/\?/g, '_');
        clauses.push(`${col} LIKE '${escapeSql(likePattern)}'`);
      } else {
        clauses.push(`${col} = '${escapeSql(value)}'`);
      }
    } else if (typeof value === 'number') {
      clauses.push(`${col} = ${value}`);
    } else if (Array.isArray(value)) {
      const vals = value.map((v) => `'${escapeSql(String(v))}'`).join(', ');
      clauses.push(`${col} IN (${vals})`);
    } else if (typeof value === 'object' && value !== null) {
      if (value.gt !== undefined) clauses.push(`${col} > ${Number(value.gt)}`);
      if (value.gte !== undefined) clauses.push(`${col} >= ${Number(value.gte)}`);
      if (value.lt !== undefined) clauses.push(`${col} < ${Number(value.lt)}`);
      if (value.lte !== undefined) clauses.push(`${col} <= ${Number(value.lte)}`);
    }
  }

  return clauses;
}

function sanitizeColumnName(name) {
  return `"${name.replace(/"/g, '""')}"`;
}

function escapeSql(str) {
  return str.replace(/'/g, "''");
}

function tenantTableName(tenantId) {
  return tenantId.replace(/[^a-zA-Z0-9]/g, '_');
}

function segmentListSql(segments) {
  return segments.map((s) => `'${escapeSql(s)}'`).join(', ');
}

/**
 * Execute a structured or wildcard query on a tenant table.
 */
async function executeDirectQuery(conn, tableName, segments, filters, { limit = 100, offset = 0, startTs, endTs }) {
  const clauses = buildWhereClauses(filters);
  clauses.push(`"_segment" IN (${segmentListSql(segments)})`);
  if (startTs) clauses.push(`"timestamp" >= ${startTs}`);
  if (endTs) clauses.push(`"timestamp" <= ${endTs}`);

  const where = `WHERE ${clauses.join(' AND ')}`;

  const sql = `SELECT * EXCLUDE (_row_id, _segment) FROM ${tableName} ${where} ORDER BY "timestamp" DESC LIMIT ${Number(limit)} OFFSET ${Number(offset)}`;

  const reader = await conn.runAndReadAll(sql);
  return reader.getRowObjectsJson();
}

/**
 * Execute a basic search query using ILIKE on the message column.
 */
async function executeBasicSearchQuery(conn, tableName, segments, filters, search, { limit = 100, offset = 0, startTs, endTs }) {
  const clauses = buildWhereClauses(filters);
  clauses.push(`"_segment" IN (${segmentListSql(segments)})`);
  if (startTs) clauses.push(`"timestamp" >= ${startTs}`);
  if (endTs) clauses.push(`"timestamp" <= ${endTs}`);
  const searchEscaped = escapeSql(search);
  clauses.push(`"message" ILIKE '%${searchEscaped}%'`);
  const where = `WHERE ${clauses.join(' AND ')}`;

  const sql = `SELECT * EXCLUDE (_row_id, _segment) FROM ${tableName} ${where} ORDER BY "timestamp" DESC LIMIT ${Number(limit)} OFFSET ${Number(offset)}`;

  const reader = await conn.runAndReadAll(sql);
  return reader.getRowObjectsJson();
}

/**
 * Main query execution entry point.
 * @param {object} conn - DuckDB connection
 * @param {string} tenant - tenant ID (e.g. "tenant-1")
 * @param {string[]} segments - segment names to query (e.g. ["2025-01-15_00", "2025-01-15_01"])
 * @param {object} options - { filters, search, limit, offset, startTs, endTs }
 */
export async function execute(conn, tenant, segments, { filters, search, limit, offset, startTs, endTs }) {
  const start = performance.now();
  const tableName = tenantTableName(tenant);
  const searchMode = detectSearchMode(filters, search);

  let rows;
  if (searchMode === 'basic') {
    rows = await executeBasicSearchQuery(conn, tableName, segments, filters, search, { limit, offset, startTs, endTs });
  } else {
    rows = await executeDirectQuery(conn, tableName, segments, filters, { limit, offset, startTs, endTs });
  }

  const queryTimeMs = Math.round(performance.now() - start);

  return {
    rows,
    queryTimeMs,
    searchMode,
    filesScanned: segments.length,
    rowsMatched: rows.length,
  };
}
