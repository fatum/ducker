/**
 * Simple bloom filter implementation for file-level column filtering.
 * Each parquet file gets one bloom filter per indexed column.
 * Uses Kirsch-Mitzenmacher double hashing: h(i) = h1 + i*h2
 */

const MIN_SIZE = 1024; // minimum bits to avoid degenerate small filters

function fnv1a(str) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash;
}

function murmurMix(str) {
  let h = 0x12345678;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x5bd1e995);
    h ^= h >>> 15;
  }
  return h >>> 0;
}

export class BloomFilter {
  constructor(size, hashCount) {
    this.size = size;
    this.hashCount = hashCount;
    this.bits = new Uint8Array(Math.ceil(size / 8));
  }

  add(value) {
    const str = String(value);
    const h1 = fnv1a(str);
    const h2 = murmurMix(str);
    for (let i = 0; i < this.hashCount; i++) {
      const bit = (h1 + i * h2) % this.size;
      this.bits[bit >> 3] |= 1 << (bit & 7);
    }
  }

  probe(value) {
    const str = String(value);
    const h1 = fnv1a(str);
    const h2 = murmurMix(str);
    for (let i = 0; i < this.hashCount; i++) {
      const bit = (h1 + i * h2) % this.size;
      if (!(this.bits[bit >> 3] & (1 << (bit & 7)))) {
        return false; // definitely absent
      }
    }
    return true; // maybe present
  }

  serialize() {
    return {
      size: this.size,
      hashCount: this.hashCount,
      bits: Buffer.from(this.bits).toString('base64'),
    };
  }

  static deserialize(data) {
    const bf = new BloomFilter(data.size, data.hashCount);
    bf.bits = new Uint8Array(Buffer.from(data.bits, 'base64'));
    return bf;
  }

  /**
   * Build a bloom filter from an array of values.
   * Auto-sizes based on expected element count for ~1% false positive rate.
   */
  static build(values, { falsePositiveRate = 0.01 } = {}) {
    const n = values.length || 1;
    const computed = Math.ceil((-n * Math.log(falsePositiveRate)) / (Math.LN2 * Math.LN2));
    const size = Math.max(MIN_SIZE, computed);
    const hashCount = Math.max(1, Math.round((size / n) * Math.LN2));
    const bf = new BloomFilter(size, hashCount);
    for (const v of values) {
      bf.add(v);
    }
    return bf;
  }
}

/**
 * Build file-level bloom filters for multiple columns.
 * Returns { columns: { colName: serialized bloom, ... } }
 */
export function buildFileBloom(columnValues) {
  const columns = {};
  for (const [col, values] of Object.entries(columnValues)) {
    const bf = BloomFilter.build(values);
    columns[col] = bf.serialize();
  }
  return { columns };
}

/**
 * Probe a file-level bloom for a set of equality filters.
 * Returns true if the file might contain matches, false if it definitely doesn't.
 */
function escapeSql(str) {
  return str.replace(/'/g, "''");
}

/**
 * Store a file-level bloom (from cold storage JSON) into DuckDB's _bloom_cache table.
 * Stores raw bytes as BLOB instead of base64.
 */
export async function storeBloomToDb(conn, tenant, segment, bloomData) {
  for (const [col, data] of Object.entries(bloomData.columns)) {
    const bits = Buffer.from(data.bits, 'base64');
    const hex = bits.toString('hex');
    await conn.run(
      `INSERT OR REPLACE INTO _bloom_cache VALUES ('${escapeSql(tenant)}', '${escapeSql(segment)}', '${escapeSql(col)}', ${data.size}, ${data.hashCount}, '\\x${hex}'::BLOB)`
    );
  }
}

/**
 * Load a file-level bloom from DuckDB's _bloom_cache table.
 * Returns the same { columns: { col: { size, hashCount, bits } } } format as JSON files,
 * or null if not found.
 */
export async function loadBloomFromDb(conn, tenant, segment) {
  const result = await conn.runAndReadAll(
    `SELECT column_name, filter_size, hash_count, encode(bits) AS bits_b64 FROM _bloom_cache WHERE tenant = '${escapeSql(tenant)}' AND segment = '${escapeSql(segment)}'`
  );
  const rows = result.getRowObjectsJson();
  if (rows.length === 0) return null;

  const columns = {};
  for (const row of rows) {
    columns[row.column_name] = {
      size: Number(row.filter_size),
      hashCount: Number(row.hash_count),
      bits: row.bits_b64,
    };
  }
  return { columns };
}

export function probeFileBloom(bloomData, filters) {
  for (const [col, value] of Object.entries(filters)) {
    if (!bloomData.columns[col]) continue; // no bloom for this column, can't skip
    const bf = BloomFilter.deserialize(bloomData.columns[col]);
    if (Array.isArray(value)) {
      // IN filter — file can be skipped only if ALL values are absent
      const anyPresent = value.some((v) => bf.probe(v));
      if (!anyPresent) return false;
    } else {
      if (!bf.probe(value)) return false;
    }
  }
  return true; // might contain matches
}
