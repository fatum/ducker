import { probeFileBloom } from '../bloom/bloom.js';

/**
 * Determines which files need to be scanned for a query.
 * Applies time-range filtering, then bloom filter pruning.
 */
export class QueryPlanner {
  constructor(coldStorage, dbCache) {
    this.coldStorage = coldStorage;
    this.dbCache = dbCache || null;
  }

  /**
   * Plan which files to query.
   * @param {object} manifest - tenant manifest with .files array
   * @param {object} query - { startTs, endTs, filters }
   * @returns {{ files: object[], stats: object }}
   */
  async plan(tenantId, manifest, query) {
    const { startTs, endTs, filters = {} } = query;
    const totalFiles = manifest.files.length;

    // 1. Time-range filter
    const timeFiltered = manifest.files.filter(
      (f) => f.endTs >= startTs && f.startTs <= endTs
    );

    // 2. Extract equality filters for bloom probing (skip wildcards/ranges)
    const bloomFilters = {};
    for (const [key, value] of Object.entries(filters)) {
      if (typeof value === 'string' && !value.includes('*') && !value.includes('?')) {
        bloomFilters[key] = value;
      } else if (Array.isArray(value) && value.every((v) => typeof v === 'string')) {
        bloomFilters[key] = value;
      }
      // skip range objects and wildcard patterns
    }

    // 3. Bloom filter pruning
    let bloomFiltered = timeFiltered;
    const hasBloomFilters = Object.keys(bloomFilters).length > 0;

    if (hasBloomFilters) {
      bloomFiltered = [];
      for (const file of timeFiltered) {
        const bloomData = await this._getBloomData(tenantId, file.segment);
        if (!bloomData || probeFileBloom(bloomData, bloomFilters)) {
          bloomFiltered.push(file);
        }
      }
    }

    return {
      files: bloomFiltered,
      stats: {
        totalFiles,
        filesAfterTimeFilter: timeFiltered.length,
        filesAfterBloom: bloomFiltered.length,
      },
    };
  }

  async _getBloomData(tenantId, segment) {
    // Try DuckDB cache first
    if (this.dbCache) {
      try {
        const data = await this.dbCache.getBloomData(tenantId, segment);
        if (data) return data;
      } catch {
        // fall through to cold storage
      }
    }

    // Fall back to cold storage
    try {
      return await this.coldStorage.getBloom(tenantId, segment);
    } catch {
      // No bloom available — can't prune
      return null;
    }
  }
}
