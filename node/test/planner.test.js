import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryPlanner } from '../src/query/planner.js';
import { buildFileBloom } from '../src/bloom/bloom.js';

// Mock cold storage that returns pre-built bloom data
class MockColdStorage {
  constructor(blooms = {}) {
    this.blooms = blooms;
  }

  async getBloom(tenantId, segmentName) {
    const key = `${tenantId}/${segmentName}`;
    if (this.blooms[key]) return this.blooms[key];
    throw new Error(`No bloom for ${key}`);
  }
}

function makeManifest(files) {
  return { tenantId: 'tenant-1', files };
}

function makeFile(segment, startTs, endTs) {
  return {
    path: `tenant-1/year=2025/month=01/day=15/${segment}.parquet`,
    segment,
    startTs,
    endTs,
    rowCount: 10000,
    sizeBytes: 1024000,
  };
}

describe('QueryPlanner', () => {
  const hour = 3600000;
  const base = new Date('2025-01-15T00:00:00Z').getTime();

  // Create 24 hourly files
  const files = Array.from({ length: 24 }, (_, i) => {
    return makeFile(`2025-01-15_${String(i).padStart(2, '0')}`, base + i * hour, base + (i + 1) * hour - 1);
  });

  describe('time filtering', () => {
    it('should return only files overlapping the time range', async () => {
      const planner = new QueryPlanner(new MockColdStorage());
      const manifest = makeManifest(files);

      // Query hours 2-5
      const result = await planner.plan('tenant-1', manifest, {
        startTs: base + 2 * hour,
        endTs: base + 5 * hour,
        filters: {},
      });

      assert.equal(result.stats.totalFiles, 24);
      assert.equal(result.stats.filesAfterTimeFilter, 4); // hours 2,3,4,5
    });

    it('should return all files for full range', async () => {
      const planner = new QueryPlanner(new MockColdStorage());
      const manifest = makeManifest(files);

      const result = await planner.plan('tenant-1', manifest, {
        startTs: base,
        endTs: base + 24 * hour,
        filters: {},
      });

      assert.equal(result.stats.filesAfterTimeFilter, 24);
    });
  });

  describe('bloom filtering', () => {
    it('should prune files that definitely lack the filtered value', async () => {
      // Files 0-11: service includes 'auth'
      // Files 12-23: service does NOT include 'auth'
      const blooms = {};
      for (let i = 0; i < 24; i++) {
        const segment = `2025-01-15_${String(i).padStart(2, '0')}`;
        const services = i < 12 ? ['api', 'auth', 'web'] : ['api', 'web', 'billing'];
        blooms[`tenant-1/${segment}`] = buildFileBloom({ service: services, level: ['info', 'error'] });
      }

      const planner = new QueryPlanner(new MockColdStorage(blooms));
      const manifest = makeManifest(files);

      const result = await planner.plan('tenant-1', manifest, {
        startTs: base,
        endTs: base + 24 * hour,
        filters: { service: 'auth' },
      });

      assert.equal(result.stats.filesAfterTimeFilter, 24);
      assert.equal(result.stats.filesAfterBloom, 12); // only first 12 have 'auth'
    });

    it('should not prune on wildcard filters', async () => {
      const blooms = {};
      for (let i = 0; i < 24; i++) {
        const segment = `2025-01-15_${String(i).padStart(2, '0')}`;
        blooms[`tenant-1/${segment}`] = buildFileBloom({ service: ['api'] });
      }

      const planner = new QueryPlanner(new MockColdStorage(blooms));
      const manifest = makeManifest(files);

      const result = await planner.plan('tenant-1', manifest, {
        startTs: base,
        endTs: base + 24 * hour,
        filters: { service: 'a*' }, // wildcard — should NOT bloom prune
      });

      assert.equal(result.stats.filesAfterBloom, 24);
    });

    it('should handle missing bloom files gracefully', async () => {
      const planner = new QueryPlanner(new MockColdStorage({})); // no blooms at all
      const manifest = makeManifest(files);

      const result = await planner.plan('tenant-1', manifest, {
        startTs: base,
        endTs: base + 24 * hour,
        filters: { service: 'auth' },
      });

      // Can't prune without blooms, all files included
      assert.equal(result.stats.filesAfterBloom, 24);
    });
  });
});
