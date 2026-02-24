import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BloomFilter, buildFileBloom, probeFileBloom } from '../src/bloom/bloom.js';

describe('BloomFilter', () => {
  it('should return true for added values', () => {
    const bf = BloomFilter.build(['apple', 'banana', 'cherry']);
    assert.equal(bf.probe('apple'), true);
    assert.equal(bf.probe('banana'), true);
    assert.equal(bf.probe('cherry'), true);
  });

  it('should return false for values never added (low false positive rate)', () => {
    const values = Array.from({ length: 1000 }, (_, i) => `item-${i}`);
    const bf = BloomFilter.build(values);

    let falsePositives = 0;
    const testCount = 10000;
    for (let i = 0; i < testCount; i++) {
      if (bf.probe(`nonexistent-${i}`)) falsePositives++;
    }
    const fpRate = falsePositives / testCount;
    // Should be around 1% (configured default)
    assert.ok(fpRate < 0.05, `False positive rate too high: ${(fpRate * 100).toFixed(1)}%`);
  });

  it('should serialize and deserialize correctly', () => {
    const bf = BloomFilter.build(['hello', 'world']);
    const serialized = bf.serialize();
    const restored = BloomFilter.deserialize(serialized);

    assert.equal(restored.probe('hello'), true);
    assert.equal(restored.probe('world'), true);
    assert.equal(restored.size, bf.size);
    assert.equal(restored.hashCount, bf.hashCount);
  });

  it('should handle empty input', () => {
    const bf = BloomFilter.build([]);
    assert.ok(bf.size > 0);
  });

  it('should handle numeric values as strings', () => {
    const bf = BloomFilter.build(['200', '404', '500']);
    assert.equal(bf.probe('200'), true);
    assert.equal(bf.probe('404'), true);
    assert.equal(bf.probe('999'), false);
  });
});

describe('buildFileBloom / probeFileBloom', () => {
  it('should build bloom filters for multiple columns', () => {
    const bloom = buildFileBloom({
      service: ['api', 'auth', 'web'],
      level: ['info', 'error'],
    });

    assert.ok(bloom.columns.service);
    assert.ok(bloom.columns.level);
  });

  it('should correctly probe equality filters', () => {
    const bloom = buildFileBloom({
      service: ['api', 'auth'],
      level: ['info', 'warn'],
    });

    // Should match — 'api' is present
    assert.equal(probeFileBloom(bloom, { service: 'api' }), true);

    // Should NOT match — 'billing' was never added
    assert.equal(probeFileBloom(bloom, { service: 'billing' }), false);

    // Should match — both present
    assert.equal(probeFileBloom(bloom, { service: 'auth', level: 'info' }), true);

    // Should NOT match — level 'fatal' not present
    assert.equal(probeFileBloom(bloom, { service: 'auth', level: 'fatal' }), false);
  });

  it('should handle IN (array) filters', () => {
    const bloom = buildFileBloom({
      level: ['info', 'warn'],
    });

    // At least one value present
    assert.equal(probeFileBloom(bloom, { level: ['error', 'info'] }), true);

    // None present
    assert.equal(probeFileBloom(bloom, { level: ['error', 'fatal'] }), false);
  });

  it('should pass through filters for columns without blooms', () => {
    const bloom = buildFileBloom({
      service: ['api'],
    });

    // 'host' has no bloom — can't prune, should return true
    assert.equal(probeFileBloom(bloom, { host: 'host-999' }), true);
  });
});
