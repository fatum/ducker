package cache

import "testing"

func TestCountersAndStats(t *testing.T) {
	c := New("/tmp/test.duckdb", "/tmp/cold")
	if s := c.Stats(); s.Hits != 0 || s.Misses != 0 || s.HitRate != 0 {
		t.Fatalf("expected zero stats, got %+v", s)
	}

	c.incrementHits()
	c.incrementHits()
	c.incrementMisses()
	s := c.Stats()
	if s.Hits != 2 || s.Misses != 1 {
		t.Fatalf("unexpected stats: %+v", s)
	}
	if s.HitRate <= 0.66 || s.HitRate >= 0.67 {
		t.Fatalf("unexpected hit rate: %f", s.HitRate)
	}

	c.ResetCounters()
	s = c.Stats()
	if s.Hits != 0 || s.Misses != 0 || s.HitRate != 0 {
		t.Fatalf("expected reset stats, got %+v", s)
	}
}

func TestSegmentLockReuse(t *testing.T) {
	c := New("/tmp/test.duckdb", "/tmp/cold")
	l1 := c.segmentLock("tenant-1", "seg-1")
	l2 := c.segmentLock("tenant-1", "seg-1")
	if l1 != l2 {
		t.Fatalf("expected same lock instance for same tenant/segment")
	}
	l3 := c.segmentLock("tenant-1", "seg-2")
	if l1 == l3 {
		t.Fatalf("expected different lock instance for different segment")
	}
}
