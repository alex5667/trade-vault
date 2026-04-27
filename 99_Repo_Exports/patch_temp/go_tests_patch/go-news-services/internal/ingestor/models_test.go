package ingestor

import "testing"

func TestBucketStartMsDeterministic(t *testing.T) {
	bucket := int64(6 * 60 * 60 * 1000) // 6h
	// 1700000000000ms and +1ms should fall in same bucket
	ts1 := int64(1700000000000)
	ts2 := ts1 + 1
	b1 := BucketStartMs(ts1, bucket)
	b2 := BucketStartMs(ts2, bucket)
	if b1 != b2 {
		t.Fatalf("expected same bucket: %d vs %d", b1, b2)
	}
	// next bucket boundary
	ts3 := b1 + bucket
	b3 := BucketStartMs(ts3, bucket)
	if b3 != ts3 {
		t.Fatalf("expected boundary to map to itself: got %d want %d", b3, ts3)
	}
}

func TestStableUIDDeterministicAndDifferent(t *testing.T) {
	a := StableUID("rss", "BTC", "title", "1700")
	b := StableUID("rss", "BTC", "title", "1700")
	if a != b {
		t.Fatalf("expected deterministic uid: %q vs %q", a, b)
	}
	c := StableUID("rss", "BTC", "title", "1701")
	if a == c {
		t.Fatalf("expected different uid for different parts")
	}
	if len(a) == 0 {
		t.Fatalf("expected non-empty uid")
	}
}
