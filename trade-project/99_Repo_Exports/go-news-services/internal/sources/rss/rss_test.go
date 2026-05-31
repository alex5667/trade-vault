package rss

import "testing"

func TestIngestorTimeBucket(t *testing.T) {
	bucketMs := int64(6 * 60 * 60 * 1000)
	// 1700000000000ms => deterministic string
	b1 := ingestorTimeBucket(1700000000000, bucketMs)
	b2 := ingestorTimeBucket(1700000000001, bucketMs)
	if b1 != b2 {
		t.Fatalf("expected same bucket; got %q vs %q", b1, b2)
	}
}
