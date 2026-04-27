package ingestor

import (
	"strconv"
	"testing"
	"time"
)

func TestStableUID_Deterministic(t *testing.T) {
	a := StableUID("rss", "u", "t", "guid", "123")
	b := StableUID("rss", "u", "t", "guid", "123")
	if a != b {
		t.Fatalf("uids differ: %s vs %s", a, b)
	}
	if len(a) != 24 {
		t.Fatalf("expected len=24 got=%d uid=%s", len(a), a)
	}
}

func TestUIDStableWhenPublishedMissing(t *testing.T) {
	bucketMs := BucketStartMsOrZero(0, 6*time.Hour) // must be 0
	u1 := StableUID("newsapi", "http://x", "title", "src|http://x", strconv.FormatInt(bucketMs, 10))
	u2 := StableUID("newsapi", "http://x", "title", "src|http://x", strconv.FormatInt(bucketMs, 10))
	if u1 != u2 {
		t.Fatal("uid must be stable when publishedAt missing")
	}
}

func TestUIDDifferentWhenProviderIDDifferent(t *testing.T) {
	b := int64(1700000000000)
	u1 := StableUID("fmp", "u", "t", "A|site|date", strconv.FormatInt(b, 10))
	u2 := StableUID("fmp", "u", "t", "B|site|date", strconv.FormatInt(b, 10))
	if u1 == u2 {
		t.Fatal("uid must differ for different providerID")
	}
}
