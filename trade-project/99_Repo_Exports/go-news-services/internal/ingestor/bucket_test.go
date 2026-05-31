package ingestor

import (
	"testing"
	"time"
)

func TestBucketStartMs(t *testing.T) {
	// 2024-01-01 10:17 UTC, bucket 6h => 06:00 UTC
	ts := time.Date(2024, 1, 1, 10, 17, 0, 0, time.UTC).UnixMilli()
	got := BucketStartMs(ts, 6*time.Hour)
	want := time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC).UnixMilli()
	if got != want {
		t.Fatalf("got %d want %d", got, want)
	}
}

func TestBucketStartMsOrZero_ParseFailGivesZero(t *testing.T) {
	b := 6 * time.Hour
	if got := BucketStartMsOrZero(0, b); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
	if got := BucketStartMsOrZero(-10, b); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestBucketStartMsOrZero_Normal(t *testing.T) {
	b := 6 * time.Hour
	// 12:34 -> bucket floor 12:00 (в ms относительно epoch)
	ts := int64(1700000000000) // любое
	got := BucketStartMsOrZero(ts, b)
	if got <= 0 {
		t.Fatalf("expected >0, got %d", got)
	}
	// floor property
	if got > ts {
		t.Fatalf("bucket must be <= ts, got bucket=%d ts=%d", got, ts)
	}
}