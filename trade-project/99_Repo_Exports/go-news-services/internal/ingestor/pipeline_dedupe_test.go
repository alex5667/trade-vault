package ingestor

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestDedupeAndXAdd(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	p := &Pipeline{
		cfg: PipelineConfig{
			Redis:       rdb,
			DedupeTTL:   2 * time.Hour,
			MaxStreamLen: 1000,
		},
	}

	ctx := context.Background()
	ok1, err := p.DedupeAndXAdd(ctx, "news:raw", "news:dedupe:", "u1", map[string]any{"uid":"u1"})
	if err != nil || !ok1 {
		t.Fatalf("first ok=%v err=%v", ok1, err)
	}

	ok2, err := p.DedupeAndXAdd(ctx, "news:raw", "news:dedupe:", "u1", map[string]any{"uid":"u1"})
	if err != nil || ok2 {
		t.Fatalf("second ok=%v err=%v", ok2, err)
	}

	// check that message was added to stream
	// use Redis client to check stream length
	xlen, err := rdb.XLen(ctx, "news:raw").Result()
	if err != nil {
		t.Fatal(err)
	}
	if xlen != 1 {
		t.Fatalf("stream len=%d want=1", xlen)
	}
}
