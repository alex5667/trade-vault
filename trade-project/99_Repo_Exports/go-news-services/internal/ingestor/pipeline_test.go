package ingestor_test

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"trade-news-ingestor/internal/ingestor"
)

type fakeRSS struct{ items []ingestor.NewsRawItem }

func (f *fakeRSS) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) { return f.items, nil }

func TestDedupeXAdd(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil { t.Fatal(err) }
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	uid := ingestor.StableUID("rss", "http://x", "title", "bucket")
	rss := &fakeRSS{items: []ingestor.NewsRawItem{
		{UID: uid, PublishedTSms: ingestor.NowMs(), IngestedTSms: ingestor.NowMs(), Source: "rss", Title: "t", URL: "http://x", SymbolsJSON: "[]", PayloadJSON: "{}"},
	}}

	p := ingestor.NewPipeline(ingestor.PipelineConfig{
		Redis: rdb,
		StreamNewsRaw: "news:raw",
		StreamCalEvents: "calendar:events",
		StreamNewsHB: "news:hb",
		StreamCalHB: "calendar:hb",
		DedupeTTL: 10 * time.Minute,
		MaxStreamLen: 1000,
		HeartbeatTTL: 10 * time.Second,
		InstanceID: "t1",
		PollInterval: 10 * time.Second,
		CalPollInterval: 10 * time.Second,
		RSS: rss,
		Calendar: ingestor.NewNoopCalendarSource("noop"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// один проход
	p.FetchNewsOnce(ctx)

	// проверка: один элемент в stream
	xlen, err := rdb.XLen(ctx, "news:raw").Result()
	if err != nil {
		t.Fatal(err)
	}
	if xlen != 1 {
		t.Fatalf("expected 1, got %d", xlen)
	}

	// второй раз: не должно добавиться
	time.Sleep(1 * time.Second)
	p.FetchNewsOnce(ctx)
	xlen2, err := rdb.XLen(ctx, "news:raw").Result()
	if err != nil {
		t.Fatal(err)
	}
	if xlen2 != 1 {
		t.Fatalf("expected still 1, got %d", xlen2)
	}
}

func TestDedupeRollbackOnXAddError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	// сломаем XADD: сделаем ключ stream нестримовым типом заранее
	mr.Set("news:raw", "not-a-stream")

	p := ingestor.NewPipeline(ingestor.PipelineConfig{
		Redis: rdb,
		StreamNewsRaw: "news:raw",
		DedupeTTL: 7 * 24 * time.Hour,
		MaxStreamLen: 1000,
		InstanceID: "t1",
	})

	ctx := context.Background()
	uid := "abc"
	dedupeKey := "news:dedupe:" + uid

	ok, err := p.DedupeAndXAdd(ctx, "news:raw", "news:dedupe:", uid, map[string]any{"uid": uid})
	if err == nil {
		t.Fatalf("expected error, got nil (ok=%v)", ok)
	}

	// дедуп-ключ должен быть удален из-за rollback
	exists, err := rdb.Exists(ctx, dedupeKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if exists > 0 {
		t.Fatalf("dedupe key should be rolled back, but exists: %s", dedupeKey)
	}
}
