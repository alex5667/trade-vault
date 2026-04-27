package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"trade-news-ingestor/internal/redisx"
)

func main() {
	logger := log.New(os.Stdout, "[news-watchdog] ", log.LstdFlags|log.Lmicroseconds)
	redisURL := getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
	maxAge := mustDur(getenv("HB_MAX_AGE", "45s"), 45*time.Second)

	rdb := redisx.NewClient(redisURL)
	defer func() { _ = rdb.Close() }()

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for range t.C {
		checkHB(context.Background(), logger, rdb, "news", maxAge)
		checkHB(context.Background(), logger, rdb, "calendar", maxAge)
	}
}

func checkHB(ctx context.Context, l *log.Logger, rdb *redis.Client, kind string, maxAge time.Duration) {
	raw, err := rdb.Get(ctx, "hb:"+kind).Result()
	if err != nil {
		// Distinguish between "key not found" (expected if service disabled) and actual errors
		if err == redis.Nil {
			// Key doesn't exist - service may be disabled, log as WARN not CRIT
			// This is throttled by the ticker (every 10s) so won't spam
			l.Printf("WARN no heartbeat key for kind=%s (service may be disabled)", kind)
		} else {
			// Actual Redis error
			l.Printf("CRIT heartbeat check failed kind=%s err=%v", kind, err)
		}
		return
	}
	var obj map[string]any
	if json.Unmarshal([]byte(raw), &obj) != nil {
		l.Printf("CRIT bad heartbeat json kind=%s", kind)
		return
	}
	ts, _ := obj["ts_ms"].(float64)
	age := time.Since(time.UnixMilli(int64(ts)))
	if age > maxAge {
		l.Printf("CRIT stale heartbeat kind=%s age=%s obj=%s", kind, age, raw)
		// пример: положить алерт в redis
		_ = rdb.Set(ctx, "alerts:"+kind+":stale", raw, 5*time.Minute).Err()
	}
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" { return def }
	return v
}
func mustDur(s string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil { return def }
	return d
}
