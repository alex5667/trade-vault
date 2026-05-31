// Reference-only P4.1 adapter for external Go ingest service.
// Writes unified latency contract hashes for service=go_ingest stage=ingest_to_redis.
// Copy this snippet into your Go worker; adapt the LatencyPayload fields as needed.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// LatencyPayload captures the timing fields for one symbol's ingestion event.
type LatencyPayload struct {
	Symbol           string
	TsEventMs        int64
	TsIngestSourceMs int64
	TsRedisXaddMs    int64
	InstanceID       string
	Source           string
}

// writeIngestToRedis writes the canonical latency contract hash for go_ingest/ingest_to_redis.
// keyPrefix is typically "metrics:latency_contract:last".
func writeIngestToRedis(ctx context.Context, rdb *redis.Client, keyPrefix string, ttl time.Duration, p LatencyPayload) error {
	symbol := strings.ToUpper(strings.TrimSpace(p.Symbol))
	if symbol == "" {
		return nil
	}
	durationMs := p.TsRedisXaddMs - p.TsIngestSourceMs
	if durationMs < 0 {
		durationMs = 0
	}
	nowMs := time.Now().UnixMilli()
	key := fmt.Sprintf("%s:go_ingest:ingest_to_redis:%s", keyPrefix, symbol)
	mapping := map[string]any{
		"schema_version":      "1",
		"service":             "go_ingest",
		"stage":               "ingest_to_redis",
		"symbol":              symbol,
		"last_duration_ms":    durationMs,
		"last_ts_ms":          nowMs,
		"ts_event_ms":         p.TsEventMs,
		"ts_ingest_source_ms": p.TsIngestSourceMs,
		"ts_redis_xadd_ms":    p.TsRedisXaddMs,
		"instance_id":         p.InstanceID,
		"source":              p.Source,
	}
	if err := rdb.HSet(ctx, key, mapping).Err(); err != nil {
		return err
	}
	if ttl > 0 {
		return rdb.Expire(ctx, key, ttl).Err()
	}
	return nil
}

func main() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		panic(err)
	}
	rdb := redis.NewClient(opts)
	ttlS, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("LATENCY_CONTRACT_TTL_S")))
	if ttlS <= 0 {
		ttlS = 172800
	}
	_ = writeIngestToRedis(context.Background(), rdb, "metrics:latency_contract:last", time.Duration(ttlS)*time.Second, LatencyPayload{
		Symbol:           "BTCUSDT",
		TsEventMs:        time.Now().Add(-250 * time.Millisecond).UnixMilli(),
		TsIngestSourceMs: time.Now().Add(-100 * time.Millisecond).UnixMilli(),
		TsRedisXaddMs:    time.Now().UnixMilli(),
		InstanceID:       os.Getenv("HOSTNAME"),
		Source:           "go_ingest_example",
	})
}
