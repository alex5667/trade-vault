package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/monitoring"
	"go-worker/internal/streams"

	"github.com/redis/go-redis/v9"
)

// StreamPublisher — публикация market snapshots в Redis Streams.
// Структура и поля намеренно близки к go-worker/binance/stream_publisher.go,
// чтобы downstream мог легко потреблять данные.
type StreamPublisher struct {
	redisClient *redis.Client
}

func NewStreamPublisher() *StreamPublisher {
	return &StreamPublisher{redisClient: redisclient.Client}
}

func (sp *StreamPublisher) PublishTicker24h(ctx context.Context, data []Ticker24h, snapshotTsMs int64, streamName string) error {
	if sp == nil || sp.redisClient == nil {
		return fmt.Errorf("redis client not initialised")
	}
	if streamName == "" {
		streamName = streams.BybitTicker24h
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal tickers: %w", err)
	}

	// В отличие от Binance, timestamp берём как server time (resp.time).
	// Если upstream не дал time, APIClient подставляет time.Now().UnixMilli().
	values := map[string]any{
		"data":           string(jsonData),
		"type":           "ticker_24h",
		"timestamp":      snapshotTsMs,
		"count":          len(data),
		"venue":          "bybit",
		"category":       "linear",
		"written_at":     time.Now().UnixMilli(),
		"schema_version": "1",
		"trace_id":       fmt.Sprintf("%d", time.Now().UnixNano()),
	}

	if _, err := redisclient.XAddWithRetry(ctx, sp.redisClient, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: streams.MaxLenGlobal,
		Approx: true,
		Values: values,
	}); err != nil {
		monitoring.RecordBybitPublish(streamName, false)
		return fmt.Errorf("publish tickers to %s: %w", streamName, err)
	}
	monitoring.RecordBybitPublish(streamName, true)
	return nil
}

func (sp *StreamPublisher) PublishFundingRates(ctx context.Context, data []FundingRatePoint, snapshotTsMs int64, streamName string) error {
	if sp == nil || sp.redisClient == nil {
		return fmt.Errorf("redis client not initialised")
	}
	if streamName == "" {
		streamName = streams.BybitFundingRate
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal funding: %w", err)
	}

	values := map[string]any{
		"data":           string(jsonData),
		"type":           "funding_rate",
		"timestamp":      snapshotTsMs,
		"count":          len(data),
		"venue":          "bybit",
		"category":       "linear",
		"written_at":     time.Now().UnixMilli(),
		"schema_version": "1",
		"trace_id":       fmt.Sprintf("%d", time.Now().UnixNano()),
	}

	if _, err := redisclient.XAddWithRetry(ctx, sp.redisClient, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: streams.MaxLenGlobal,
		Approx: true,
		Values: values,
	}); err != nil {
		monitoring.RecordBybitPublish(streamName, false)
		return fmt.Errorf("publish funding to %s: %w", streamName, err)
	}
	monitoring.RecordBybitPublish(streamName, true)
	return nil
}

// CreateConsumerGroup создает consumer group для стрима.
// Повторяем поведение Binance: BUSYGROUP не считаем ошибкой.
func (sp *StreamPublisher) CreateConsumerGroup(ctx context.Context, streamName, groupName string) error {
	if sp == nil || sp.redisClient == nil {
		return fmt.Errorf("redis client not initialised")
	}
	err := sp.redisClient.XGroupCreate(ctx, streamName, groupName, "$").Err()
	if err == nil {
		return nil
	}
	if err.Error() == "BUSYGROUP Consumer Group name already exists" {
		return nil
	}
	// Redis may be loading — не считаем фатальным.
	if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
		return nil
	}
	return err
}
