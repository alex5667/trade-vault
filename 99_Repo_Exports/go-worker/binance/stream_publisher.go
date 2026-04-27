package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/streams"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// StreamPublisher структура для публикации в Redis Streams
type StreamPublisher struct {
	redisClient *redis.Client
}

// NewStreamPublisher создает новый экземпляр издателя стримов
func NewStreamPublisher() *StreamPublisher {
	return &StreamPublisher{
		redisClient: redisclient.Client,
	}
}

// PublishTickerData публикует данные тикеров в Redis Stream
func (sp *StreamPublisher) PublishTickerData(ctx context.Context, data []TickerData, streamName string) error {
	// Сериализуем данные в JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("сериализация данных тикеров: %w", err)
	}

	// Создаем метаданные
	metadata := CreateStreamMetadata("ticker_24h", time.Now()) // UTC время

	// Публикуем в стрим
	if _, err := redisclient.XAddWithRetry(ctx, sp.redisClient, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: streams.MaxLenGlobal,
		Approx: true,
		Values: map[string]interface{}{
			"data":           string(jsonData),
			"type":           metadata["type"],
			"timestamp":      metadata["timestamp"],
			"count":          len(data),
			"schema_version": "1",
			"trace_id":       fmt.Sprintf("%d", time.Now().UnixNano()),
		},
	}); err != nil {
		return fmt.Errorf("публикация тикеров в стрим %s: %w", streamName, err)
	}

	// Закомментировано для уменьшения шума в логах
	// zap.S().Infof("📡 Тикеры опубликованы в Redis Stream: %s (%d записей)", streamName, len(data))
	return nil
}

// PublishFundingRates публикует данные funding rates в Redis Stream
func (sp *StreamPublisher) PublishFundingRates(ctx context.Context, data []FundingRate, streamName string) error {
	// Сериализуем данные в JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("сериализация данных funding rates: %w", err)
	}

	// Создаем метаданные
	metadata := CreateStreamMetadata("funding_rates", time.Now()) // UTC время

	// Публикуем в стрим
	if _, err := redisclient.XAddWithRetry(ctx, sp.redisClient, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: streams.MaxLenGlobal,
		Approx: true,
		Values: map[string]interface{}{
			"data":           string(jsonData),
			"type":           metadata["type"],
			"timestamp":      metadata["timestamp"],
			"count":          len(data),
			"schema_version": "1",
			"trace_id":       fmt.Sprintf("%d", time.Now().UnixNano()),
		},
	}); err != nil {
		return fmt.Errorf("публикация funding rates в стрим %s: %w", streamName, err)
	}

	zap.S().Infof("📡 Funding rates опубликованы в Redis Stream: %s (%d записей)", streamName, len(data))
	return nil
}

// PublishSignal публикует сигнал в Redis Stream
func (sp *StreamPublisher) PublishSignal(ctx context.Context, signalType string, signal interface{}, streamName string) error {
	// Сериализуем сигнал в JSON
	jsonData, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("сериализация сигнала: %w", err)
	}

	// Создаем метаданные
	metadata := CreateStreamMetadata(signalType, time.Now())

	// Публикуем в стрим
	if _, err := redisclient.XAddWithRetry(ctx, sp.redisClient, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: streams.MaxLenGlobal,
		Approx: true,
		Values: map[string]interface{}{
			"data":           string(jsonData),
			"type":           metadata["type"],
			"timestamp":      metadata["timestamp"],
			"schema_version": "1",
			"trace_id":       fmt.Sprintf("%d", time.Now().UnixNano()),
		},
	}); err != nil {
		return fmt.Errorf("публикация сигнала в стрим %s: %w", streamName, err)
	}

	zap.S().Infof("📡 Сигнал %s опубликован в Redis Stream: %s", signalType, streamName)
	return nil
}

// CreateStreamMetadata создает метаданные для стрима
func CreateStreamMetadata(dataType string, timestamp time.Time) map[string]interface{} {
	return map[string]interface{}{
		"type":      dataType,
		"timestamp": timestamp.UnixMilli(), // Используем миллисекунды по UTC
		"source":    "binance-api",
	}
}

// CreateConsumerGroup создает consumer group для стрима
func (sp *StreamPublisher) CreateConsumerGroup(ctx context.Context, streamName, groupName string) error {
	err := sp.redisClient.XGroupCreate(ctx, streamName, groupName, "$").Err()
	if err != nil {
		// Проверяем на ошибку загрузки Redis
		if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
			zap.S().Warnf("⚠️ Redis is loading dataset, skipping CreateConsumerGroup for %s", streamName)
			return nil // Пропускаем создание группы при загрузке Redis
		}
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("ошибка создания consumer group %s для стрима %s: %w", groupName, streamName, err)
		}
	}
	zap.S().Infof("✅ Consumer group %s создана/обновлена для стрима %s", groupName, streamName)
	return nil
}

// GetStreamInfo получает информацию о стриме
func (sp *StreamPublisher) GetStreamInfo(ctx context.Context, streamName string) (*redis.XInfoStream, error) {
	info, err := sp.redisClient.XInfoStream(ctx, streamName).Result()
	if err != nil {
		// Проверяем на ошибку загрузки Redis
		if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
			zap.S().Warnf("⚠️ Redis is loading dataset, skipping GetStreamInfo for %s", streamName)
			return nil, fmt.Errorf("redis is loading dataset")
		}
		return nil, fmt.Errorf("ошибка получения информации о стриме %s: %w", streamName, err)
	}
	return info, nil
}

// TrimStream обрезает стрим до указанного размера
func (sp *StreamPublisher) TrimStream(ctx context.Context, streamName string, maxLen int64) error {
	err := sp.redisClient.XTrimMaxLen(ctx, streamName, maxLen).Err()
	if err != nil {
		// Проверяем на ошибку загрузки Redis
		if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
			zap.S().Warnf("⚠️ Redis is loading dataset, skipping TrimStream for %s", streamName)
			return nil // Пропускаем обрезку при загрузке Redis
		}
		return fmt.Errorf("ошибка обрезки стрима %s: %w", streamName, err)
	}
	zap.S().Infof("🧹 Стрим %s обрезан до %d сообщений", streamName, maxLen)
	return nil
}

// PublishMarketData публикует рыночные данные в соответствующие стримы
func (sp *StreamPublisher) PublishMarketData(ctx context.Context, tickers []TickerData, fundingRates []FundingRate) error {
	// Публикуем тикеры
	if err := sp.PublishTickerData(ctx, tickers, streams.Ticker24h); err != nil {
		zap.S().Errorf("❌ Ошибка публикации тикеров: %v", err)
	}

	// Публикуем funding rates
	if err := sp.PublishFundingRates(ctx, fundingRates, streams.FundingRate); err != nil {
		zap.S().Errorf("❌ Ошибка публикации funding rates: %v", err)
	}

	return nil
}

// PublishVolatilitySignal публикует сигнал волатильности
func (sp *StreamPublisher) PublishVolatilitySignal(ctx context.Context, signal map[string]interface{}) error {
	return sp.PublishSignal(ctx, "volatility", signal, streams.Volatility)
}

// PublishVolatilityRangeSignal публикует сигнал волатильности по диапазону
func (sp *StreamPublisher) PublishVolatilityRangeSignal(ctx context.Context, signal map[string]interface{}) error {
	return sp.PublishSignal(ctx, "volatilityRange", signal, streams.VolatilityRange)
}

// PublishGainersSignal публикует сигнал растущих активов
func (sp *StreamPublisher) PublishGainersSignal(ctx context.Context, signal map[string]interface{}) error {
	return sp.PublishSignal(ctx, "top-gainers", signal, streams.TopGainers)
}

// PublishLosersSignal публикует сигнал падающих активов
func (sp *StreamPublisher) PublishLosersSignal(ctx context.Context, signal map[string]interface{}) error {
	return sp.PublishSignal(ctx, "top-losers", signal, streams.TopLosers)
}
