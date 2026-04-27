// Пакет redis (public) реализует потребителей/публикаторов Redis Streams для go‑worker.
package redis

import (
	"encoding/json"
	"fmt"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/streams"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// StreamPublisher структура для публикации в Redis Streams
type StreamPublisher struct {
	client *redis.Client
}

// NewStreamPublisher создает новый экземпляр издателя стримов
func NewStreamPublisher() *StreamPublisher {
	return &StreamPublisher{
		client: redisclient.Client,
	}
}

// PublishToStream публикует сообщение в Redis Stream
func (sp *StreamPublisher) PublishToStream(streamName string, data interface{}) (string, error) {
	// Сериализуем данные в JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("ошибка сериализации данных: %v", err)
	}

	// Подготавливаем поля для стрима
	fields := map[string]interface{}{
		"data":      string(jsonData),
		"timestamp": time.Now().UTC().UnixMilli(), // UTC время в миллисекундах
	}

	// Добавляем метаданные если это карта
	if dataMap, ok := data.(map[string]interface{}); ok {
		if dataType, exists := dataMap["type"]; exists {
			fields["type"] = dataType
		}
		if symbol, exists := dataMap["symbol"]; exists {
			fields["symbol"] = symbol
		}
	}

	// Публикуем в стрим
	messageID, err := redisclient.XAddWithRetry(redisclient.Ctx, redisclient.Client, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: streams.MaxLenCandles(), // Было 50000, buffer ~44h для 19 символов × 1m
		Approx: true,                  // Приблизительная очистка для производительности
		ID:     "*",                   // Автоматическая генерация ID
		Values: fields,
	})

	if err != nil {
		return "", fmt.Errorf("ошибка публикации в стрим %s: %v", streamName, err)
	}

	zap.S().Infof("✅ Сообщение опубликовано в стрим %s, ID: %s", streamName, messageID)
	return messageID, nil
}

// PublishBinanceData публикует данные Binance в соответствующий стрим
func (sp *StreamPublisher) PublishBinanceData(dataType string, data interface{}) error {
	streamName := fmt.Sprintf("stream:binance:%s", dataType)
	_, err := sp.PublishToStream(streamName, data)
	return err
}

// PublishSignal публикует сигнал в соответствующий стрим
func (sp *StreamPublisher) PublishSignal(signalType string, signal interface{}) error {
	streamName := fmt.Sprintf("stream:signal:%s", signalType)
	_, err := sp.PublishToStream(streamName, signal)
	return err
}

// CreateConsumerGroup создает consumer group для стрима
func (sp *StreamPublisher) CreateConsumerGroup(streamName, groupName string) error {
	err := redisclient.Client.XGroupCreate(redisclient.Ctx, streamName, groupName, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("ошибка создания consumer group %s для стрима %s: %v", groupName, streamName, err)
	}
	zap.S().Infof("✅ Consumer group %s создана/обновлена для стрима %s", groupName, streamName)
	return nil
}

// GetStreamInfo получает информацию о стриме
func (sp *StreamPublisher) GetStreamInfo(streamName string) (*redis.XInfoStream, error) {
	info, err := redisclient.Client.XInfoStream(redisclient.Ctx, streamName).Result()
	if err != nil {
		return nil, fmt.Errorf("ошибка получения информации о стриме %s: %v", streamName, err)
	}
	return info, nil
}

// TrimStream обрезает стрим до указанного размера
func (sp *StreamPublisher) TrimStream(streamName string, maxLen int64) error {
	err := redisclient.Client.XTrimMaxLen(redisclient.Ctx, streamName, maxLen).Err()
	if err != nil {
		return fmt.Errorf("ошибка обрезки стрима %s: %v", streamName, err)
	}
	zap.S().Infof("🧹 Стрим %s обрезан до %d сообщений", streamName, maxLen)
	return nil
}

// Глобальный экземпляр для совместимости
var globalStreamPublisher = NewStreamPublisher()

// PublishToRedisStream публикует данные в Redis Stream (для совместимости с существующим кодом)
func PublishToRedisStream(streamName string, data interface{}) error {
	_, err := globalStreamPublisher.PublishToStream(streamName, data)
	return err
}
