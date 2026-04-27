// Пакет redis (public) реализует потребителей/публикаторов Redis Streams для go‑worker.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"go-worker/infra/redisclient"
	streamkeys "go-worker/internal/streams"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

const (
	// orphanIdleThreshold — время простоя сообщения в PEL,
	// после которого оно считается «осиротевшим» и подлежит рекламации.
	orphanIdleThreshold = 30 * time.Second
	// orphanReclaimInterval — интервал между циклами рекламации.
	orphanReclaimInterval = 30 * time.Second
	// orphanBatchSize — максимальное количество сообщений за один вызов XAUTOCLAIM.
	orphanBatchSize = 100
)

// StreamConsumer — потребитель сообщений из Redis Streams (XREADGROUP/XACK).
// Поддерживает чтение одного или нескольких стримов, auto-ack и graceful shutdown.
type StreamConsumer struct {
	client        *redis.Client
	consumerGroup string
	consumerName  string
	ctx           context.Context
	cancel        context.CancelFunc
}

// MessageHandler — функция-обработчик одного сообщения.
// На вход получает имя стрима, ID сообщения и набор полей (map).
type MessageHandler func(streamName string, messageID string, fields map[string]interface{}) error

// NewStreamConsumer создаёт новый экземпляр потребителя с заданной группой и именем.
func NewStreamConsumer(consumerGroup, consumerName string) *StreamConsumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &StreamConsumer{
		client:        redisclient.Client,
		consumerGroup: consumerGroup,
		consumerName:  consumerName,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// CreateConsumerGroup создаёт consumer group для стрима, если её ещё нет (начиная с '$').
func (sc *StreamConsumer) CreateConsumerGroup(streamName string) error {
	// Пытаемся создать consumer group, начиная с '$' (только новые сообщения)
	err := sc.client.XGroupCreateMkStream(sc.ctx, streamName, sc.consumerGroup, "$").Err()
	if err != nil && err != redis.Nil {
		// Если группа уже существует, это не ошибка
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("ошибка создания consumer group для стрима %s: %v", streamName, err)
		}
	}
	return nil
}

// ReclaimOrphans выполняет один цикл XPENDING→XAUTOCLAIM для заданного стрима.
// Рекламирует сообщения, простоявшие дольше orphanIdleThreshold, на текущего consumer,
// после чего немедленно их обрабатывает и подтверждает через XACK.
func (sc *StreamConsumer) ReclaimOrphans(ctx context.Context, streamName string, handler MessageHandler) {
	var cursor string // "0-0" — начало; XAUTOCLAIM возвращает новый курсор
	cursor = "0-0"

	for {
		if ctx.Err() != nil {
			return
		}

		messages, nextCursor, err := sc.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   streamName,
			Group:    sc.consumerGroup,
			Consumer: sc.consumerName,
			MinIdle:  orphanIdleThreshold,
			Start:    cursor,
			Count:    orphanBatchSize,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			zap.S().Warnf("⚠️  XAUTOCLAIM %s: %v", streamName, err)
			return
		}

		for _, msg := range messages {
			if err := handler(streamName, msg.ID, msg.Values); err != nil {
				zap.S().Errorf("❌ ReclaimOrphans: ошибка обработки orphan %s из %s: %v", msg.ID, streamName, err)
				continue
			}
			if ackErr := sc.client.XAck(ctx, streamName, sc.consumerGroup, msg.ID).Err(); ackErr != nil {
				zap.S().Errorf("🔥 ReclaimOrphans: XACK orphan %s из %s: %v", msg.ID, streamName, ackErr)
			}
		}

		if len(messages) > 0 {
			zap.S().Infof("♻️  ReclaimOrphans: %s — рекламировано %d сообщений (cursor=%s)",
				streamName, len(messages), cursor)
		}

		// Пустой nextCursor означает, что PEL полностью просканирован.
		if nextCursor == "0-0" || nextCursor == "" {
			return
		}
		cursor = nextCursor
	}
}

// startOrphanReclaimer запускает фоновую горутину, которая периодически вызывает ReclaimOrphans.
func (sc *StreamConsumer) startOrphanReclaimer(streamName string, handler MessageHandler) {
	go func() {
		ticker := time.NewTicker(orphanReclaimInterval)
		defer ticker.Stop()

		for {
			select {
			case <-sc.ctx.Done():
				return
			case <-ticker.C:
				sc.ReclaimOrphans(sc.ctx, streamName, handler)
			}
		}
	}()
}

// ConsumeFromStream потребляет сообщения из одного стрима в бесконечном цикле.
// Для каждого сообщения вызывается handler, после успешной обработки — XACK.
// При старте запускает фоновый цикл XAUTOCLAIM для рекламации orphan-сообщений (idle>30s).
func (sc *StreamConsumer) ConsumeFromStream(streamName string, handler MessageHandler) error {
	// Создаём consumer group перед началом потребления
	if err := sc.CreateConsumerGroup(streamName); err != nil {
		return fmt.Errorf("не удалось создать consumer group: %v", err)
	}

	// P1-1: запускаем фоновый reclaim-цикл для orphan pending entries
	sc.startOrphanReclaimer(streamName, handler)

	zap.S().Infof("🔄 Запуск потребления стрима: %s (orphan-reclaim каждые %s, idle>%s)",
		streamName, orphanReclaimInterval, orphanIdleThreshold)

	for {
		select {
		case <-sc.ctx.Done():
			zap.S().Infof("🛑 Остановка потребления стрима: %s", streamName)
			return sc.ctx.Err()
		default:
			// Читаем сообщения из стрима
			streams, err := sc.client.XReadGroup(sc.ctx, &redis.XReadGroupArgs{
				Group:    sc.consumerGroup,
				Consumer: sc.consumerName,
				Streams:  []string{streamName, ">"},
				Count:    10,
				Block:    time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					// Нет новых сообщений, продолжаем (это нормально)
					continue
				}

				errMsg := err.Error()

				// 🎯 ИСПРАВЛЕНИЕ: context.Canceled - нормальное завершение работы
				if errMsg == "context canceled" || errMsg == "context deadline exceeded" {
					// Это нормальное завершение, просто выходим
					zap.S().Infof("🛑 Остановка потребления стрима %s (context canceled)", streamName)
					return nil
				}

				if strings.Contains(strings.ToUpper(errMsg), "NOGROUP") {
					zap.S().Infof("ℹ️ Обнаружен NOGROUP для стрима %s, создаём consumer group '%s'...", streamName, sc.consumerGroup)
					if cgErr := sc.CreateConsumerGroup(streamName); cgErr != nil {
						zap.S().Errorf("❌ Не удалось пересоздать consumer group '%s' для %s: %v", sc.consumerGroup, streamName, cgErr)
					} else {
						zap.S().Infof("✅ Consumer group '%s' готова для %s", sc.consumerGroup, streamName)
					}
					time.Sleep(2 * time.Second)
					continue
				}

				zap.S().Errorf("❌ Ошибка чтения стрима %s: %v", streamName, err)
				time.Sleep(5 * time.Second)
				continue
			}

			// Обрабатываем полученные сообщения
			for _, stream := range streams {
				for _, message := range stream.Messages {
					if err := handler(streamName, message.ID, message.Values); err != nil {
						zap.S().Errorf("❌ Ошибка обработки сообщения %s из стрима %s: %v", message.ID, streamName, err)
						continue
					}

					// Подтверждаем успешную обработку сообщения
					err := sc.client.XAck(sc.ctx, streamName, sc.consumerGroup, message.ID).Err()
					if err != nil {
						// P1-4: Critical DLQ routing for XACK failures in Go worker
						zap.S().Errorf("🔥 CRITICAL XACK FAILURE (stream=%s id=%s): %v", streamName, message.ID, err)

						dlqPayload := map[string]interface{}{
							"consumer_group": sc.consumerGroup,
							"consumer_name":  sc.consumerName,
							"stream":         streamName,
							"ids":            message.ID,
							"error":          err.Error(),
							"ts_ms":          time.Now().UnixMilli(),
						}
						// Best-effort DLQ write.
						// MaxLen is sourced from the canonical StreamRetention map so that
						// the producer and the janitor always use the same bound (currently 2000).
						sc.client.XAdd(sc.ctx, &redis.XAddArgs{
							Stream: streamkeys.SignalAckDLQ,
							MaxLen: streamkeys.StreamRetention[streamkeys.SignalAckDLQ],
							Approx: true,
							Values: dlqPayload,
						})

						// Stop processing this batch to prevent further drift
						break
					}
				}
			}
		}
	}
}

// ConsumeFromMultipleStreams потребляет сообщения из нескольких стримов параллельно (горутинно).
// Для каждого стрима создаётся отдельный цикл XREADGROUP; после успешной обработки — XACK.
func (sc *StreamConsumer) ConsumeFromMultipleStreams(streamHandlers map[string]MessageHandler) error {
	zap.S().Infof("🔄 Запуск потребления %d стримов", len(streamHandlers))

	// Запускаем горутину для каждого стрима
	for streamName, handler := range streamHandlers {
		go func(sName string, h MessageHandler) {
			if err := sc.ConsumeFromStream(sName, h); err != nil {
				// Проверяем, не является ли ошибка отменой контекста (graceful shutdown)
				if err == context.Canceled {
					zap.S().Infof("🛑 Остановка потребления стрима %s: контекст отменен", sName)
					return
				}
				zap.S().Errorf("❌ Ошибка потребления стрима %s: %v", sName, err)
			}
		}(streamName, handler)
	}

	// Главный цикл теперь просто ждет завершения
	select {
	case <-sc.ctx.Done():
		zap.S().Infof("🛑 Остановка потребления всех стримов")
		return sc.ctx.Err()
	}
}

// Stop останавливает потребление сообщений (graceful cancel контекста).
func (sc *StreamConsumer) Stop() {
	zap.S().Infof("🛑 Остановка StreamConsumer")
	sc.cancel()
}

// ParseMessageData извлекает JSON из поля "data" сообщения Redis Stream и парсит в map.
func ParseMessageData(fields map[string]interface{}) (map[string]interface{}, error) {
	dataField, exists := fields["data"]
	if !exists {
		return nil, fmt.Errorf("поле 'data' не найдено в сообщении")
	}

	dataStr, ok := dataField.(string)
	if !ok {
		return nil, fmt.Errorf("поле 'data' не является строкой")
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
		return nil, fmt.Errorf("ошибка парсинга JSON из поля 'data': %v", err)
	}

	return data, nil
}

// NewDefaultStreamConsumer создаёт потребителя с настройками по умолчанию.
func NewDefaultStreamConsumer() *StreamConsumer {
	hostname, _ := os.Hostname()
	tf := os.Getenv("BINANCE_WS_TIMEFRAME")
	if tf == "" {
		tf = "default"
	}
	consumerGroup := fmt.Sprintf("scanner-group-%s", tf)
	consumerName := fmt.Sprintf("go-worker-%s-%s", tf, hostname)
	return NewStreamConsumer(consumerGroup, consumerName)
}

// SubscribeToRedisStream подписывается на стрим Redis и вызывает callback для каждого сообщения.
func SubscribeToRedisStream(streamName string, handler func(string)) {
	consumer := NewDefaultStreamConsumer()

	// Обработчик сообщений: парсит поле data → JSON → вызывает пользовательский callback
	messageHandler := func(stream string, messageID string, fields map[string]interface{}) error {
		data, err := ParseMessageData(fields)
		if err != nil {
			return fmt.Errorf("ошибка парсинга данных сообщения: %v", err)
		}

		// Сериализуем обратно в JSON для совместимости с существующим API
		jsonData, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("ошибка сериализации данных: %v", err)
		}

		zap.S().Infof("📥 Получено сообщение из стрима %s, ID: %s", stream, messageID)
		handler(string(jsonData))
		return nil
	}

	// Запускаем потребление стрима
	if err := consumer.ConsumeFromStream(streamName, messageHandler); err != nil {
		zap.S().Errorf("❌ Ошибка потребления стрима %s: %v", streamName, err)
	}
}
