// Пакет connections управляет WebSocket‑подключениями к Binance (открытие, учёт, переподключение).
package connections

import (
	"context"
	"encoding/json" // Added for json.Marshal
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go-worker/binance"
	"go-worker/infra/redisclient"
	"go-worker/internal/interfaces"
	"go-worker/internal/monitoring"
	"go-worker/internal/streams"
	"go-worker/pkg/timeutil"

	"go.uber.org/zap"
)

// Счетчик для уменьшения логов отправки свечей
var candleSentCounter uint64

var (
// ОТКЛЮЧЕНО: старые отдельные соединения больше не используются
// activeConnections = make(map[string]bool)
// connMutex         = &sync.Mutex{}
)

// Manager управляет WebSocket соединениями и реализует интерфейс CandlePublisher
type Manager struct {
	symbolConsumer     *binance.SymbolConsumer      // Добавляем ссылку на SymbolConsumer
	symbolSupplementer *binance.SymbolSupplementer  // Добавляем ссылку на SymbolSupplementer для получения свечей
	monitor            *monitoring.WebSocketMonitor // Добавляем монитор для отслеживания состояния соединений
	ctx                context.Context
	cancel             context.CancelFunc
}

// Убеждаемся, что Manager реализует интерфейс CandlePublisher
var _ interfaces.CandlePublisher = (*Manager)(nil)

// NewManager создает новый менеджер соединений
func NewManager(ctx context.Context, symbolConsumer *binance.SymbolConsumer) *Manager {
	mCtx, cancel := context.WithCancel(ctx)
	mgr := &Manager{
		symbolConsumer:     symbolConsumer,
		symbolSupplementer: binance.NewSymbolSupplementer(redisclient.Client, ctx), // (Priority 5) Переданный ctx
		monitor:            monitoring.NewWebSocketMonitor(),
		ctx:                mCtx,
		cancel:             cancel,
	}
	// Запускаем фоновый поллинг длины Redis Stream (go_worker_candle_stream_length).
	// Интервал: 30s — не hot-path, Redis XLEN дешёвый.
	go mgr.pollStreamLength(mCtx, 30*time.Second)
	return mgr
}

// Stop завершает работу менеджера (отменяет контекст)
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

// pollStreamLength периодически читает XLEN candle-стримов и обновляет
// gauge go_worker_candle_stream_length для Prometheus.
// Вызывается из фонового горутина в NewManager — НЕ в hot-path.
func (m *Manager) pollStreamLength(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Primary Redis
			if n, err := redisclient.Client.XLen(ctx, streams.CandleDataStream).Result(); err == nil {
				monitoring.SetStreamLength(streams.CandleDataStream, n)
			}
			// Secondary Redis (worker)
			if redisclient.ClientWorker != nil {
				if n, err := redisclient.ClientWorker.XLen(ctx, streams.CandleDataStream).Result(); err == nil {
					monitoring.SetStreamLength(streams.CandleDataStream+":worker", n)
				}
			}
		}
	}
}

// SetMonitor устанавливает монитор для отслеживания соединений
func (m *Manager) SetMonitor(monitor *monitoring.WebSocketMonitor) {
	m.monitor = monitor
}

// Временные интервалы для свечей
const (
	TimeframeM1  = "1m"  // 1 минута
	TimeframeM5  = "5m"  // 5 минут
	TimeframeM15 = "15m" // 15 минут
	TimeframeH1  = "1h"  // 1 час
	TimeframeH4  = "4h"  // 4 часа
	TimeframeD1  = "1d"  // 1 день
	TimeframeW1  = "1w"  // 1 неделя
	TimeframeMN1 = "1M"  // 1 месяц
	TimeframeQ1  = "3M"  // 1 квартал (3 месяца)
	TimeframeY1  = "1y"  // 1 год
)

// InitializeCandleDataStream инициализирует Redis Stream и Consumer Group для данных свечей в обоих Redis
//
// Контракт XGROUP CREATE start position:
//   - "$" (default, live-only): consumer читает только новые сообщения, поступившие ПОСЛЕ создания группы.
//     Используется для real-time потребителей (Python worker, NestJS).
//     БЕЗОПАСНО: не требует idempotency у consumer для исторических баров.
//   - "0" (replay): consumer получает ВСЕ сообщения в стриме, включая исторические.
//     Используется только при replay/backtest. Consumer'ы ОБЯЗАНЫ быть idempotent!
//     Риск: при всплеске новых символов возможен overshoot из-за Approx:true на XADD.
//
// Управление через ENV: CANDLE_GROUP_START="$"|"0" (default: "$")
func (m *Manager) InitializeCandleDataStream() error {
	ctx := context.Background()

	// Создаем поток если его нет в обоих Redis
	err := redisclient.XAddDual(ctx, &redisclient.XAddArgs{
		Stream: streams.CandleDataStream,
		MaxLen: streams.MaxLenCandles(),
		Approx: true,
		Values: map[string]interface{}{
			"type":      "init",
			"timestamp": timeutil.GetCurrentTimestampMs(),
			"message":   "Candle data stream initialized",
		},
	})

	if err != nil {
		return err
	}

	// groupStart управляет semantics чтения для consumer group.
	// "$" = live-only (production default).
	// "0" = replay всей истории (только для backtest/replay режима, требует idempotent consumers).
	// Переключается через ENV CANDLE_GROUP_START без пересборки.
	groupStart := "$"
	if v := strings.TrimSpace(os.Getenv("CANDLE_GROUP_START")); v == "0" {
		groupStart = "0"
		zap.S().Warnf("⚠️ CandleDataGroup создаётся с start='0' (replay mode). " +
			"Consumer'ы ОБЯЗАНЫ быть idempotent. Используйте '0' только для backtest/replay.")
	}

	// Создаем Consumer Group в обоих Redis
	err = redisclient.XGroupCreateDual(ctx, streams.CandleDataStream, streams.CandleDataGroup, groupStart)
	if err != nil {
		return err
	}

	zap.S().Infof("✅ Redis Stream и Consumer Group для данных свечей инициализированы в обоих Redis")
	zap.S().Infof("📊 Stream: %s | Group: %s | start=%s", streams.CandleDataStream, streams.CandleDataGroup, groupStart)

	return nil
}

// PublishCandleData публикует данные свечи в Redis Stream для бэкенда (dual-write в оба Redis)
func (m *Manager) PublishCandleData(ctx context.Context, symbol, timeframe string, candleData map[string]interface{}) error {
	// Извлекаем closeTime из candleData для использования в качестве ts
	closeTime := timeutil.GetCurrentTimestampMs() // значение по умолчанию (UTC)
	if ct, ok := candleData["closeTime"]; ok {
		switch v := ct.(type) {
		case int64:
			closeTime = v
		case float64:
			closeTime = int64(v)
		case int:
			closeTime = int64(v)
		}
	}

	// Преобразуем данные свечи в JSON строку для корректной сериализации
	candleDataJSON, err := json.Marshal(candleData)
	if err != nil {
		return err
	}

	// Формируем поля для Redis Stream согласно новому протоколу
	// Обязательные поля: symbol, tf, ts, payload
	fields := map[string]interface{}{
		"symbol":  symbol,
		"tf":      timeframe,              // "1m"|"3m"|...
		"ts":      closeTime,              // время закрытия свечи в миллисекундах
		"payload": string(candleDataJSON), // JSON строка с полными данными свечи
	}

	// Публикуем в оба Redis Stream одновременно
	// MaxLenCandles предотвращает неограниченный рост стрима
	publishStart := time.Now()
	err = redisclient.XAddDual(ctx, &redisclient.XAddArgs{
		Stream: streams.CandleDataStream,
		MaxLen: streams.MaxLenCandles(),
		Approx: true,
		Values: fields,
	})

	// Метрики latency — zero-overhead path: оба гистогрммы под одним observe.
	latSec := time.Since(publishStart).Seconds()
	monitoring.RecordPublishLatency("binance", latSec)
	monitoring.RedisPublishDurationSeconds.WithLabelValues("binance", timeframe).Observe(latSec)

	if err != nil {
		return err
	}

	// Логируем только каждое 5000-е сообщение
	count := atomic.AddUint64(&candleSentCounter, 1)
	if count%5000 == 0 {
		zap.S().Infof("📤 Данные свечи %s@%s отправлены в оба Redis Stream %s (ts=%d) [всего отправлено: %d]", symbol, timeframe, streams.CandleDataStream, closeTime, count)
	}
	return nil
}

// GetCandleDataStreamInfo возвращает информацию о потоке данных свечей
func (m *Manager) GetCandleDataStreamInfo() (map[string]interface{}, error) {
	ctx := context.Background()

	// Получаем информацию о потоке
	info, err := redisclient.Client.XInfoStream(ctx, streams.CandleDataStream).Result()
	if err != nil {
		return nil, err
	}

	// Получаем информацию о группах
	groups, err := redisclient.Client.XInfoGroups(ctx, streams.CandleDataStream).Result()
	if err != nil {
		return nil, err
	}

	// Получаем информацию о потребителях в нашей группе
	var consumers interface{}
	for _, group := range groups {
		if group.Name == streams.CandleDataGroup {
			consumers, err = redisclient.Client.XInfoConsumers(ctx, streams.CandleDataStream, streams.CandleDataGroup).Result()
			if err != nil {
				zap.S().Errorf("⚠️ Ошибка получения информации о потребителях: %v", err)
			}
			break
		}
	}

	return map[string]interface{}{
		"stream": map[string]interface{}{
			"length":          info.Length,
			"lastGeneratedId": info.LastGeneratedID,
			"firstEntry":      info.FirstEntry,
			"lastEntry":       info.LastEntry,
		},
		"groups":    groups,
		"consumers": consumers,
	}, nil
}

// CleanupCandleDataStream очищает старые данные из потока (опционально)
func (m *Manager) CleanupCandleDataStream(maxLen int64) error {
	ctx := context.Background()

	// Ограничиваем длину потока
	err := redisclient.Client.XTrimMaxLen(ctx, streams.CandleDataStream, maxLen).Err()
	if err != nil {
		return err
	}

	zap.S().Infof("🧹 Поток данных свечей очищен, максимальная длина: %d", maxLen)
	return nil
}

// UpdateConnections - ОТКЛЮЧЕНО: теперь используется MultiplexedManager
func (m *Manager) UpdateConnections(symbols []string) {
	// ВСЕ WebSocket соединения теперь управляются через MultiplexedManager
	// Старые отдельные соединения отключены для экономии ресурсов
	zap.S().Infof("📤 UpdateConnections: символы передаются в MultiplexedManager")
}

// InitializeInitialConnections запускает WebSocket подключения для начальных пар
func (m *Manager) InitializeInitialConnections() {
	go func() {
		// Даем время для старта других подсистем, либо отменяем при shutdown
		select {
		case <-time.After(5 * time.Second):
		case <-m.ctx.Done():
			zap.S().Info("🛑 Инициализация WebSocket подключений отменена")
			return
		}

		zap.S().Info("🔄 Инициализация WebSocket подключений через SymbolConsumer...")

		// Проверяем, что SymbolConsumer доступен
		if m.symbolConsumer == nil {
			zap.S().Errorf("❌ SymbolConsumer не инициализирован")
			return
		}

		// Запускаем SymbolConsumer для мониторинга symbol:details
		// SymbolConsumer автоматически дополнит символы через Binance API если их меньше 50
		if err := m.symbolConsumer.Start(); err != nil {
			zap.S().Errorf("❌ Ошибка запуска SymbolConsumer: %v", err)
			return
		}

		zap.S().Infof("✅ SymbolConsumer запущен успешно")
		zap.S().Infof("📊 Мониторинг символов из symbol:details активен")
		zap.S().Infof("🌐 Автоматическое дополнение через Binance API при недостатке символов")
	}()
}

// GetActiveConnectionsCount возвращает количество активных соединений
func (m *Manager) GetActiveConnectionsCount() int {
	// Если SymbolConsumer доступен, получаем количество от него
	if m.symbolConsumer != nil {
		return m.symbolConsumer.GetActiveConnectionsCount()
	}

	// Fallback: возвращаем 0, так как старые соединения отключены
	return 0
}

// GetLastFrameAt возвращает время последнего полученного WS-фрейма из SymbolConsumer.
func (m *Manager) GetLastFrameAt() time.Time {
	if m.symbolConsumer != nil {
		return m.symbolConsumer.GetLastFrameAt()
	}
	return time.Time{}
}

// GetActiveConnections возвращает список активных соединений
func (m *Manager) GetActiveConnections() []string {
	// Если SymbolConsumer доступен, получаем соединения от него
	if m.symbolConsumer != nil {
		symbolConnections := m.symbolConsumer.GetActiveConnections()
		connections := make([]string, 0, len(symbolConnections))
		for connectionKey := range symbolConnections {
			connections = append(connections, connectionKey)
		}
		return connections
	}

	// Fallback: возвращаем пустой список, так как старые соединения отключены
	return []string{}
}

// GetCandles получает свечи для символа и таймфрейма через Binance API
func (m *Manager) GetCandles(symbol, timeframe string, limit int) ([]map[string]interface{}, error) {
	if m.symbolSupplementer == nil {
		return nil, fmt.Errorf("SymbolSupplementer не инициализирован")
	}

	// Получаем свечи через SymbolSupplementer
	candles, err := m.symbolSupplementer.GetCandles(symbol, timeframe, limit)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения свечей: %v", err)
	}

	// Преобразуем в map[string]interface{} для совместимости
	result := make([]map[string]interface{}, len(candles))
	for i, candle := range candles {
		result[i] = map[string]interface{}{
			"openTime":       candle.OpenTime,
			"open":           candle.Open,
			"high":           candle.High,
			"low":            candle.Low,
			"close":          candle.Close,
			"volume":         candle.Volume,
			"closeTime":      candle.CloseTime,
			"quoteVolume":    candle.QuoteVolume,
			"numberOfTrades": candle.NumberOfTrades,
			"takerBuyVolume": candle.TakerBuyVolume,
			"takerBuyQuote":  candle.TakerBuyQuote,
		}
	}

	return result, nil
}

// GetCandlesForTimeframes получает свечи для нескольких таймфреймов одновременно
func (m *Manager) GetCandlesForTimeframes(symbol string, timeframes []string, limit int) (map[string][]map[string]interface{}, error) {
	if m.symbolSupplementer == nil {
		return nil, fmt.Errorf("SymbolSupplementer не инициализирован")
	}

	// Получаем свечи через SymbolSupplementer
	candlesMap, err := m.symbolSupplementer.GetCandlesForTimeframes(symbol, timeframes, limit)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения свечей для таймфреймов: %v", err)
	}

	// Преобразуем в map[string]interface{} для совместимости
	result := make(map[string][]map[string]interface{})
	for timeframe, candles := range candlesMap {
		result[timeframe] = make([]map[string]interface{}, len(candles))
		for i, candle := range candles {
			result[timeframe][i] = map[string]interface{}{
				"openTime":       candle.OpenTime,
				"open":           candle.Open,
				"high":           candle.High,
				"low":            candle.Low,
				"close":          candle.Close,
				"volume":         candle.Volume,
				"closeTime":      candle.CloseTime,
				"quoteVolume":    candle.QuoteVolume,
				"numberOfTrades": candle.NumberOfTrades,
				"takerBuyVolume": candle.TakerBuyVolume,
				"takerBuyQuote":  candle.TakerBuyQuote,
			}
		}
	}

	return result, nil
}

// GetCandlesForAllTimeframes получает свечи для всех поддерживаемых таймфреймов
func (m *Manager) GetCandlesForAllTimeframes(symbol string, limit int) (map[string][]map[string]interface{}, error) {
	allTimeframes := []string{
		TimeframeM1, TimeframeM5, TimeframeM15, TimeframeH1, TimeframeH4,
		TimeframeD1, TimeframeW1, TimeframeMN1, TimeframeQ1, TimeframeY1,
	}

	return m.GetCandlesForTimeframes(symbol, allTimeframes, limit)
}

// GetCandlesForQuarter получает квартальные свечи для символа
func (m *Manager) GetCandlesForQuarter(symbol string, limit int) ([]map[string]interface{}, error) {
	return m.GetCandles(symbol, TimeframeQ1, limit)
}

// GetCandlesForYear получает годовые свечи для символа
func (m *Manager) GetCandlesForYear(symbol string, limit int) ([]map[string]interface{}, error) {
	return m.GetCandles(symbol, TimeframeY1, limit)
}
