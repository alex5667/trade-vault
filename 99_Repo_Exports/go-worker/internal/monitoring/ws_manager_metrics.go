// Package monitoring — критические агрегированные метрики для critical path.
//
// Эти метрики дополняют per-symbol метрики в websocket_metrics.go:
//   - ws_connections_active        — сколько WebSocket-соединений живо прямо сейчас
//   - ws_messages_total            — суммарный поток входящих WS-сообщений (label: timeframe)
//   - ws_reconnects_total          — суммарные переподключения (label: timeframe, reason)
//   - ws_disconnects_total         — суммарные разрывы соединения (label: exchange, reason)
//   - go_worker_publish_latency_seconds — гистограмма задержки публикации через CandlePublisher (p99 SLO)
//   - go_worker_candle_stream_length    — текущая длина candle Redis Stream (gauge per stream)
//   - parse_errors_total           — ошибки парсинга/нормализации сообщений биржи (label: exchange, symbol)
//   - liq_events_total             — алиас суммарного потока liquidation-событий (label: source)
//
// Метрики экспортируются через пакет-уровневые функции, чтобы не тянуть зависимость
// на конкретный struct в hot-path (нет аллокаций на вызов).
package monitoring

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// WsConnectionsActive — gauge активных WebSocket-соединений по таймфрейму.
	// Инкрементируется при создании соединения, декрементируется при Stop().
	// Используется для алерта: ws_connections_active == 0 при работающем воркере.
	WsConnectionsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ws_connections_active",
			Help: "Количество активных WebSocket-соединений к бирже (по таймфрейму).",
		},
		[]string{"exchange", "timeframe"},
	)

	// WsMessagesTotal — суммарный счётчик входящих WS-сообщений (все символы).
	// Увеличивается на каждый успешно прочитанный фрейм ДО processMessage.
	// Позволяет отличить: сообщения пришли, но не опубликованы (redis_publish ошибки).
	WsMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ws_messages_total",
			Help: "Суммарное количество WebSocket-сообщений, принятых воркером.",
		},
		[]string{"exchange", "timeframe"},
	)

	// WsReconnectsTotal — суммарные переподключения WebSocket (не per-symbol).
	// Label reason: connection_reset | abnormal_closure | timeout | other_error | circuit_breaker.
	WsReconnectsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ws_reconnects_total",
			Help: "Суммарные переподключения WebSocket (не per-symbol).",
		},
		[]string{"exchange", "timeframe", "reason"},
	)

	// WsDisconnectsTotal — счётчик разрывов WS-соединений (P2).
	// Инкрементируется при каждом закрытии соединения до reconnect.
	// Labels:
	//   exchange — биржа (binance | bybit | hyperliquid)
	//   reason   — причина разрыва (connection_reset | abnormal_closure | timeout | other_error | graceful)
	// Алерт: rate(ws_disconnects_total[5m]) > 0.1 → possible cascade disconnect.
	WsDisconnectsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ws_disconnects_total",
			Help: "Суммарные разрывы WS-соединений (каждый reconnect = 1 disconnect). SLO: < 2/min per exchange.",
		},
		[]string{"exchange", "reason"},
	)

	// PublishLatencySeconds — гистограмма задержки одного вызова PublishCandleData (P2).
	// Заменяет RedisPublishDurationSeconds, добавляя gauge p99 для PromQL.
	// SLO: p99 < 5ms (0.005s).
	// Buckets охватывают диапазон 100µs – 100ms.
	PublishLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_worker_publish_latency_seconds",
			Help:    "Задержка публикации одного события CandlePublisher.PublishCandleData (секунды). SLO: p99 < 5ms.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100},
		},
		[]string{"exchange"},
	)

	// RedisPublishDurationSeconds оставлен для обратной совместимости с существующими дашбордами.
	// Новый код использует PublishLatencySeconds.
	RedisPublishDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_publish_duration_seconds",
			Help:    "Время одного вызова CandlePublisher.PublishCandleData (секунды). SLO: p99 < 5ms. (deprecated: use go_worker_publish_latency_seconds)",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100},
		},
		[]string{"exchange", "timeframe"},
	)

	// CandleStreamLength — текущая длина Redis Stream candles:data (P2 gauging).
	// Устанавливается периодически (например, каждые 30s) фоновым горутином.
	// Labels:
	//   stream — имя стрима (candles:data | candles:data:worker)
	// Алерт: go_worker_candle_stream_length > CANDLE_STREAM_MAXLEN * 0.9 → danger zone.
	CandleStreamLength = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_worker_candle_stream_length",
			Help: "Текущая длина Redis Stream (entries count). Алерт при > 90%% от MAXLEN.",
		},
		[]string{"stream"},
	)

	// ParseErrorsTotal — счётчик ошибок parse/нормализации сообщений биржи (P2).
	// Аналог futuresDecodeErrorsTotal, но с каноническим именем parse_errors_total
	// и label-set совместимым с Prometheus naming conventions.
	// Labels:
	//   exchange — биржа (binance | bybit | hyperliquid)
	//   symbol   — торговая пара (BTCUSDT | unknown)
	// Алерт: rate(parse_errors_total[5m]) > 5 → possible exchange API change.
	ParseErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "parse_errors_total",
			Help: "Ошибки парсинга/нормализации сообщений биржи. Алерт: > 5/min → potential exchange API change.",
		},
		[]string{"exchange", "symbol"},
	)

	// LiqEventsTotal — суммарный счётчик liquidation-событий после parse/DQ/publish.
	// label status: received | published | dropped | quarantined.
	// Агрегирует liq_events_in_total + liq_events_published_total в одном месте
	// для простого алерта: rate(liq_events_total{status="published"}) == 0.
	LiqEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "liq_events_total",
			Help: "Суммарный счётчик liquidation-событий по статусу (received/published/dropped/quarantined).",
		},
		[]string{"source", "status"},
	)
)

// RecordWSDisconnect инкрементирует ws_disconnects_total при каждом разрыве WS-соединения.
// Вызывается перед reconnect-попыткой в hot-path.
// reason: connection_reset | abnormal_closure | timeout | other_error | graceful.
func RecordWSDisconnect(exchange, reason string) {
	if exchange == "" {
		exchange = "unknown"
	}
	if reason == "" {
		reason = "other_error"
	}
	WsDisconnectsTotal.WithLabelValues(strings.ToLower(exchange), reason).Inc()
}

// RecordPublishLatency записывает задержку одной публикации в Redis Stream (секунды).
// Вызывается ПОСЛЕ каждого XAdd в CandlePublisher.PublishCandleData.
// exchange: binance | bybit | hyperliquid.
func RecordPublishLatency(exchange string, latencySeconds float64) {
	if exchange == "" {
		exchange = "unknown"
	}
	PublishLatencySeconds.WithLabelValues(strings.ToLower(exchange)).Observe(latencySeconds)
}

// SetStreamLength устанавливает gauge текущей длины Redis Stream.
// Вызывается из фонового горутина раз в 30s (не в hot-path).
// stream: имя Redis-стрима (e.g. "candles:data").
func SetStreamLength(stream string, length int64) {
	if stream == "" {
		return
	}
	CandleStreamLength.WithLabelValues(stream).Set(float64(length))
}

// RecordParseError инкрементирует parse_errors_total при ошибке парсинга/нормализации сообщения биржи.
// exchange: binance | bybit | hyperliquid.
// symbol: торговая пара или "unknown".
func RecordParseError(exchange, symbol string) {
	if exchange == "" {
		exchange = "unknown"
	}
	if symbol == "" {
		symbol = "unknown"
	}
	ParseErrorsTotal.WithLabelValues(strings.ToLower(exchange), strings.ToUpper(symbol)).Inc()
}
