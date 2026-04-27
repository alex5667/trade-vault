package monitoring

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	futuresMsgLabelSymbol = "symbol"
	futuresMsgLabelType   = "type"
)

var (
	binanceFuturesMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "binance_futures_messages_total",
			Help: "Количество обработанных сообщений Binance Futures по типам",
		},
		[]string{futuresMsgLabelSymbol, futuresMsgLabelType},
	)

	binanceFuturesReconnectsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "binance_futures_reconnects_total",
			Help: "Количество переподключений Binance Futures WebSocket по причинам",
		},
		[]string{futuresMsgLabelSymbol, "reason"},
	)

	futuresMessagesUnifiedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "futures_messages_total",
			Help: "Unified processed futures messages by exchange, symbol, and type",
		},
		[]string{"exchange", futuresMsgLabelSymbol, futuresMsgLabelType},
	)

	futuresReconnectsUnifiedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "futures_reconnects_total",
			Help: "Unified reconnects of Futures WebSockets by exchange, symbol, and reason",
		},
		[]string{"exchange", "reason"},
	)

	OrderflowL2StaleRatioTickGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orderflow_l2_stale_ratio_tick",
			Help: "Ratio of ticks with L2 age exceeding threshold (tick-relative)",
		},
		[]string{futuresMsgLabelSymbol},
	)

	OrderflowL2StaleRatioNowGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orderflow_l2_stale_ratio_now",
			Help: "Ratio of ticks with L2 age exceeding threshold (now-relative)",
		},
		[]string{futuresMsgLabelSymbol},
	)

	OrderflowSignalEmitRateGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orderflow_signal_emit_rate",
			Help: "Signals emitted per second",
		},
		[]string{futuresMsgLabelSymbol},
	)

	BinanceClockDriftMsGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "binance_clock_drift_ms",
			Help: "Смещение часов сервера относительно EventTime биржи (NTP drift)",
		},
		[]string{futuresMsgLabelSymbol},
	)

	BinanceWSLiveSubscriptionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "binance_ws_live_subscriptions_total",
			Help: "Количество успешно отправленных live-команд SUBSCRIBE/UNSUBSCRIBE",
		},
		[]string{"method"},
	)

	GoWorkerDrainTimeoutTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "go_worker_graceful_drain_timeout_total",
			Help: "Количество таймаутов при попытке graceful drain",
		},
	)

	// BatchPublisher метрики (Priority 8)
	BatchPublisherDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_worker_batch_publisher_dropped_total",
			Help: "Total number of messages dropped due to buffer full",
		},
		[]string{"stream"},
	)

	BatchPublisherPublishedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "go_worker_batch_publisher_published_total",
			Help: "Total number of messages successfully published by BatchTickPublisher",
		},
	)

	BatchPublisherFlushErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "go_worker_batch_publisher_flush_errors_total",
			Help: "Total number of Pipeline flush errors in BatchTickPublisher",
		},
	)

	// Backpressure: how many times channel was full and we waited before dropping
	BatchPublisherBackpressureTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "go_worker_batch_publisher_backpressure_total",
			Help: "Total backpressure events: channel full, waited before drop",
		},
	)

	// Channel fill ratio at time of overflow (leading indicator for capacity)
	BatchPublisherChanFillRatio = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_worker_batch_publisher_chan_fill_ratio",
			Help: "Channel fill ratio at time of overflow (0=empty, 1=full). Leading indicator for backpressure.",
		},
	)

	// P1 fix: WS read-loop load-shedding drops (buffer overflow вместо reconnect)
	FuturesMessagesDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_worker_futures_messages_dropped_total",
			Help: "WS messages dropped due to msgChan overflow (load-shedding, not reconnect)",
		},
		[]string{"stream"},
	)

	// WSMsgChanFillRatio — leading indicator for backpressure (0=empty, 1=full).
	// Alert threshold: > 0.9 for 30s → increase *_WS_MSG_CHAN_CAP.
	WSMsgChanFillRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_worker_ws_msg_chan_fill_ratio",
			Help: "Fraction of WS message channel capacity in use at time of drop (0=empty, 1=full). Leading indicator for backpressure.",
		},
		[]string{"exchange"},
	)

	// decodeErrorsTotal: counts normalization failures for market data.
	// Alert: > 5 errors in 1m → potential exchange API change or malformed data.
	futuresDecodeErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_worker_futures_decode_errors_total",
			Help: "Total number of market data normalization/decoding errors",
		},
		[]string{"exchange", futuresMsgLabelSymbol},
	)

	// DLQ write errors — XAdd to a DLQ stream failed.
	// Alert: any sustained rate → DLQ delivery broken, events lost.
	dlqWriteErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_worker_dlq_write_errors_total",
			Help: "Total number of failed XAdd calls to DLQ streams",
		},
		[]string{"stream"},
	)

	// Timestamp fallback — exchange delivered ts<=0, we substituted now().
	// Alert: sustained rate on a symbol → exchange time issue.
	ingestTsFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_worker_ingest_ts_fallback_total",
			Help: "Total number of events where exchange timestamp was missing/zero and local time was substituted",
		},
		[]string{"exchange", "stream_type"},
	)

	// Batch pipeline retry — fallback single-XADD after pipeline failure.
	// Non-zero rate signals partial pipeline failure; reordering risk within batch.
	batchPipelineRetryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_worker_batch_pipeline_retry_total",
			Help: "Total single-XADD retries after pipeline flush failure (reordering risk)",
		},
		[]string{"stream", "result"},
	)
)

// RecordOrderflowHealth записывает метрики здоровья прямо в Prometheus.
func RecordOrderflowHealth(symbol string, staleTick, staleNow, signalRate float64) {
	if symbol == "" {
		return
	}
	s := strings.ToUpper(symbol)
	OrderflowL2StaleRatioTickGauge.WithLabelValues(s).Set(staleTick)
	OrderflowL2StaleRatioNowGauge.WithLabelValues(s).Set(staleNow)
	OrderflowSignalEmitRateGauge.WithLabelValues(s).Set(signalRate)
}

// RecordClockDrift записывает смещение часов сервера относительно биржи.
func RecordClockDrift(symbol string, driftMs float64) {
	if symbol == "" {
		return
	}
	BinanceClockDriftMsGauge.WithLabelValues(strings.ToUpper(symbol)).Set(driftMs)
}

// RecordLiveSubscription фиксирует успешную LIVE-подписку/отписку.
func RecordLiveSubscription(method string) {
	BinanceWSLiveSubscriptionsTotal.WithLabelValues(method).Inc()
}

// RecordDrainTimeout фиксирует таймаут при остановке воркера.
func RecordDrainTimeout() {
	GoWorkerDrainTimeoutTotal.Inc()
}

// RecordFuturesMessage увеличивает счётчик сообщений для символа и типа (aggTrade, depth).
func RecordFuturesMessage(symbol, msgType string) {
	if symbol == "" || msgType == "" {
		return
	}
	binanceFuturesMessagesTotal.WithLabelValues(strings.ToUpper(symbol), msgType).Inc()
}

// RecordFuturesReconnect увеличивает счётчик переподключений для символа и причины.
func RecordFuturesReconnect(symbol, reason string) {
	if symbol == "" {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	binanceFuturesReconnectsTotal.WithLabelValues(strings.ToUpper(symbol), reason).Inc()
}

// RecordBatchPublisherDropped фиксирует dropped сообщение BatchPublisher
func RecordBatchPublisherDropped(stream string) {
	if stream == "" {
		stream = "unknown"
	}
	BatchPublisherDroppedTotal.WithLabelValues(stream).Inc()
}

// RecordBatchPublisherPublished фиксирует успешно опуликованные batch msgs
func RecordBatchPublisherPublished(count int64) {
	BatchPublisherPublishedTotal.Add(float64(count))
}

// RecordBatchPublisherError фиксирует ошибку flush в BatchPublisher
func RecordBatchPublisherError() {
	BatchPublisherFlushErrorsTotal.Inc()
}

// RecordBatchPublisherBackpressure фиксирует событие backpressure (channel full, waiting before drop).
func RecordBatchPublisherBackpressure() {
	BatchPublisherBackpressureTotal.Inc()
}

// RecordBatchPublisherChanFillRatio записывает долю заполненности канала при overflow.
func RecordBatchPublisherChanFillRatio(ratio float64) {
	BatchPublisherChanFillRatio.Set(ratio)
}

// RecordFuturesMessageDropped фиксирует WS-сообщение сброшенное из-за переполнения буфера.
// P1 fix: заменяет panic-reconnect на graceful load-shedding.
func RecordFuturesMessageDropped(stream string) {
	FuturesMessagesDroppedTotal.WithLabelValues(stream).Inc()
}

// RecordFuturesMessageUnified records a message with standard exchange labels.
func RecordFuturesMessageUnified(exchange, symbol, msgType string) {
	if exchange == "" || symbol == "" || msgType == "" {
		return
	}
	futuresMessagesUnifiedTotal.WithLabelValues(strings.ToLower(exchange), strings.ToUpper(symbol), msgType).Inc()
}

// RecordFuturesReconnectUnified records a websocket reconnect with standard exchange labels.
func RecordFuturesReconnectUnified(exchange, reason string) {
	if exchange == "" {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	futuresReconnectsUnifiedTotal.WithLabelValues(strings.ToLower(exchange), reason).Inc()
}

// RecordWSChanFillRatio records the WS message channel fill ratio at the time of a drop.
// ratio = float64(len(msgChan)) / float64(cap(msgChan)) — must be in [0, 1].
// Call this together with RecordFuturesMessageDropped when load-shedding.
// Use exchange label: "binance", "bybit", "hyperliquid".
func RecordWSChanFillRatio(exchange string, ratio float64) {
	if exchange == "" {
		return
	}
	WSMsgChanFillRatio.WithLabelValues(strings.ToLower(exchange)).Set(ratio)
}

// RecordFuturesDecodeError increments the decode error counter.
func RecordFuturesDecodeError(exchange, symbol string) {
	if exchange == "" {
		return
	}
	if symbol == "" {
		symbol = "unknown"
	}
	futuresDecodeErrorsTotal.WithLabelValues(strings.ToLower(exchange), strings.ToUpper(symbol)).Inc()
	RecordParseError(exchange, symbol)
}

// RecordDLQWriteError increments the DLQ write error counter for a given stream.
// Call this whenever an XAdd to a DLQ stream returns an error.
func RecordDLQWriteError(stream string) {
	if stream == "" {
		stream = "unknown"
	}
	dlqWriteErrorsTotal.WithLabelValues(stream).Inc()
}

// RecordIngestTsFallback increments the timestamp-fallback counter.
// Call this when exchange ts is 0/missing and local time is substituted.
// exchange: "binance", "bybit"; streamType: "tick", "book", "liq", "candle".
func RecordIngestTsFallback(exchange, streamType string) {
	if exchange == "" {
		exchange = "unknown"
	}
	if streamType == "" {
		streamType = "unknown"
	}
	ingestTsFallbackTotal.WithLabelValues(strings.ToLower(exchange), streamType).Inc()
}

// RecordBatchPipelineRetry increments the batch pipeline retry counter.
// result: "ok" (single XADD succeeded) or "fail" (single XADD also failed).
func RecordBatchPipelineRetry(stream, result string) {
	if stream == "" {
		stream = "unknown"
	}
	batchPipelineRetryTotal.WithLabelValues(stream, result).Inc()
}
