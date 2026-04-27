package monitoring

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Отдельные метрики для Bybit futures ingestion.
// Не используем binance_* имена, чтобы не смешивать источники.

var (
	bybitFuturesMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bybit_futures_messages_total",
			Help: "Количество обработанных сообщений Bybit Futures по типам",
		},
		[]string{"symbol", "type"},
	)

	bybitFuturesReconnectsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bybit_futures_reconnects_total",
			Help: "Количество переподключений Bybit Futures WebSocket по причинам",
		},
		[]string{"symbol", "reason"},
	)

	bybitWSLiveSubscriptionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bybit_ws_live_subscriptions_total",
			Help: "Количество успешно отправленных live-команд subscribe/unsubscribe (Bybit)",
		},
		[]string{"method"},
	)

	// bybitBookDeltaGapsTotal counts orderbook delta sequence gaps per symbol.
	// A gap means delta.UpdateID != prevUpdateID+1; the local book is flushed
	// and a re-snapshot is forced.  Non-zero rate → investigate WS lag or drops.
	bybitBookDeltaGapsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bybit_book_delta_gaps_total",
			Help: "Количество разрывов последовательности дельт книги Bybit (FlushAndResnapshot)",
		},
		[]string{"symbol"},
	)
)

// RecordBybitFuturesMessage увеличивает счётчик сообщений для символа и типа.
func RecordBybitFuturesMessage(symbol, msgType string) {
	if symbol == "" || msgType == "" {
		return
	}
	bybitFuturesMessagesTotal.WithLabelValues(strings.ToUpper(symbol), msgType).Inc()
}

// RecordBybitFuturesReconnect увеличивает счётчик переподключений.
func RecordBybitFuturesReconnect(symbol, reason string) {
	if symbol == "" {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	bybitFuturesReconnectsTotal.WithLabelValues(strings.ToUpper(symbol), reason).Inc()
}

// RecordBybitLiveSubscription фиксирует успешную subscribe/unsubscribe.
func RecordBybitLiveSubscription(method string) {
	if method == "" {
		method = "unknown"
	}
	bybitWSLiveSubscriptionsTotal.WithLabelValues(method).Inc()
}

// RecordBybitBookDeltaGap увеличивает счётчик разрывов последовательности
// дельт книги для символа.  Вызывается сразу после сброса локального состояния
// и записи события в dlq:book_deltas.
func RecordBybitBookDeltaGap(symbol string) {
	if symbol == "" {
		return
	}
	bybitBookDeltaGapsTotal.WithLabelValues(strings.ToUpper(symbol)).Inc()
}
