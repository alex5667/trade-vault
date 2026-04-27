package monitoring

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	hyperliquidMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hyperliquid_messages_total",
			Help: "Количество обработанных сообщений Hyperliquid WS по типам",
		},
		[]string{"symbol", "type"},
	)

	hyperliquidReconnectsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hyperliquid_reconnects_total",
			Help: "Количество переподключений Hyperliquid WS по причинам",
		},
		[]string{"reason"},
	)

	hyperliquidLiveSubscriptionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hyperliquid_ws_live_subscriptions_total",
			Help: "Количество успешно отправленных subscribe/unsubscribe команд Hyperliquid",
		},
		[]string{"method"},
	)

	hyperliquidParseErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hyperliquid_parse_errors_total",
			Help: "Количество ошибок парсинга сообщений Hyperliquid",
		},
	)
)

func RecordHyperliquidMessage(symbol, msgType string) {
	if symbol == "" || msgType == "" {
		return
	}
	hyperliquidMessagesTotal.WithLabelValues(strings.ToUpper(symbol), msgType).Inc()
}

func RecordHyperliquidReconnect(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	hyperliquidReconnectsTotal.WithLabelValues(reason).Inc()
}

func RecordHyperliquidLiveSubscription(method string) {
	if method == "" {
		method = "unknown"
	}
	hyperliquidLiveSubscriptionsTotal.WithLabelValues(strings.ToUpper(method)).Inc()
}

func RecordHyperliquidParseError() {
	hyperliquidParseErrorsTotal.Inc()
}
