package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Метрики Bybit REST (market snapshots).
// Цель: быстро диагностировать деградации (rate limit, таймауты, ошибки формата).

var (
	BybitRestRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bybit_rest_requests_total",
			Help: "Total number of Bybit REST requests by endpoint and result",
		},
		[]string{"endpoint", "result"}, // result: ok|error
	)

	BybitRedisPublishesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bybit_redis_publishes_total",
			Help: "Total number of Bybit Redis stream publish attempts by stream and result",
		},
		[]string{"stream", "result"}, // result: ok|error
	)
)

func RecordBybitRest(endpoint string, ok bool) {
	if endpoint == "" {
		endpoint = "unknown"
	}
	if ok {
		BybitRestRequestsTotal.WithLabelValues(endpoint, "ok").Inc()
		return
	}
	BybitRestRequestsTotal.WithLabelValues(endpoint, "error").Inc()
}

func RecordBybitPublish(stream string, ok bool) {
	if stream == "" {
		stream = "unknown"
	}
	if ok {
		BybitRedisPublishesTotal.WithLabelValues(stream, "ok").Inc()
		return
	}
	BybitRedisPublishesTotal.WithLabelValues(stream, "error").Inc()
}
