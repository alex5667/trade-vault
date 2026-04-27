package monitoring

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ---- Latency Audit (Phase 1) — Process‑message duration histogram ----

// ProcessMessageDurationMs measures the full processMessage() call in the
// stream controller (Normalize → health → PublishTick/Book).
// Labels: exchange ("binance", "bybit", "hyperliquid"), type ("tick", "book", "mixed").
// Buckets are sub‑millisecond to 50 ms — anything above 50 ms is a hard SLO breach.
var ProcessMessageDurationMs = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "go_worker_process_message_duration_ms",
		Help:    "Time spent in controller.processMessage (ms). SLO: p99 < 8ms.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 10, 15, 25, 50},
	},
	[]string{"exchange", "type"},
)

// RedisXaddDurationMs measures the XADD call latency (per-tick or per-book publish).
// This isolates Redis write latency from normalization overhead.
var RedisXaddDurationMs = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "go_worker_redis_xadd_duration_ms",
		Help:    "Time spent in a single Redis XADD publish (ms). SLO: p99 < 3ms.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 25},
	},
	[]string{"exchange", "type"},
)

// RecordProcessMessageDuration records the processMessage latency for a given
// exchange and message type. durationMs MUST be pre‑computed by the caller
// as float64(time.Since(start).Microseconds()) / 1000.0 for sub‑ms precision.
func RecordProcessMessageDuration(exchange, msgType string, durationMs float64) {
	if exchange == "" {
		exchange = "unknown"
	}
	ProcessMessageDurationMs.WithLabelValues(strings.ToLower(exchange), msgType).Observe(durationMs)
}

// RecordRedisXaddDuration records a single XADD call's latency.
func RecordRedisXaddDuration(exchange, msgType string, durationMs float64) {
	if exchange == "" {
		exchange = "unknown"
	}
	RedisXaddDurationMs.WithLabelValues(strings.ToLower(exchange), msgType).Observe(durationMs)
}
