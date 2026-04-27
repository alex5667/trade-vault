package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Метрики для ingestion потока ликвидаций.
//
// Важно разделять:
//   - parse/validation ошибки (плохой формат)
//   - DQ-отбросы (stale/out-of-order/unknown symbol)
//   - задержки (end-to-end)
var (
	liqWsConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "liq_ws_connected",
			Help: "Статус WS соединения по источнику (1 = connected, 0 = disconnected)",
		},
		[]string{"source"},
	)

	liqEventsInTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "liq_events_in_total",
			Help: "Сколько liquidation событий пришло из WS (до фильтрации)",
		},
		[]string{"source"},
	)

	liqEventsPublishedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "liq_events_published_total",
			Help: "Сколько liquidation событий опубликовано в Redis Stream",
		},
		[]string{"source"},
	)

	liqEventsDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "liq_events_dropped_total",
			Help: "Сколько liquidation событий было отброшено (DQ/фильтр/очередь)",
		},
		[]string{"source", "reason"},
	)

	// liq_events_quarantined_total — подмножество dropped, отправленных в quarantine stream.
	// Исключает filtered_symbol и dedup (они не являются DQ-нарушениями).
	// Используется для расчёта quarantine rate: sum(rate(quarantined)) / sum(rate(in)).
	liqEventsQuarantinedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "liq_events_quarantined_total",
			Help: "Liquidation события, отправленные в quarantine stream (DQ-нарушения: stale/bad_ts/out_of_order/queue_full)",
		},
		[]string{"source", "reason"},
	)

	liqParseErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "liq_parse_errors_total",
			Help: "Ошибки парсинга WS payload",
		},
		[]string{"source"},
	)

	liqEventLagMs = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "liq_event_lag_ms",
			Help:    "Лаг события (now_ms - event_ts_ms)",
			Buckets: []float64{0, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10000},
		},
		[]string{"source"},
	)

	liqEventsRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "liq_events_rate_eps",
			Help: "Оценка входного потока событий (events/sec)",
		},
		[]string{"source"},
	)
)

type LiquidationMetrics struct {
	lastTick map[string]time.Time
}

func NewLiquidationMetrics() *LiquidationMetrics {
	return &LiquidationMetrics{lastTick: map[string]time.Time{}}
}

func (m *LiquidationMetrics) SetConnected(source string, ok bool) {
	if ok {
		liqWsConnections.WithLabelValues(source).Set(1)
		return
	}
	liqWsConnections.WithLabelValues(source).Set(0)
}

func (m *LiquidationMetrics) IncIn(source string, n int) {
	if n <= 0 {
		return
	}
	liqEventsInTotal.WithLabelValues(source).Add(float64(n))
}

func (m *LiquidationMetrics) IncPublished(source string, n int) {
	if n <= 0 {
		return
	}
	liqEventsPublishedTotal.WithLabelValues(source).Add(float64(n))
}

func (m *LiquidationMetrics) Drop(source, reason string, n int) {
	if n <= 0 {
		return
	}
	liqEventsDroppedTotal.WithLabelValues(source, reason).Add(float64(n))
}

// IncQuarantined регистрирует событие, реально отправленное в quarantine stream.
// Вызывать только из controller.quarantine() — не для filtered_symbol/dedup.
func (m *LiquidationMetrics) IncQuarantined(source, reason string, n int) {
	if n <= 0 {
		return
	}
	liqEventsQuarantinedTotal.WithLabelValues(source, reason).Add(float64(n))
}

func (m *LiquidationMetrics) ParseErr(source string) {
	liqParseErrorsTotal.WithLabelValues(source).Inc()
}

func (m *LiquidationMetrics) ObserveLag(source string, lagMs int64) {
	if lagMs < 0 {
		lagMs = 0
	}
	liqEventLagMs.WithLabelValues(source).Observe(float64(lagMs))
}

// TickRate обновляет gauge EPS (примерная оценка).
// Вызывайте не чаще, чем раз в ~1s.
func (m *LiquidationMetrics) TickRate(source string, events int) {
	now := time.Now()
	prev, ok := m.lastTick[source]
	if !ok {
		m.lastTick[source] = now
		return
	}
	dt := now.Sub(prev).Seconds()
	if dt <= 0 {
		return
	}
	liqEventsRate.WithLabelValues(source).Set(float64(events) / dt)
	m.lastTick[source] = now
}
