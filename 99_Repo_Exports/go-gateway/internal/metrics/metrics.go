// Package metrics provides Prometheus metrics for Go Gateway
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// GatewayMetrics holds all Prometheus metrics for the gateway
type GatewayMetrics struct {
	GatewayUp *prometheus.GaugeVec

	// Counters
	SignalsTotal        *prometheus.CounterVec
	OrdersEnqueuedTotal *prometheus.CounterVec
	OrdersPushedTotal   *prometheus.CounterVec
	DealsWinTotal       *prometheus.CounterVec
	DealsLossTotal      *prometheus.CounterVec

	// Gauges - текущие значения из Analytics v2.0
	LastThreshold *prometheus.GaugeVec // labels: strategy, symbol
	AvgPnlUsd     *prometheus.GaugeVec // labels: strategy, symbol
	Winrate       *prometheus.GaugeVec // labels: strategy, symbol
	LastAUC       *prometheus.GaugeVec // labels: strategy, symbol

	// Execution Quality
	ExecutionSlippageHist   *prometheus.HistogramVec
	SignalToFillLatencyHist *prometheus.HistogramVec
}

// M is the global metrics instance
var M *GatewayMetrics

// Register initializes and registers all Prometheus metrics
func Register() {
	M = &GatewayMetrics{
		GatewayUp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gateway_up",
				Help: "1 if gateway is up, 0 otherwise",
			},
			[]string{"service"},
		),

		SignalsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "signals_total",
				Help: "Total number of signals received",
			},
			[]string{"strategy", "symbol"},
		),

		OrdersEnqueuedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "orders_enqueued_total",
				Help: "Total number of orders enqueued",
			},
			[]string{"strategy", "symbol"},
		),

		OrdersPushedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "orders_pushed_total",
				Help: "Total number of orders pushed to MT5",
			},
			[]string{"strategy", "symbol"},
		),

		DealsWinTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "deals_win_total",
				Help: "Total number of winning deals",
			},
			[]string{"strategy", "symbol"},
		),

		DealsLossTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "deals_loss_total",
				Help: "Total number of losing deals",
			},
			[]string{"strategy", "symbol"},
		),

		LastThreshold: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "strategy_last_threshold",
				Help: "Current tuned threshold from Analytics v2.0",
			},
			[]string{"strategy", "symbol"},
		),

		AvgPnlUsd: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "strategy_avg_pnl_usd",
				Help: "Average P/L in USD (last window) from Analytics v2.0",
			},
			[]string{"strategy", "symbol"},
		),

		Winrate: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "strategy_winrate",
				Help: "Winrate 0..1 (last window) from Analytics v2.0",
			},
			[]string{"strategy", "symbol"},
		),

		LastAUC: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "strategy_last_auc",
				Help: "AUC from ROC tuner (Analytics v2.0)",
			},
			[]string{"strategy", "symbol"},
		),

		ExecutionSlippageHist: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "execution_slippage_bps",
				Help:    "Execution slippage in basis points relative to signal entry price",
				Buckets: []float64{-5, -2, -1, 0, 1, 2, 5, 10, 20, 50},
			},
			[]string{"strategy", "symbol"},
		),

		SignalToFillLatencyHist: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "signal_to_fill_latency_ms",
				Help:    "Latency in milliseconds from signal generation to execution fill",
				Buckets: []float64{10, 50, 100, 200, 500, 1000, 2000, 5000},
			},
			[]string{"strategy", "symbol"},
		),
	}

	// Register all metrics
	prometheus.MustRegister(
		M.GatewayUp,
		M.SignalsTotal,
		M.OrdersEnqueuedTotal,
		M.OrdersPushedTotal,
		M.DealsWinTotal,
		M.DealsLossTotal,
		M.LastThreshold,
		M.AvgPnlUsd,
		M.Winrate,
		M.LastAUC,
		M.ExecutionSlippageHist,
		M.SignalToFillLatencyHist,
	)

	// Set initial state
	M.GatewayUp.WithLabelValues("go-gateway").Set(1)
}

// Handler returns HTTP handler for /metrics endpoint
func Handler() http.Handler {
	return promhttp.Handler()
}

// ObserveSignal increments signal counter
func ObserveSignal(strategy, symbol string) {
	if M == nil {
		return
	}
	M.SignalsTotal.WithLabelValues(strategy, symbol).Inc()
}

// ObserveOrderEnqueued increments order enqueued counter
func ObserveOrderEnqueued(strategy, symbol string) {
	if M == nil {
		return
	}
	M.OrdersEnqueuedTotal.WithLabelValues(strategy, symbol).Inc()
}

// ObserveOrderPushed increments order pushed counter
func ObserveOrderPushed(strategy, symbol string) {
	if M == nil {
		return
	}
	M.OrdersPushedTotal.WithLabelValues(strategy, symbol).Inc()
}

// ObserveDeal records a deal result (win/loss based on PnL)
func ObserveDeal(strategy, symbol string, pnlUSD float64) {
	if M == nil {
		return
	}
	if pnlUSD >= 0 {
		M.DealsWinTotal.WithLabelValues(strategy, symbol).Inc()
	} else {
		M.DealsLossTotal.WithLabelValues(strategy, symbol).Inc()
	}
}

// SetThreshold updates threshold gauge from Analytics v2.0
func SetThreshold(strategy, symbol string, thr float64) {
	if M == nil {
		return
	}
	M.LastThreshold.WithLabelValues(strategy, symbol).Set(thr)
}

// SetAUC updates AUC gauge from Analytics v2.0
func SetAUC(strategy, symbol string, auc float64) {
	if M == nil {
		return
	}
	M.LastAUC.WithLabelValues(strategy, symbol).Set(auc)
}

// SetWinrate updates winrate gauge from Analytics v2.0
func SetWinrate(strategy, symbol string, wr float64) {
	if M == nil {
		return
	}
	M.Winrate.WithLabelValues(strategy, symbol).Set(wr)
}

// SetAvgPnl updates average P/L gauge from Analytics v2.0
func SetAvgPnl(strategy, symbol string, avg float64) {
	if M == nil {
		return
	}
	M.AvgPnlUsd.WithLabelValues(strategy, symbol).Set(avg)
}

// Heartbeat sends periodic heartbeat to indicate gateway is alive
func Heartbeat() {
	if M == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			M.GatewayUp.WithLabelValues("go-gateway").Set(1)
		}
	}()
}
