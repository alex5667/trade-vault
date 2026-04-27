// internal/metrics/health.go
package metrics

import (
	"context"
	"math"
	"sync"
	"time"

	"go-worker/internal/monitoring"

	"github.com/redis/go-redis/v9"
)

type TickMetricsInput struct {
	Symbol       string
	L2AgeMs      float64
	L2AgeMsTick  float64
	L2IsStale    bool // Гейт для сигналов (относительно тика)
	L2IsStaleNow bool // Диагностика пайплайна (относительно now)
	EtaFillMs    *float64
	BurstRatio   *float64
	ImbalanceMin *float64
}

type SymbolBucket struct {
	TicksTotal       int64
	TicksWithL2      int64
	TicksL2StaleTick int64 // Stale относительно тика (для сигналов)
	TicksL2StaleNow  int64 // Stale относительно now (для SRE)

	SumL2AgeMs     float64
	SumL2AgeMsTick float64

	SumEtaFillMs float64
	EtaFillCount int64

	SumBurstRatio float64
	BurstCount    int64

	SumImbalanceMin float64
	ImbalanceCount  int64

	SignalsEmitted int64
	DlqCount       int64
}

type HealthMetrics struct {
	mu      sync.Mutex
	buckets map[string]*SymbolBucket
	window  time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewHealthMetrics(rdb *redis.Client, window time.Duration) *HealthMetrics {
	ctx, cancel := context.WithCancel(context.Background())
	return &HealthMetrics{
		buckets: make(map[string]*SymbolBucket),
		window:  window,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (h *HealthMetrics) getBucket(symbol string) *SymbolBucket {
	b, ok := h.buckets[symbol]
	if !ok {
		b = &SymbolBucket{}
		h.buckets[symbol] = b
	}
	return b
}

func (h *HealthMetrics) OnTick(input TickMetricsInput) {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := h.getBucket(input.Symbol)

	b.TicksTotal++
	if !isNaN(input.L2AgeMs) {
		b.TicksWithL2++
		b.SumL2AgeMs += input.L2AgeMs
		b.SumL2AgeMsTick += input.L2AgeMsTick
	}
	if input.L2IsStale {
		b.TicksL2StaleTick++
	}
	if input.L2IsStaleNow {
		b.TicksL2StaleNow++
	}

	if input.EtaFillMs != nil && !isNaN(*input.EtaFillMs) {
		b.SumEtaFillMs += *input.EtaFillMs
		b.EtaFillCount++
	}
	if input.BurstRatio != nil && !isNaN(*input.BurstRatio) {
		b.SumBurstRatio += *input.BurstRatio
		b.BurstCount++
	}
	if input.ImbalanceMin != nil && !isNaN(*input.ImbalanceMin) {
		b.SumImbalanceMin += *input.ImbalanceMin
		b.ImbalanceCount++
	}
}

func (h *HealthMetrics) OnSignalEmit(symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := h.getBucket(symbol)
	b.SignalsEmitted++
}

func (h *HealthMetrics) OnDLQ(symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := h.getBucket(symbol)
	b.DlqCount++
}

func (h *HealthMetrics) Run() {
	ticker := time.NewTicker(h.window)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.flushSnapshot()
		case <-h.ctx.Done():
			return
		}
	}
}

func (h *HealthMetrics) Stop() {
	h.cancel()
}

func (h *HealthMetrics) flushSnapshot() {
	h.mu.Lock()
	buckets := h.buckets
	h.buckets = make(map[string]*SymbolBucket)
	h.mu.Unlock()

	if len(buckets) == 0 {
		return
	}

	windowSec := h.window.Seconds()

	for symbol, b := range buckets {
		if b.TicksTotal == 0 && b.SignalsEmitted == 0 && b.DlqCount == 0 {
			continue
		}

		var l2StaleRatioTick, l2StaleRatioNow float64
		if b.TicksWithL2 > 0 {
			l2StaleRatioTick = float64(b.TicksL2StaleTick) / float64(b.TicksWithL2)
			l2StaleRatioNow = float64(b.TicksL2StaleNow) / float64(b.TicksWithL2)
		}

		var signalEmitRate float64
		if windowSec > 0 {
			signalEmitRate = float64(b.SignalsEmitted) / windowSec
		}

		// ВМЕСТО Redis Pipeline экспортируем напрямую в Prometheus
		monitoring.RecordOrderflowHealth(symbol, l2StaleRatioTick, l2StaleRatioNow, signalEmitRate)
	}
}

// Unified pipeline метрики
func (h *HealthMetrics) IncUnifiedError(symbol string) {
	// (Priority 5) - Unified error is recorded via prometheus in internal/monitoring
	monitoring.RecordFuturesReconnect(symbol, "unified_error")
}

func (h *HealthMetrics) LogUnifiedFallbackOnce(symbol string, err error) {
	// Добавляем metric counter для Fallbacks (часть Priority 5)
	monitoring.RecordFuturesReconnect(symbol, "unified_fallback")
}

func isNaN(f float64) bool {
	return math.IsNaN(f)
}
