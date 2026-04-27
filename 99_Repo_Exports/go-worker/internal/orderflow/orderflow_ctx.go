package orderflow

import (
	"time"

	"go-worker/internal/monitoring"
)

// StalenessConfig содержит пороги для определения staleness
type StalenessConfig struct {
	MaxAgeTickMs int64 // порог для l2_is_stale (гейт сигналов, относительно тика)
	MaxAgeNowMs  int64 // порог для l2_is_stale_now (алерты/SRE, относительно now)
}

// OrderflowCtx содержит контекст обработки тика с метриками staleness
type OrderflowCtx struct {
	Symbol string

	// Таймстемпы
	TickTsMs int64 // timestamp тика
	BookTsMs int64 // timestamp L2-книги
	NowMs    int64 // время обработки

	// Производные величины
	L2AgeMsTick float64 // tick_ts_ms - book_ts_ms
	L2AgeMsNow  float64 // now_ms - book_ts_ms

	// Флаги staleness
	L2IsStale    bool // Гейт для сигналов (относительно тика)
	L2IsStaleNow bool // Диагностика пайплайна (относительно now)
}

// ComputeStaleness рассчитывает метрики staleness и устанавливает флаги
func (c *OrderflowCtx) ComputeStaleness(cfg StalenessConfig) {
	// Защита от мусорных значений
	if c.BookTsMs == 0 || c.TickTsMs == 0 || c.NowMs == 0 {
		c.L2AgeMsTick = 0
		c.L2AgeMsNow = 0
		c.L2IsStale = false
		c.L2IsStaleNow = false
		return
	}

	c.L2AgeMsTick = float64(c.TickTsMs - c.BookTsMs)
	c.L2AgeMsNow = float64(c.NowMs - c.BookTsMs)

	rawAgeNow := float64(c.NowMs - c.TickTsMs)

	// Экспортируем сырые значения в метрики (ДО КЛАМПИНГА!)
	monitoring.RecordClockDrift(c.Symbol, rawAgeNow)

	// Теперь безопасно обрезаем для бизнес-логики (чтобы стратегии не ломались)
	if c.L2AgeMsTick < 0 {
		c.L2AgeMsTick = 0
	}
	if c.L2AgeMsNow < 0 {
		c.L2AgeMsNow = 0
	}

	c.L2IsStale = int64(c.L2AgeMsTick) > cfg.MaxAgeTickMs
	c.L2IsStaleNow = int64(c.L2AgeMsNow) > cfg.MaxAgeNowMs
}

// NewOrderflowCtx создает новый контекст
func NewOrderflowCtx(symbol string, tickTsMs, bookTsMs int64) *OrderflowCtx {
	return &OrderflowCtx{
		Symbol:   symbol,
		TickTsMs: tickTsMs,
		BookTsMs: bookTsMs,
		NowMs:    time.Now().UnixMilli(),
	}
}
