package orderflow

import (
	"testing"
	"time"
)

func TestComputeStaleness(t *testing.T) {
	cfg := StalenessConfig{
		MaxAgeTickMs: 150,  // 150ms for tick-relative staleness
		MaxAgeNowMs:  1000, // 1sec for now-relative staleness
	}

	nowMs := time.Now().UnixMilli()

	tests := []struct {
		name           string
		tickTsMs       int64
		bookTsMs       int64
		nowMs          int64
		expectStale    bool
		expectStaleNow bool
	}{
		{
			name:           "fresh data",
			tickTsMs:       nowMs,
			bookTsMs:       nowMs - 50, // 50ms ago
			nowMs:          nowMs,
			expectStale:    false,
			expectStaleNow: false,
		},
		{
			name:           "stale relative to tick",
			tickTsMs:       nowMs,
			bookTsMs:       nowMs - 200, // 200ms ago (> 150ms threshold)
			nowMs:          nowMs,
			expectStale:    true,
			expectStaleNow: false,
		},
		{
			name:           "stale relative to now",
			tickTsMs:       nowMs - 500,
			bookTsMs:       nowMs - 1500, // 1.5sec ago (> 1sec threshold)
			nowMs:          nowMs,
			expectStale:    true, // tick-book diff = 1000ms (> 150ms threshold)
			expectStaleNow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &OrderflowCtx{
				Symbol:   "BTCUSDT",
				TickTsMs: tt.tickTsMs,
				BookTsMs: tt.bookTsMs,
				NowMs:    tt.nowMs,
			}

			ctx.ComputeStaleness(cfg)

			if ctx.L2IsStale != tt.expectStale {
				t.Errorf("L2IsStale = %v, want %v", ctx.L2IsStale, tt.expectStale)
			}

			if ctx.L2IsStaleNow != tt.expectStaleNow {
				t.Errorf("L2IsStaleNow = %v, want %v", ctx.L2IsStaleNow, tt.expectStaleNow)
			}
		})
	}
}

func TestNewOrderflowCtx(t *testing.T) {
	symbol := "ETHUSDT"
	tickTsMs := int64(1703123456789)
	bookTsMs := int64(1703123456700)

	ctx := NewOrderflowCtx(symbol, tickTsMs, bookTsMs)

	if ctx.Symbol != symbol {
		t.Errorf("Symbol = %v, want %v", ctx.Symbol, symbol)
	}

	if ctx.TickTsMs != tickTsMs {
		t.Errorf("TickTsMs = %v, want %v", ctx.TickTsMs, tickTsMs)
	}

	if ctx.BookTsMs != bookTsMs {
		t.Errorf("BookTsMs = %v, want %v", ctx.BookTsMs, bookTsMs)
	}

	// NowMs should be set to current time
	if ctx.NowMs <= 0 {
		t.Errorf("NowMs should be set to current time, got %v", ctx.NowMs)
	}
}
