package binance

import (
	"testing"
	"time"
)

// TestGetReadTimeoutForTimeframe проверяет корректный таймаут для всех таймфреймов.
// Регрессия P2-1: строки kline_3M / kline_1M ранее сравнивались без strings.ToLower,
// что приводило к дефолтному таймауту для KLINE_3M, kline_3m и т.д.
func TestGetReadTimeoutForTimeframe(t *testing.T) {
	// baseReadTimeout = 300s (дефолт ENV FUTURES_WS_READ_TIMEOUT)
	base := 300 * time.Second

	tests := []struct {
		name      string
		timeframe string
		wantMin   time.Duration // ожидаемый минимум (max(base, X))
	}{
		// ── 1y ─────────────────────────────────────────────────────────────
		{name: "1y canonical", timeframe: "kline_1y", wantMin: 1200 * time.Second},
		{name: "1y uppercase", timeframe: "KLINE_1Y", wantMin: 1200 * time.Second},
		{name: "1y mixed", timeframe: "Kline_1Y", wantMin: 1200 * time.Second},
		{name: "1y short", timeframe: "1y", wantMin: 1200 * time.Second},
		{name: "1y short upper", timeframe: "1Y", wantMin: 1200 * time.Second},

		// ── 3M ─────────────────────────────────────────────────────────────
		// P2-1 regression test + 2026-04 fix: case-sensitivity для месяцев
		{name: "3M canonical", timeframe: "kline_3M", wantMin: 900 * time.Second},
		{name: "3m (minutes!)", timeframe: "kline_3m", wantMin: base},
		{name: "3M uppercase", timeframe: "KLINE_3M", wantMin: 900 * time.Second},
		{name: "3M short", timeframe: "3M", wantMin: 900 * time.Second},
		{name: "3m short (minutes!)", timeframe: "3m", wantMin: base},

		// ── 1M ─────────────────────────────────────────────────────────────
		// P2-1 regression test + 2026-04 fix: case-sensitivity для месяцев
		{name: "1M canonical", timeframe: "kline_1M", wantMin: 600 * time.Second},
		{name: "1m (minutes!)", timeframe: "kline_1m", wantMin: base},
		{name: "1M uppercase", timeframe: "KLINE_1M", wantMin: 600 * time.Second},
		{name: "1M short", timeframe: "1M", wantMin: 600 * time.Second},
		{name: "1m short (minutes!)", timeframe: "1m", wantMin: base},

		// ── 1w ─────────────────────────────────────────────────────────────
		{name: "1w canonical", timeframe: "kline_1w", wantMin: 450 * time.Second},
		{name: "1w uppercase", timeframe: "KLINE_1W", wantMin: 450 * time.Second},
		{name: "1w short", timeframe: "_1w", wantMin: 450 * time.Second},

		// ── 1d ─────────────────────────────────────────────────────────────
		{name: "1d canonical", timeframe: "kline_1d", wantMin: 400 * time.Second},
		{name: "1d uppercase", timeframe: "KLINE_1D", wantMin: 400 * time.Second},
		{name: "1d short", timeframe: "_1d", wantMin: 400 * time.Second},

		// ── дефолт ─────────────────────────────────────────────────────────
		{name: "1h default", timeframe: "kline_1h", wantMin: base},
		{name: "4h default", timeframe: "kline_4h", wantMin: base},
		{name: "15m default", timeframe: "kline_15m", wantMin: base},
		{name: "empty default", timeframe: "", wantMin: base},
		{name: "unknown tf", timeframe: "kline_99z", wantMin: base},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := getReadTimeoutForTimeframe(tc.timeframe)
			if got < tc.wantMin {
				t.Errorf("getReadTimeoutForTimeframe(%q) = %v; want >= %v (P2-1 regression)",
					tc.timeframe, got, tc.wantMin)
			}
		})
	}
}

// TestGetReadTimeoutForTimeframe_OrderPriority проверяет, что 1y > 3M > 1M > 1w > 1d > base.
// Это гарантирует, что долгосрочные таймфреймы не дропаются раньше срока.
func TestGetReadTimeoutForTimeframe_OrderPriority(t *testing.T) {
	t1y := getReadTimeoutForTimeframe("kline_1y")
	t3M := getReadTimeoutForTimeframe("kline_3M")
	t1M := getReadTimeoutForTimeframe("kline_1M")
	t1w := getReadTimeoutForTimeframe("kline_1w")
	t1d := getReadTimeoutForTimeframe("kline_1d")
	t1h := getReadTimeoutForTimeframe("kline_1h")

	cases := []struct {
		name   string
		a, b   time.Duration
		aLabel string
		bLabel string
	}{
		{"1y >= 3M", t1y, t3M, "1y", "3M"},
		{"3M >= 1M", t3M, t1M, "3M", "1M"},
		{"1M >= 1w", t1M, t1w, "1M", "1w"},
		{"1w >= 1d", t1w, t1d, "1w", "1d"},
		{"1d >= 1h", t1d, t1h, "1d", "1h"},
	}

	for _, c := range cases {
		if c.a < c.b {
			t.Errorf("%s: timeout(%s)=%v < timeout(%s)=%v — приоритет нарушен",
				c.name, c.aLabel, c.a, c.bLabel, c.b)
		}
	}
}
