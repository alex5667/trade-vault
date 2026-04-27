package liquidation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nowMs() int64 { return time.Now().UnixMilli() }

func validEvent(symbol string) NormalizedEvent {
	now := nowMs()
	return NormalizedEvent{
		Source:      "binance_usdm",
		Symbol:      symbol,
		EventTsMs:   now,
		RecvTsMs:    now,
		Price:       "50000.00",
		Qty:         "0.01",
		NotionalUsd: "500.00",
		LiqSide:     "SELL",
		RawSide:     "SELL",
	}
}

func defaultPolicy() DQPolicy {
	return DefaultDQPolicy([]string{"BTCUSDT", "ETHUSDT", "SOLUSDT"})
}

func TestDQValidate_ValidEvent(t *testing.T) {
	p := defaultPolicy()
	ev := validEvent("BTCUSDT")
	ok, reason := p.Validate(ev, nowMs())
	assert.True(t, ok)
	assert.Empty(t, reason)
}

func TestDQValidate_MissingSymbol(t *testing.T) {
	p := defaultPolicy()
	ev := validEvent("BTCUSDT")
	ev.Symbol = ""
	ok, reason := p.Validate(ev, nowMs())
	assert.False(t, ok)
	assert.Equal(t, "missing_symbol", reason)
}

func TestDQValidate_FilteredSymbol(t *testing.T) {
	p := defaultPolicy()
	ev := validEvent("DOGEUSDT") // not in allowlist
	ok, reason := p.Validate(ev, nowMs())
	assert.False(t, ok)
	assert.Equal(t, "filtered_symbol", reason)
}

func TestDQValidate_EmptyAllowlistAcceptsAll(t *testing.T) {
	p := DefaultDQPolicy(nil) // empty allowlist = accept all
	ev := validEvent("RANDOMCOIN")
	ok, _ := p.Validate(ev, nowMs())
	assert.True(t, ok)
}

func TestDQValidate_BadTs(t *testing.T) {
	p := defaultPolicy()
	ev := validEvent("BTCUSDT")
	ev.EventTsMs = 0
	ok, reason := p.Validate(ev, nowMs())
	assert.False(t, ok)
	assert.Equal(t, "bad_ts", reason)
}

func TestDQValidate_BadTsUnit_EpochSeconds(t *testing.T) {
	p := defaultPolicy()
	ev := validEvent("BTCUSDT")
	ev.EventTsMs = 1_700_000_000 // looks like epoch seconds, not ms
	ok, reason := p.Validate(ev, nowMs())
	assert.False(t, ok)
	assert.Equal(t, "bad_ts_unit", reason)
}

func TestDQValidate_MissingPrice(t *testing.T) {
	p := defaultPolicy()
	ev := validEvent("BTCUSDT")
	ev.Price = ""
	ok, reason := p.Validate(ev, nowMs())
	assert.False(t, ok)
	assert.Equal(t, "missing_price", reason)
}

func TestDQValidate_MissingQty(t *testing.T) {
	p := defaultPolicy()
	ev := validEvent("BTCUSDT")
	ev.Qty = "  " // whitespace-only
	ok, reason := p.Validate(ev, nowMs())
	assert.False(t, ok)
	assert.Equal(t, "missing_qty", reason)
}

func TestDQValidate_Stale(t *testing.T) {
	p := defaultPolicy()
	now := nowMs()
	ev := validEvent("BTCUSDT")
	ev.EventTsMs = now - 15_000 // 15s ago, MaxEventAge=10s
	ok, reason := p.Validate(ev, now)
	assert.False(t, ok)
	assert.Equal(t, "stale", reason)
}

func TestDQValidate_FutureSkew(t *testing.T) {
	p := defaultPolicy()
	now := nowMs()
	ev := validEvent("BTCUSDT")
	ev.EventTsMs = now + 5_000 // 5s in the future, MaxFutureSkew=2s
	ok, reason := p.Validate(ev, now)
	assert.False(t, ok)
	assert.Equal(t, "future_skew", reason)
}

func TestDQValidate_OutOfOrder(t *testing.T) {
	p := defaultPolicy()
	now := nowMs()

	ev1 := validEvent("BTCUSDT")
	ev1.EventTsMs = now
	ok1, _ := p.Validate(ev1, now)
	require.True(t, ok1)

	// Out-of-order: 10s older than previous (MaxOutOfOrder=2s)
	ev2 := validEvent("BTCUSDT")
	ev2.EventTsMs = now - 10_000
	ok2, reason2 := p.Validate(ev2, now)
	assert.False(t, ok2)
	assert.Equal(t, "out_of_order", reason2)
}

func TestDQValidate_SmallOOOAllowed(t *testing.T) {
	p := defaultPolicy()
	now := nowMs()

	ev1 := validEvent("ETHUSDT")
	ev1.EventTsMs = now
	ok1, _ := p.Validate(ev1, now)
	require.True(t, ok1)

	// Small OOO: 1s back (MaxOutOfOrder=2s) — should pass
	ev2 := validEvent("ETHUSDT")
	ev2.EventTsMs = now - 1_000
	ok2, _ := p.Validate(ev2, now)
	assert.True(t, ok2, "small out-of-order within tolerance should pass")
}

func TestDQValidate_Dedup(t *testing.T) {
	p := defaultPolicy()
	p.DedupEnabled = true
	p.DedupTTL = 5 * time.Second
	p.DedupMaxKeys = 1000

	now := nowMs()
	ev := validEvent("SOLUSDT")
	ok1, _ := p.Validate(ev, now)
	require.True(t, ok1, "first occurrence should pass")

	ok2, reason2 := p.Validate(ev, now)
	assert.False(t, ok2, "duplicate should be rejected")
	assert.Equal(t, "dedup", reason2)
}

func BenchmarkDQValidate_Valid(b *testing.B) {
	p := defaultPolicy()
	now := nowMs()
	ev := validEvent("BTCUSDT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Validate(ev, now)
	}
}

func BenchmarkDQValidate_WithDedup(b *testing.B) {
	p := defaultPolicy()
	p.DedupEnabled = true
	p.DedupTTL = 5 * time.Second
	p.DedupMaxKeys = 100000
	now := nowMs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := validEvent("BTCUSDT")
		ev.EventTsMs = now + int64(i) // unique ts per iter to avoid dedup
		_, _ = p.Validate(ev, now)
	}
}
