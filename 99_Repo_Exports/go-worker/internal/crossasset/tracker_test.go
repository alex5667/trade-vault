package crossasset

import (
	"context"
	"math"
	"testing"
)

// TestPearson verifies the Pearson correlation helper.
func TestPearson(t *testing.T) {
	// Perfect positive correlation
	x := make([]float64, 30)
	y := make([]float64, 30)
	for i := range x {
		x[i] = float64(i)
		y[i] = float64(i)
	}
	r := pearson(x, y)
	if math.Abs(r-1.0) > 1e-9 {
		t.Errorf("expected r=1.0 for identical series, got %f", r)
	}

	// Perfect negative correlation
	for i := range y {
		y[i] = -float64(i)
	}
	r = pearson(x, y)
	if math.Abs(r+1.0) > 1e-9 {
		t.Errorf("expected r=-1.0 for perfectly negative series, got %f", r)
	}

	// Degenerate (constant x)
	for i := range x {
		x[i] = 5.0
	}
	r = pearson(x, y)
	if r != 0.0 {
		t.Errorf("expected r=0.0 for constant x (degenerate), got %f", r)
	}
}

// TestOnTick_returnRingBuffer verifies the ring buffer accumulates returns.
func TestOnTick_returnRingBuffer(t *testing.T) {
	tracker := New(nil) // nil Redis → publishAsync is a no-op
	ctx := context.Background()

	var price float64 = 100.0
	var ts int64 = 1_000_000
	for i := 0; i < 50; i++ {
		price += 1.0
		ts += 1000
		tracker.OnTick(ctx, "SOLUSDT", price, ts)
	}

	s := tracker.getOrCreate("SOLUSDT")
	s.mu.Lock()
	n := s.returnsBufN
	s.mu.Unlock()

	if n != 49 { // 50 ticks → 49 log-returns (first tick has no prev)
		t.Errorf("expected 49 log-returns, got %d", n)
	}
}

// TestOnBook_depthMigration verifies depth_migration_bps_ema updates.
func TestOnBook_depthMigration(t *testing.T) {
	tracker := New(nil)
	ctx := context.Background()

	// Initial book: bid at 100.0
	tracker.OnBook(ctx, "BTCUSDT", 100.0, 1_000_000)

	s := tracker.getOrCreate("BTCUSDT")
	s.mu.Lock()
	migEMA0 := s.migEMA
	s.mu.Unlock()

	// EMA should be 0 after first call (no previous bid to diff against)
	if migEMA0 != 0.0 {
		t.Errorf("expected migEMA=0 after first book, got %f", migEMA0)
	}

	// Second book: bid moves up 10 bps (100.0 → 100.1)
	tracker.OnBook(ctx, "BTCUSDT", 100.1, 1_100_000)

	s.mu.Lock()
	migEMA1 := s.migEMA
	s.mu.Unlock()

	// bps = (100.1 - 100.0) / 100.0 * 10000 = 10.0 bps
	// EMA is seeded (migEMA was 0) → migEMA1 = absMig = 10.0
	expectedSeed := 10.0
	if math.Abs(migEMA1-expectedSeed) > 1e-9 {
		t.Errorf("expected migEMA=%.6f (seed) after second book, got %.6f", expectedSeed, migEMA1)
	}

	// Third book: bid stays at 100.1 (0 bps change)
	tracker.OnBook(ctx, "BTCUSDT", 100.1, 1_200_000)

	s.mu.Lock()
	migEMA2 := s.migEMA
	s.mu.Unlock()

	// absMig = 0 → EMA decays: migEMA2 = α*0 + (1-α)*10.0 = (1-depthMigAlpha)*10.0
	expectedDecay := (1 - depthMigAlpha) * 10.0
	if math.Abs(migEMA2-expectedDecay) > 1e-9 {
		t.Errorf("expected migEMA=%.6f (decay) after third book, got %.6f", expectedDecay, migEMA2)
	}
}

// TestUpdateStableCoin verifies EMA of stable-coin dominance delta.
func TestUpdateStableCoin(t *testing.T) {
	tracker := New(nil)
	ctx := context.Background()

	// Ensure there's a tracked symbol
	tracker.OnTick(ctx, "BTCUSDT", 50000.0, 1_000_000)

	tracker.UpdateStableCoin(ctx, 10.0) // first call, just sets prev
	tracker.UpdateStableCoin(ctx, 10.5) // Δ = +0.5, EMA = α*0.5

	tracker.scMu.Lock()
	delta := tracker.scDelta
	tracker.scMu.Unlock()

	expected := stableCoinAlpha * 0.5
	if math.Abs(delta-expected) > 1e-9 {
		t.Errorf("expected scDelta=%.6f, got %.6f", expected, delta)
	}
}

// TestPublishAsync_nilRedis ensures no panic when Redis client is nil.
func TestPublishAsync_nilRedis(t *testing.T) {
	tracker := New(nil)
	// Should not panic
	tracker.publishAsync(context.Background(), "BTCUSDT", map[string]any{
		"eth_btc_corr_5m": "0.95",
	})
}
