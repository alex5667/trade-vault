// Package crossasset computes four v12_of cross-asset and book-dynamics metrics
// and publishes them to Redis Hash runtime:crossasset:{SYMBOL}.
//
// Metrics:
//
//	perp_spot_basis_bps     – (perp_price - spot_price) / spot_price * 10_000
//	eth_btc_corr_5m         – rolling 5-min Pearson correlation of symbol vs BTC/ETH returns
//	stable_coin_flow_delta  – Δ(USDT+USDC market-cap dominance) over a rolling window (publisher-side proxy)
//	depth_migration_bps_ema – EMA of |best_bid migration velocity| in bps/tick
//
// All metrics fail-open: if source data is unavailable the Redis field is not
// updated, and the Python worker falls back to 0.0 via getattr().
//
// Redis layout:
//
//	HSET runtime:crossasset:{SYMBOL}  \
//	   perp_spot_basis_bps     <float> \
//	   eth_btc_corr_5m         <float> \
//	   stable_coin_flow_delta  <float> \
//	   depth_migration_bps_ema <float>
//
// TTL is refreshed on every write (default 300 s so stale data auto-expires).
package crossasset

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// hashTTL controls how long the Redis hash lives without a refresh.
	hashTTL = 300 * time.Second

	// corrWindow is the number of returns kept for rolling Pearson correlation.
	corrWindow = 300 // ~5 min at 1-s resolution

	// emaAlpha for depth_migration_bps_ema (λ ≈ 2/(N+1) for N=20 ticks).
	depthMigAlpha = 0.095

	// stableCoinAlpha for the stable-coin dominance EMA.
	stableCoinAlpha = 0.20

	// keyPrefix for all runtime hashes.
	keyPrefix = "runtime:crossasset:"
)

// --------- per-symbol state -------------------------------------------------

// symbolState holds per-symbol rolling state.
type symbolState struct {
	mu sync.Mutex

	// depth migration
	prevBestBidPx float64
	prevMigTs     int64
	migEMA        float64

	// return series for Pearson correlation (ring buffer)
	returnsBuf  [corrWindow]float64
	returnsBufN int // how many valid entries
	returnHead  int // ring-buffer write pointer

	prevPx float64
	prevTs int64
}

// pushReturn appends a new log-return to the ring buffer.
func (s *symbolState) pushReturn(logRet float64) {
	s.returnsBuf[s.returnHead] = logRet
	s.returnHead = (s.returnHead + 1) % corrWindow
	if s.returnsBufN < corrWindow {
		s.returnsBufN++
	}
}

// returns copies the valid portion of the ring buffer (most recent N).
func (s *symbolState) returns() []float64 {
	n := s.returnsBufN
	out := make([]float64, n)
	if n == corrWindow {
		// full ring – read from head (oldest) to head-1 (newest)
		for i := 0; i < n; i++ {
			out[i] = s.returnsBuf[(s.returnHead+i)%corrWindow]
		}
	} else {
		// not yet full – linear from index 0 to returnHead-1
		copy(out, s.returnsBuf[:n])
	}
	return out
}

// --------- Tracker ----------------------------------------------------------

// Tracker is the main entry point.  Create one per go-worker instance and call
// OnTick / OnBook from the stream controller's processMessage hook.
type Tracker struct {
	rdb *redis.Client

	mu      sync.RWMutex
	symbols map[string]*symbolState

	// BTC and ETH return series (shared across all symbols for correlation).
	btcState *symbolState
	ethState *symbolState

	// stable-coin dominance EMA (global, updated externally via UpdateStableCoin).
	scMu      sync.Mutex
	scPrev    float64 // previous combined dominance value
	scDelta   float64 // current EMA of Δ-dominance
	scHasData bool
}

// New creates a Tracker that publishes to the given Redis client.
func New(rdb *redis.Client) *Tracker {
	return &Tracker{
		rdb:      rdb,
		symbols:  make(map[string]*symbolState),
		btcState: &symbolState{},
		ethState: &symbolState{},
	}
}

func (t *Tracker) getOrCreate(symbol string) *symbolState {
	t.mu.RLock()
	s, ok := t.symbols[symbol]
	t.mu.RUnlock()
	if ok {
		return s
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok = t.symbols[symbol]; ok {
		return s
	}
	s = &symbolState{}
	t.symbols[symbol] = s
	return s
}

// ---------------------------------------------------------------------------
// OnTick is called for every NormalizedTick event.
//
// It updates the per-symbol log-return ring buffer used for eth_btc_corr_5m
// and triggers the Redis publish.
// ---------------------------------------------------------------------------
func (t *Tracker) OnTick(ctx context.Context, symbol string, price float64, tsMs int64) {
	sym := strings.ToUpper(symbol)
	s := t.getOrCreate(sym)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update log-return for this symbol.
	if s.prevPx > 0 && price > 0 && tsMs > s.prevTs {
		logRet := math.Log(price / s.prevPx)
		s.pushReturn(logRet)
	}
	s.prevPx = price
	s.prevTs = tsMs

	// Also update BTC/ETH reference series.
	isBTC := sym == "BTCUSDT"
	isETH := sym == "ETHUSDT"
	if isBTC || isETH {
		ref := t.btcState
		if isETH {
			ref = t.ethState
		}
		ref.mu.Lock()
		if ref.prevPx > 0 && price > 0 && tsMs > ref.prevTs {
			ref.pushReturn(math.Log(price / ref.prevPx))
		}
		ref.prevPx = price
		ref.prevTs = tsMs
		ref.mu.Unlock()
	}

	// Compute correlation and publish (best-effort, non-blocking).
	corr := t.computeCorr(sym, s)
	t.publishAsync(ctx, sym, map[string]any{
		"eth_btc_corr_5m": fmt.Sprintf("%.6f", corr),
	})
}

// ---------------------------------------------------------------------------
// OnBook is called for every NormalizedDepth event.
//
// It computes:
//   - depth_migration_bps_ema (best bid velocity)
//   - perp_spot_basis_bps (if spot price available via Redis SET runtime:spot:{SYMBOL})
//
// ---------------------------------------------------------------------------
func (t *Tracker) OnBook(ctx context.Context, symbol string, bestBidPx float64, tsMs int64) {
	sym := strings.ToUpper(symbol)
	s := t.getOrCreate(sym)

	s.mu.Lock()
	defer s.mu.Unlock()

	extra := make(map[string]any, 2)

	// depth_migration_bps_ema
	if s.prevBestBidPx > 0 && bestBidPx > 0 && tsMs > s.prevMigTs && s.prevMigTs > 0 {
		migBps := (bestBidPx - s.prevBestBidPx) / s.prevBestBidPx * 10_000.0
		absMig := math.Abs(migBps)
		if s.migEMA == 0 {
			s.migEMA = absMig
		} else {
			s.migEMA = depthMigAlpha*absMig + (1-depthMigAlpha)*s.migEMA
		}
	}
	s.prevBestBidPx = bestBidPx
	s.prevMigTs = tsMs
	extra["depth_migration_bps_ema"] = fmt.Sprintf("%.6f", s.migEMA)

	// perp_spot_basis_bps: read spot price from Redis (written by REST fetcher or external feed).
	// Key convention: runtime:spot:{SYMBOL}  (plain string in USD)
	if t.rdb != nil {
		spotKey := "runtime:spot:" + sym
		spotStr, err := t.rdb.Get(ctx, spotKey).Result()
		if err == nil {
			if spotPx, e2 := strconv.ParseFloat(strings.TrimSpace(spotStr), 64); e2 == nil && spotPx > 0 && bestBidPx > 0 {
				basis := (bestBidPx - spotPx) / spotPx * 10_000.0
				extra["perp_spot_basis_bps"] = fmt.Sprintf("%.4f", basis)
			}
		}
	}
	// If spot unavailable, we skip the key — Python worker keeps its last value or 0.0.

	t.publishAsync(ctx, sym, extra)
}

// ---------------------------------------------------------------------------
// UpdateStableCoin is called externally (e.g. from a periodic REST fetcher)
// with the combined USDT+USDC market-cap dominance in percent (0-100).
// It updates a global EMA of the delta and pushes it to every tracked symbol.
// ---------------------------------------------------------------------------
func (t *Tracker) UpdateStableCoin(ctx context.Context, combinedDominance float64) {
	t.scMu.Lock()
	defer t.scMu.Unlock()

	if !t.scHasData {
		t.scPrev = combinedDominance
		t.scHasData = true
		return
	}

	delta := combinedDominance - t.scPrev
	t.scPrev = combinedDominance
	t.scDelta = stableCoinAlpha*delta + (1-stableCoinAlpha)*t.scDelta

	val := fmt.Sprintf("%.6f", t.scDelta)

	// Broadcast to all tracked symbols.
	t.mu.RLock()
	syms := make([]string, 0, len(t.symbols))
	for sym := range t.symbols {
		syms = append(syms, sym)
	}
	t.mu.RUnlock()

	for _, sym := range syms {
		t.publishAsync(ctx, sym, map[string]any{"stable_coin_flow_delta": val})
	}
}

// ---------------------------------------------------------------------------
// computeCorr returns rolling Pearson correlation of `sym` vs BTC returns.
// Falls back to ETH/BTC pair if sym is ETHUSDT.
// Returns 0.0 if insufficient data.
// ---------------------------------------------------------------------------
func (t *Tracker) computeCorr(sym string, s *symbolState) float64 {
	var refState *symbolState
	if sym == "BTCUSDT" {
		// BTC vs ETH (reversed)
		refState = t.ethState
	} else {
		refState = t.btcState
	}

	refState.mu.Lock()
	refReturns := refState.returns()
	refState.mu.Unlock()

	symReturns := s.returns()

	n := len(symReturns)
	if len(refReturns) < n {
		n = len(refReturns)
	}
	if n < 30 {
		return 0.0
	}

	// Align to most recently appended N values.
	symR := symReturns[len(symReturns)-n:]
	refR := refReturns[len(refReturns)-n:]
	return pearson(symR, refR)
}

// pearson computes Pearson r for two equal-length slices. Returns 0 on degenerate input.
func pearson(x, y []float64) float64 {
	n := len(x)
	if n == 0 {
		return 0.0
	}
	var sumX, sumY, sumXY, sumX2, sumY2 float64
	for i := 0; i < n; i++ {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
	}
	fn := float64(n)
	num := fn*sumXY - sumX*sumY
	den := math.Sqrt((fn*sumX2 - sumX*sumX) * (fn*sumY2 - sumY*sumY))
	if den < 1e-12 {
		return 0.0
	}
	r := num / den
	// clamp to [-1, 1] to handle floating-point drift
	if r > 1.0 {
		r = 1.0
	} else if r < -1.0 {
		r = -1.0
	}
	return r
}

// ---------------------------------------------------------------------------
// publishAsync fires an HSET + EXPIRE in a background goroutine.
// Fail-silent: errors are discarded (Python side is fail-open).
// ---------------------------------------------------------------------------
func (t *Tracker) publishAsync(ctx context.Context, symbol string, fields map[string]any) {
	if t.rdb == nil || len(fields) == 0 {
		return
	}
	key := keyPrefix + symbol

	// Convert map[string]any to a flat []interface{} for HSET.
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}

	go func() {
		writeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// HSET key field1 val1 field2 val2 ...
		if err := t.rdb.HSet(writeCtx, key, args...).Err(); err != nil {
			return // fail-silent
		}
		// Refresh TTL so stale keys auto-expire.
		_ = t.rdb.Expire(writeCtx, key, hashTTL).Err()
	}()
}
