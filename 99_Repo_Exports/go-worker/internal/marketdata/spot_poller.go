// Package marketdata provides periodic REST-based market data fetchers
// that feed cross-asset enrichment in the go-worker pipeline.
//
// SpotPoller fetches Binance spot ticker prices for a list of symbols and
// publishes them to Redis as plain strings under:
//
//	runtime:spot:{SYMBOL}   (e.g. "67420.50")
//	TTL: configurable, default 60 s
//
// The intent is to supply the denominator for perp_spot_basis_bps computed
// by crossasset.Tracker.  Symbols without a perp/spot pair produce no value.
package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

const (
	defaultSpotBaseURL  = "https://api.binance.com"
	defaultSpotInterval = 10 * time.Second
	defaultSpotTTL      = 60 * time.Second
	// defaultHTTPTimeout is the per-attempt deadline for the Binance batch call.
	// A 25-symbol batch typically completes in <1 s under normal conditions but
	// can take 3-8 s during exchange maintenance windows or network degradation.
	// 15 s gives headroom while still failing fast relative to the 10 s poll interval.
	defaultHTTPTimeout = 15 * time.Second
	maxSpotRetries     = 2 // total extra attempts after the first failure
	spotRetryBaseDelay = 500 * time.Millisecond
	spotLogEveryN      = 5 // suppress repeated failure logs — log every Nth failure
)

// futuresOnlyAssets lists base assets that trade on Binance USDM futures but
// have no corresponding spot pair on Binance spot (api.binance.com).
// Sending these to /api/v3/ticker/price causes HTTP 400 -1121 Invalid symbol.
var futuresOnlyAssets = map[string]bool{
	"XAU":    true, // Gold perp (XAUUSDT) — futures-only
	"XAG":    true, // Silver perp (XAGUSDT) — futures-only
	"NATGAS": true, // Natural Gas (NATGASUSDT) — futures-only
	"DEFI":   true, // DeFi index — futures-only
	"BVOL":   true, // Volatility token — futures-only
	"IBVOL":  true, // Inverse volatility — futures-only
}

// futuresSymToSpotSym normalises a Binance USDM perp symbol to its spot
// equivalent, returning ("", false) for symbols that have no spot market.
//
// Rules applied in order:
//  1. Strip the "1000" multiplier prefix used for low-unit assets
//     (1000PEPEUSDT → PEPEUSDT, 1000SHIBUSDT → SHIBUSDT, etc.)
//  2. Drop base assets listed in futuresOnlyAssets.
func futuresSymToSpotSym(sym string) (string, bool) {
	sym = strings.ToUpper(sym)

	// Strip 1000/10000 multiplier prefix (Binance notation for milli-priced assets).
	for _, prefix := range []string{"10000", "1000"} {
		if strings.HasPrefix(sym, prefix) {
			sym = strings.TrimPrefix(sym, prefix)
			break
		}
	}

	// Extract base asset (everything before "USDT" / "BUSD" quote suffix).
	var base string
	switch {
	case strings.HasSuffix(sym, "USDT"):
		base = strings.TrimSuffix(sym, "USDT")
	case strings.HasSuffix(sym, "BUSD"):
		base = strings.TrimSuffix(sym, "BUSD")
	default:
		// Unknown quote — skip.
		return "", false
	}

	if futuresOnlyAssets[base] {
		return "", false
	}
	return sym, true
}

// buildSpotSymbols converts a futures-symbol list to the de-duplicated set of
// valid Binance spot symbols, excluding both hardcoded futures-only assets
// and any caller-provided skip-list.
func buildSpotSymbols(futuresSymbols []string, skipMap map[string]bool) []string {
	seen := make(map[string]bool, len(futuresSymbols))
	out := make([]string, 0, len(futuresSymbols))
	for _, s := range futuresSymbols {
		if skipMap != nil && skipMap[strings.ToUpper(s)] {
			continue
		}
		spotSym, ok := futuresSymToSpotSym(s)
		if !ok || seen[spotSym] {
			continue
		}
		seen[spotSym] = true
		out = append(out, spotSym)
	}
	return out
}

// SpotPollerConfig configures SpotPoller.
type SpotPollerConfig struct {
	// Symbols to poll (USDT-margined perp symbols, e.g. "BTCUSDT").
	// Spot price is fetched under the same symbol on Binance spot.
	Symbols []string

	// BaseURL for Binance spot API (default: https://api.binance.com).
	BaseURL string

	// Interval between polls (default: 10s).
	Interval time.Duration

	// RedisTTL is the Redis key TTL (default: 60s).
	RedisTTL time.Duration

	// HTTPTimeout is the per-attempt HTTP deadline (default: 15s).
	// Must be < Interval to avoid overlapping polls.
	HTTPTimeout time.Duration

	// SkipSymbols is an optional map of symbols to skip (USDT-margined perp symbols).
	SkipSymbols map[string]bool

	Logger *zap.SugaredLogger
}

// SpotPoller polls Binance spot ticker prices and writes them to Redis.
type SpotPoller struct {
	cfg         SpotPollerConfig
	spotSymbols []string // normalised spot symbols derived from cfg.Symbols
	rdb         *redis.Client
	client      *http.Client
	stopCh      chan struct{}
	wg          sync.WaitGroup
	consecFails int // consecutive poll failures — used to throttle log noise
}

// NewSpotPoller creates a new SpotPoller.
func NewSpotPoller(rdb *redis.Client, cfg SpotPollerConfig) *SpotPoller {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultSpotBaseURL
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultSpotInterval
	}
	if cfg.RedisTTL <= 0 {
		cfg.RedisTTL = defaultSpotTTL
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = defaultHTTPTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.S()
	}

	spotSyms := buildSpotSymbols(cfg.Symbols, cfg.SkipSymbols)
	cfg.Logger.Infof("spot-poller: %d futures symbols → %d valid spot symbols (skipping %d), http_timeout=%s",
		len(cfg.Symbols), len(spotSyms), len(cfg.Symbols)-len(spotSyms), cfg.HTTPTimeout)

	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DisableKeepAlives:   true, // Prevents "context deadline exceeded" on stale idle connections
		TLSHandshakeTimeout: 5 * time.Second,
	}

	return &SpotPoller{
		cfg:         cfg,
		spotSymbols: spotSyms,
		rdb:         rdb,
		// http.Client.Timeout is the single authoritative deadline per attempt.
		// Do NOT additionally wrap the request in context.WithTimeout — that
		// creates a double-timeout where whichever fires first wins, effectively
		// halving the useful budget under load.
		client: &http.Client{
			Timeout:   cfg.HTTPTimeout,
			Transport: tr,
		},
		stopCh: make(chan struct{}),
	}
}

// Start begins polling in a background goroutine. Call Stop to terminate.
func (p *SpotPoller) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// Immediate first fetch.
		p.poll(ctx)
		ticker := time.NewTicker(p.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.poll(ctx)
			case <-p.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop gracefully shuts down the poller.
func (p *SpotPoller) Stop() {
	close(p.stopCh)
	p.wg.Wait()
}

// binanceTickerPrice is the JSON shape returned by /api/v3/ticker/price.
type binanceTickerPrice struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

// poll fetches all symbols in chunked batch requests using the multi-symbol endpoint.
// On transient HTTP errors it retries up to maxSpotRetries times with exponential
// back-off before giving up for this interval.
func (p *SpotPoller) poll(ctx context.Context) {
	if len(p.spotSymbols) == 0 {
		return
	}

	const chunkSize = 15
	var allTickers []binanceTickerPrice

	for i := 0; i < len(p.spotSymbols); i += chunkSize {
		end := i + chunkSize
		if end > len(p.spotSymbols) {
			end = len(p.spotSymbols)
		}
		chunk := p.spotSymbols[i:end]

		// Build JSON array ["BTCUSDT","ETHUSDT",...] and URL-encode it.
		// Binance /api/v3/ticker/price requires the `symbols` parameter to be a
		// percent-encoded JSON array; sending raw brackets/quotes causes HTTP 400.
		quoted := make([]string, len(chunk))
		for j, s := range chunk {
			quoted[j] = fmt.Sprintf(`"%s"`, s)
		}
		symbolsJSON := "[" + strings.Join(quoted, ",") + "]"
		endpoint := fmt.Sprintf("%s/api/v3/ticker/price?symbols=%s",
			p.cfg.BaseURL, url.QueryEscape(symbolsJSON))

		tickers, err := p.fetchWithRetry(ctx, endpoint)
		if err != nil {
			p.consecFails++
			// Log every failure for the first spotLogEveryN, then every Nth to avoid spam.
			if p.consecFails <= spotLogEveryN || p.consecFails%spotLogEveryN == 0 {
				p.cfg.Logger.Errorf("⚠️ spot-poller: fetch failed (consec=%d, chunk %d-%d): %v", p.consecFails, i, end, err)
			}
			return // Fail fast to avoid partial data consistency issues
		}
		allTickers = append(allTickers, tickers...)
	}

	if p.consecFails > 0 {
		p.cfg.Logger.Errorf("✅ spot-poller: recovered after %d consecutive failures", p.consecFails)
		p.consecFails = 0
	}

	if p.rdb == nil {
		// No Redis configured (test mode or disabled); skip writes.
		return
	}

	writeCtx, writeCancel := context.WithTimeout(ctx, 3*time.Second)
	defer writeCancel()

	pipe := p.rdb.Pipeline()
	count := 0
	for _, t := range allTickers {
		if t.Price == "" {
			continue
		}
		key := "runtime:spot:" + strings.ToUpper(t.Symbol)
		pipe.Set(writeCtx, key, t.Price, p.cfg.RedisTTL)
		count++
	}
	if _, err := pipe.Exec(writeCtx); err != nil {
		p.cfg.Logger.Errorf("⚠️ spot-poller: Redis pipeline error: %v", err)
		return
	}
	p.cfg.Logger.Infof("✅ spot-poller: wrote %d spot prices to Redis (TTL=%s)", count, p.cfg.RedisTTL)
}

// fetchWithRetry performs the HTTP GET with up to maxSpotRetries extra attempts.
// The http.Client.Timeout governs each individual attempt; no extra context
// deadline is added to avoid the double-timeout anti-pattern.
func (p *SpotPoller) fetchWithRetry(ctx context.Context, endpoint string) ([]binanceTickerPrice, error) {
	var lastErr error
	delay := spotRetryBaseDelay
	for attempt := 0; attempt <= maxSpotRetries; attempt++ {
		if attempt > 0 {
			// Back off before retry; bail early if context is cancelled.
			select {
			case <-time.After(delay):
				delay *= 2
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")

		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d HTTP: %w", attempt+1, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			_ = resp.Body.Close()
			// 4xx errors (e.g. invalid symbol -1121) are not transient — don't retry.
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return nil, fmt.Errorf("HTTP %d (non-retriable): %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
			}
			lastErr = fmt.Errorf("attempt %d HTTP %d: %s", attempt+1, resp.StatusCode, strings.TrimSpace(string(errBody)))
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		_ = resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("attempt %d read body: %w", attempt+1, err)
			continue
		}

		var tickers []binanceTickerPrice
		if err := json.Unmarshal(body, &tickers); err != nil {
			// Malformed JSON is unlikely to fix itself on retry.
			return nil, fmt.Errorf("JSON parse: %w", err)
		}
		return tickers, nil
	}
	return nil, lastErr
}
