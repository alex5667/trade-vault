// Package marketdata - V13CrossAssetPoller
//
// Periodically fetches ND-group cross-asset indicators from Binance Futures
// REST API and CoinGecko, then publishes them to Redis Hash:
//
//	HSET runtime:crossasset:v13:{SYMBOL}  \
//	   btc_dominance_momentum     <float> \
//	   oi_weighted_funding        <float> \
//	   total_market_oi_delta      <float> \
//	   liq_heatmap_distance_bps   <float> \
//	   long_short_ratio           <float>
//
// TTL is refreshed on every write (default 300s).
// Fail-open: any HTTP/parse/Redis error is logged and skipped.
package marketdata

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

const (
	defaultFapiBaseURL       = "https://fapi.binance.com"
	defaultV13PollInterval   = 15 * time.Second
	defaultV13GlobalPoll     = 60 * time.Second
	defaultV13HTTPTimeout    = 8 * time.Second
	defaultV13RedisTTL       = 300 * time.Second
	defaultInterRequestDelay = 200 * time.Millisecond
	defaultBanBackoff        = 120 * time.Second // fallback if ban timestamp unparseable
	v13RedisKeyPrefix        = "runtime:crossasset:v13:"
	oiEMAAlpha               = 0.10 // EMA smoothing for OI delta
	btcDomMomentumAlpha      = 0.20 // EMA smoothing for BTC dominance momentum
)

// ─── Config ──────────────────────────────────────────────────────────────────

// V13CrossAssetPollerConfig configures the v13 cross-asset poller.
type V13CrossAssetPollerConfig struct {
	// Symbols to poll (futures symbols, e.g. "BTCUSDT").
	Symbols []string

	// FapiBaseURL for Binance Futures API (default: https://fapi.binance.com).
	FapiBaseURL string

	// CGBaseURL for CoinGecko API (default: https://api.coingecko.com/api/v3).
	CGBaseURL string

	// CGAPIKey for CoinGecko Pro/Demo API key (optional).
	CGAPIKey string

	// BinanceAPIKey for Binance FAPI endpoints that require authentication
	// (e.g. /fapi/v1/forceOrders). Optional; if empty those calls get 401.
	BinanceAPIKey string

	// BinanceAPISecret for signing Binance FAPI requests (HMAC-SHA256).
	// Required for signed endpoints like /fapi/v1/forceOrders.
	BinanceAPISecret string

	// InterRequestDelay between consecutive REST calls within a poll cycle
	// to avoid burst patterns (default: 200ms).
	InterRequestDelay time.Duration

	// PollInterval between per-symbol endpoint polls (default: 15s).
	PollInterval time.Duration

	// GlobalPollInterval between global endpoints (BTC dominance) (default: 60s).
	GlobalPollInterval time.Duration

	// RedisTTL for the Redis hash keys (default: 300s).
	RedisTTL time.Duration

	Logger *zap.SugaredLogger
}

// ─── Per-symbol OI tracking ──────────────────────────────────────────────────

type symbolOIState struct {
	prevOI float64
	emaOI  float64
	hasOI  bool
}

// ─── Poller ──────────────────────────────────────────────────────────────────

// V13CrossAssetPoller periodically fetches cross-asset data for the v13_of
// ND indicator group and writes to Redis.
type V13CrossAssetPoller struct {
	cfg    V13CrossAssetPollerConfig
	rdb    *redis.Client
	client *http.Client
	stopCh chan struct{}
	wg     sync.WaitGroup

	// bannedUntil: Unix-millis timestamp until which Binance has banned us.
	// Accessed atomically. Zero means "not banned".
	bannedUntil int64 // atomic

	// Per-symbol OI state for delta calculation
	oiMu    sync.Mutex
	oiState map[string]*symbolOIState

	// Global: BTC dominance momentum
	btcDomMu       sync.Mutex
	btcDomPrev     float64
	btcDomMomentum float64
	btcDomHasData  bool
}

// NewV13CrossAssetPoller creates a new poller with defaults applied.
func NewV13CrossAssetPoller(rdb *redis.Client, cfg V13CrossAssetPollerConfig) *V13CrossAssetPoller {
	if cfg.FapiBaseURL == "" {
		cfg.FapiBaseURL = defaultFapiBaseURL
	}
	if cfg.CGBaseURL == "" {
		cfg.CGBaseURL = defaultCGBaseURL
	}
	if cfg.InterRequestDelay <= 0 {
		cfg.InterRequestDelay = defaultInterRequestDelay
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultV13PollInterval
	}
	if cfg.GlobalPollInterval <= 0 {
		cfg.GlobalPollInterval = defaultV13GlobalPoll
	}
	if cfg.RedisTTL <= 0 {
		cfg.RedisTTL = defaultV13RedisTTL
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.S()
	}

	return &V13CrossAssetPoller{
		cfg:     cfg,
		rdb:     rdb,
		client:  &http.Client{Timeout: defaultV13HTTPTimeout},
		stopCh:  make(chan struct{}),
		oiState: make(map[string]*symbolOIState),
	}
}

// Start begins polling in background goroutines.
func (p *V13CrossAssetPoller) Start(ctx context.Context) {
	// Per-symbol poller (OI, funding, long/short, liq heatmap)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.pollPerSymbol(ctx) // immediate first
		ticker := time.NewTicker(p.cfg.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.pollPerSymbol(ctx)
			case <-p.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Global poller (BTC dominance)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.pollGlobal(ctx) // immediate first
		ticker := time.NewTicker(p.cfg.GlobalPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.pollGlobal(ctx)
			case <-p.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop gracefully shuts down the poller.
func (p *V13CrossAssetPoller) Stop() {
	close(p.stopCh)
	p.wg.Wait()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Per-Symbol Polling
// ═══════════════════════════════════════════════════════════════════════════════

// isBanned returns true if the IP is currently banned by Binance.
func (p *V13CrossAssetPoller) isBanned() bool {
	until := atomic.LoadInt64(&p.bannedUntil)
	if until == 0 {
		return false
	}
	return time.Now().UnixMilli() < until
}

func (p *V13CrossAssetPoller) pollPerSymbol(ctx context.Context) {
	if p.isBanned() {
		p.cfg.Logger.Infof("⏳ v13-crossasset: skipping poll cycle — IP banned until %s",
			time.UnixMilli(atomic.LoadInt64(&p.bannedUntil)).UTC().Format(time.RFC3339))
		return
	}

	for i, sym := range p.cfg.Symbols {
		sym = strings.ToUpper(sym)

		// Inter-symbol delay to avoid burst (skip for first symbol)
		if i > 0 {
			select {
			case <-time.After(p.cfg.InterRequestDelay):
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			}
		}

		// Abort if we got banned mid-cycle
		if p.isBanned() {
			p.cfg.Logger.Infof("⏳ v13-crossasset: aborting poll cycle mid-symbol — IP banned")
			return
		}

		fields := make(map[string]interface{})

		// 1. OI + premiumIndex (funding + OI combined)
		p.fetchOIAndFunding(ctx, sym, fields)
		p.interRequestSleep(ctx)

		// 2. Long/Short ratio
		p.fetchLongShortRatio(ctx, sym, fields)
		p.interRequestSleep(ctx)

		// 3. Liquidation heatmap distance
		p.fetchLiquidationDistance(ctx, sym, fields)

		// 4. BTC dominance momentum (global, same for all symbols)
		p.btcDomMu.Lock()
		fields["btc_dominance_momentum"] = fmt.Sprintf("%.6f", p.btcDomMomentum)
		p.btcDomMu.Unlock()

		// Write to Redis
		p.publishFields(ctx, sym, fields)
	}
}

// interRequestSleep adds a small delay between REST calls to avoid request bursts.
func (p *V13CrossAssetPoller) interRequestSleep(ctx context.Context) {
	select {
	case <-time.After(p.cfg.InterRequestDelay):
	case <-ctx.Done():
	case <-p.stopCh:
	}
}

// ─── 1. Open Interest + Funding ──────────────────────────────────────────────

// premiumIndexResp is Binance /fapi/v1/premiumIndex response.
type premiumIndexResp struct {
	Symbol          string `json:"symbol"`
	LastFundingRate string `json:"lastFundingRate"`
}

// openInterestResp is Binance /fapi/v1/openInterest response.
type openInterestResp struct {
	Symbol       string `json:"symbol"`
	OpenInterest string `json:"openInterest"`
}

func (p *V13CrossAssetPoller) fetchOIAndFunding(ctx context.Context, sym string, fields map[string]interface{}) {
	// Fetch premiumIndex for funding rate
	fundingRate := 0.0
	{
		endpoint := fmt.Sprintf("%s/fapi/v1/premiumIndex?symbol=%s", p.cfg.FapiBaseURL, sym)
		body, err := p.httpGet(ctx, endpoint)
		if err == nil {
			var resp premiumIndexResp
			if err := json.Unmarshal(body, &resp); err == nil {
				if fr, e := strconv.ParseFloat(resp.LastFundingRate, 64); e == nil {
					fundingRate = fr
				}
			}
		}
	}

	// Fetch open interest
	oi := 0.0
	{
		endpoint := fmt.Sprintf("%s/fapi/v1/openInterest?symbol=%s", p.cfg.FapiBaseURL, sym)
		body, err := p.httpGet(ctx, endpoint)
		if err == nil {
			var resp openInterestResp
			if err := json.Unmarshal(body, &resp); err == nil {
				if val, e := strconv.ParseFloat(resp.OpenInterest, 64); e == nil {
					oi = val
				}
			}
		}
	}

	// oi_weighted_funding = OI × |fundingRate| × sign(fundingRate) × 10000
	// This produces a signed BPS-scaled metric.
	if oi > 0 && fundingRate != 0 {
		oiWeightedFunding := fundingRate * 10000.0 // funding in bps, weighted by presence of OI
		fields["oi_weighted_funding"] = fmt.Sprintf("%.6f", oiWeightedFunding)
	} else {
		fields["oi_weighted_funding"] = "0.000000"
	}

	// total_market_oi_delta = (OI - EMA(OI)) / EMA(OI) × 10000 (bps)
	p.oiMu.Lock()
	defer p.oiMu.Unlock()

	st, ok := p.oiState[sym]
	if !ok {
		st = &symbolOIState{}
		p.oiState[sym] = st
	}

	if oi > 0 {
		if !st.hasOI {
			st.emaOI = oi
			st.prevOI = oi
			st.hasOI = true
			fields["total_market_oi_delta"] = "0.000000"
		} else {
			st.emaOI = oiEMAAlpha*oi + (1-oiEMAAlpha)*st.emaOI
			if st.emaOI > 0 {
				delta := (oi - st.emaOI) / st.emaOI * 10000.0
				fields["total_market_oi_delta"] = fmt.Sprintf("%.4f", delta)
			} else {
				fields["total_market_oi_delta"] = "0.000000"
			}
			st.prevOI = oi
		}
	}
}

// ─── 2. Long/Short Ratio ─────────────────────────────────────────────────────

type longShortRatioResp struct {
	Symbol         string `json:"symbol"`
	LongShortRatio string `json:"longShortRatio"`
	LongAccount    string `json:"longAccount"`
	ShortAccount   string `json:"shortAccount"`
	Timestamp      int64  `json:"timestamp"`
}

func (p *V13CrossAssetPoller) fetchLongShortRatio(ctx context.Context, sym string, fields map[string]interface{}) {
	endpoint := fmt.Sprintf("%s/futures/data/globalLongShortAccountRatio?symbol=%s&period=5m&limit=1",
		p.cfg.FapiBaseURL, sym)

	body, err := p.httpGet(ctx, endpoint)
	if err != nil {
		return
	}

	var resp []longShortRatioResp
	if err := json.Unmarshal(body, &resp); err != nil || len(resp) == 0 {
		return
	}

	if ratio, e := strconv.ParseFloat(resp[0].LongShortRatio, 64); e == nil {
		fields["long_short_ratio"] = fmt.Sprintf("%.6f", ratio)
	}
}

// ─── 3. Liquidation Heatmap Distance ─────────────────────────────────────────

type forceOrderResp struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
	Side   string `json:"side"` // BUY or SELL
	Time   int64  `json:"time"`
}

func (p *V13CrossAssetPoller) fetchLiquidationDistance(ctx context.Context, sym string, fields map[string]interface{}) {
	// /fapi/v1/forceOrders is a signed endpoint — requires timestamp + HMAC-SHA256 signature.
	if p.cfg.BinanceAPISecret == "" {
		// Cannot call signed endpoint without secret; skip silently.
		return
	}

	baseQuery := fmt.Sprintf("symbol=%s&limit=50", sym)
	endpoint, err := p.signBinanceURL(fmt.Sprintf("%s/fapi/v1/forceOrders", p.cfg.FapiBaseURL), baseQuery)
	if err != nil {
		p.cfg.Logger.Errorf("⚠️ v13-crossasset: failed to sign forceOrders request: %v", err)
		return
	}

	body, err := p.httpGet(ctx, endpoint)
	if err != nil {
		return
	}

	var orders []forceOrderResp
	if err := json.Unmarshal(body, &orders); err != nil || len(orders) == 0 {
		return
	}

	// Get current price for distance calculation
	currentPrice := 0.0
	{
		endpoint := fmt.Sprintf("%s/fapi/v1/ticker/price?symbol=%s", p.cfg.FapiBaseURL, sym)
		body, err := p.httpGet(ctx, endpoint)
		if err == nil {
			var ticker struct {
				Price string `json:"price"`
			}
			if err := json.Unmarshal(body, &ticker); err == nil {
				currentPrice, _ = strconv.ParseFloat(ticker.Price, 64)
			}
		}
	}

	if currentPrice <= 0 {
		return
	}

	// Find the closest liquidation level (by price) — simplified heatmap proxy
	// Filter to last 1 hour for relevance
	cutoff := time.Now().UnixMilli() - 3600_000
	closestDistBps := math.MaxFloat64
	for _, o := range orders {
		if o.Time < cutoff {
			continue
		}
		px, e := strconv.ParseFloat(o.Price, 64)
		if e != nil || px <= 0 {
			continue
		}
		distBps := math.Abs(px-currentPrice) / currentPrice * 10000.0
		if distBps < closestDistBps {
			closestDistBps = distBps
		}
	}
	if closestDistBps < math.MaxFloat64 {
		fields["liq_heatmap_distance_bps"] = fmt.Sprintf("%.4f", closestDistBps)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Global Polling (BTC Dominance)
// ═══════════════════════════════════════════════════════════════════════════════

func (p *V13CrossAssetPoller) pollGlobal(ctx context.Context) {
	endpoint := fmt.Sprintf("%s/global", p.cfg.CGBaseURL)
	body, err := p.httpGet(ctx, endpoint)
	if err != nil {
		return
	}

	var resp struct {
		Data struct {
			MarketCapPercentage map[string]float64 `json:"market_cap_percentage"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		p.cfg.Logger.Errorf("⚠️ v13-crossasset: CoinGecko global parse error: %v", err)
		return
	}

	btcDom, ok := resp.Data.MarketCapPercentage["btc"]
	if !ok || btcDom <= 0 {
		return
	}

	p.btcDomMu.Lock()
	defer p.btcDomMu.Unlock()

	if !p.btcDomHasData {
		p.btcDomPrev = btcDom
		p.btcDomHasData = true
		return
	}

	// Momentum = EMA of Δ(btc_dominance)
	delta := btcDom - p.btcDomPrev
	p.btcDomPrev = btcDom
	p.btcDomMomentum = btcDomMomentumAlpha*delta + (1-btcDomMomentumAlpha)*p.btcDomMomentum
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

// signBinanceURL appends timestamp and HMAC-SHA256 signature to the query string.
// baseURL is the endpoint without query string, queryParams is the existing query string.
// Returns the full signed URL ready for GET.
func (p *V13CrossAssetPoller) signBinanceURL(baseURL, queryParams string) (string, error) {
	timestamp := fmt.Sprintf("%d", time.Now().UnixMilli())
	if queryParams != "" {
		queryParams += "&"
	}
	queryParams += "timestamp=" + timestamp

	mac := hmac.New(sha256.New, []byte(p.cfg.BinanceAPISecret))
	mac.Write([]byte(queryParams))
	sig := hex.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("%s?%s&signature=%s", baseURL, queryParams, sig), nil
}

func (p *V13CrossAssetPoller) httpGet(ctx context.Context, url string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, defaultV13HTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	// CoinGecko API key support
	if p.cfg.CGAPIKey != "" && strings.Contains(url, "coingecko") {
		req.Header.Set("x-cg-demo-api-key", p.cfg.CGAPIKey)
	}

	// Binance FAPI key support (required for /fapi/v1/forceOrders etc.)
	if p.cfg.BinanceAPIKey != "" && strings.Contains(url, p.cfg.FapiBaseURL) {
		req.Header.Set("X-MBX-APIKEY", p.cfg.BinanceAPIKey)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		errStr := strings.TrimSpace(string(errBody))
		p.cfg.Logger.Errorf("⚠️ v13-crossasset: HTTP %d from %s: %s",
			resp.StatusCode, url, errStr)

		// Handle rate-limit (429) and IP ban (418) with backoff
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
			p.handleRateLimitBan(errStr, resp)
		}

		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return io.ReadAll(io.LimitReader(resp.Body, 128*1024))
}

// handleRateLimitBan sets the bannedUntil timestamp based on the Binance error response.
// Binance error format: "banned until 1773808079909" (epoch millis).
// Fallback: Retry-After header (seconds), or defaultBanBackoff.
func (p *V13CrossAssetPoller) handleRateLimitBan(errBody string, resp *http.Response) {
	var banUntilMs int64

	// Try to parse "banned until <epoch_ms>" from error body
	if idx := strings.Index(errBody, "banned until "); idx >= 0 {
		numStr := errBody[idx+len("banned until "):]
		// Trim everything after the number (could be '.', '"', space, etc.)
		for i, c := range numStr {
			if c < '0' || c > '9' {
				numStr = numStr[:i]
				break
			}
		}
		if ts, err := strconv.ParseInt(numStr, 10, 64); err == nil && ts > 0 {
			banUntilMs = ts
		}
	}

	// Fallback: Retry-After header (seconds from now)
	if banUntilMs == 0 {
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				banUntilMs = time.Now().Add(time.Duration(secs) * time.Second).UnixMilli()
			}
		}
	}

	// Last resort fallback
	if banUntilMs == 0 {
		banUntilMs = time.Now().Add(defaultBanBackoff).UnixMilli()
	}

	atomic.StoreInt64(&p.bannedUntil, banUntilMs)
	banTime := time.UnixMilli(banUntilMs)
	p.cfg.Logger.Infof("🚫 v13-crossasset: IP banned! Backing off until %s (%.0fs from now)",
		banTime.UTC().Format(time.RFC3339), time.Until(banTime).Seconds())
}

func (p *V13CrossAssetPoller) publishFields(ctx context.Context, symbol string, fields map[string]interface{}) {
	if p.rdb == nil || len(fields) == 0 {
		return
	}

	key := v13RedisKeyPrefix + symbol
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}

	go func() {
		writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.rdb.HSet(writeCtx, key, args...).Err(); err != nil {
			p.cfg.Logger.Errorf("⚠️ v13-crossasset: Redis HSET %s error: %v", key, err)
			return
		}
		_ = p.rdb.Expire(writeCtx, key, p.cfg.RedisTTL).Err()
	}()
}
