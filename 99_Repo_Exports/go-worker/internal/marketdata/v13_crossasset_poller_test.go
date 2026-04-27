package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Mock Binance + CoinGecko server
// ═══════════════════════════════════════════════════════════════════════════════

func newMockAPIServer(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()

	// /fapi/v1/premiumIndex
	mux.HandleFunc("/fapi/v1/premiumIndex", func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		resp := premiumIndexResp{
			Symbol:          sym,
			LastFundingRate: "0.00012345",
		}
		json.NewEncoder(w).Encode(resp)
	})

	// /fapi/v1/openInterest
	mux.HandleFunc("/fapi/v1/openInterest", func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		resp := openInterestResp{
			Symbol:       sym,
			OpenInterest: "12500.50",
		}
		json.NewEncoder(w).Encode(resp)
	})

	// /futures/data/globalLongShortAccountRatio
	mux.HandleFunc("/futures/data/globalLongShortAccountRatio", func(w http.ResponseWriter, r *http.Request) {
		resp := []longShortRatioResp{
			{
				LongShortRatio: "1.2345",
				LongAccount:    "0.55",
				ShortAccount:   "0.45",
				Timestamp:      time.Now().UnixMilli(),
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	// /fapi/v1/forceOrders
	mux.HandleFunc("/fapi/v1/forceOrders", func(w http.ResponseWriter, r *http.Request) {
		resp := []forceOrderResp{
			{
				Symbol: "BTCUSDT",
				Price:  "66500.00",
				Side:   "SELL",
				Time:   time.Now().UnixMilli() - 60000, // 1 min ago
			},
			{
				Symbol: "BTCUSDT",
				Price:  "68500.00",
				Side:   "BUY",
				Time:   time.Now().UnixMilli() - 120000, // 2 min ago
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	// /fapi/v1/ticker/price
	mux.HandleFunc("/fapi/v1/ticker/price", func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Price string `json:"price"`
		}{Price: "67000.00"}
		json.NewEncoder(w).Encode(resp)
	})

	// /global (CoinGecko)
	mux.HandleFunc("/global", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"market_cap_percentage": map[string]float64{
					"btc": 52.5,
					"eth": 16.8,
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	return httptest.NewServer(mux)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

func TestV13CrossAssetPoller_NewDefaults(t *testing.T) {
	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols: []string{"BTCUSDT"},
	})

	assert.Equal(t, defaultFapiBaseURL, p.cfg.FapiBaseURL)
	assert.Equal(t, defaultCGBaseURL, p.cfg.CGBaseURL)
	assert.Equal(t, defaultV13PollInterval, p.cfg.PollInterval)
	assert.Equal(t, defaultV13RedisTTL, p.cfg.RedisTTL)
	assert.NotNil(t, p.oiState)
}

func TestV13CrossAssetPoller_FetchOIAndFunding(t *testing.T) {
	srv := newMockAPIServer(t)
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:     []string{"BTCUSDT"},
		FapiBaseURL: srv.URL,
	})

	ctx := context.Background()
	fields := make(map[string]interface{})

	p.fetchOIAndFunding(ctx, "BTCUSDT", fields)

	// oi_weighted_funding should be non-zero (0.00012345 * 10000 = 1.2345)
	val, ok := fields["oi_weighted_funding"]
	require.True(t, ok, "oi_weighted_funding should be set")
	assert.NotEqual(t, "0.000000", val)

	// total_market_oi_delta should be set (first call => 0)
	val2, ok2 := fields["total_market_oi_delta"]
	require.True(t, ok2, "total_market_oi_delta should be set")
	assert.Equal(t, "0.000000", val2, "first OI sample → delta should be 0")

	// Second call: delta should still work
	fields2 := make(map[string]interface{})
	p.fetchOIAndFunding(ctx, "BTCUSDT", fields2)
	_, ok3 := fields2["total_market_oi_delta"]
	assert.True(t, ok3)
}

func TestV13CrossAssetPoller_FetchLongShortRatio(t *testing.T) {
	srv := newMockAPIServer(t)
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:     []string{"BTCUSDT"},
		FapiBaseURL: srv.URL,
	})

	ctx := context.Background()
	fields := make(map[string]interface{})

	p.fetchLongShortRatio(ctx, "BTCUSDT", fields)

	val, ok := fields["long_short_ratio"]
	require.True(t, ok)
	assert.True(t, strings.Contains(fmt.Sprintf("%v", val), "1.2345"))
}

func TestV13CrossAssetPoller_FetchLiquidationDistance(t *testing.T) {
	srv := newMockAPIServer(t)
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:          []string{"BTCUSDT"},
		FapiBaseURL:      srv.URL,
		BinanceAPISecret: "test-secret-key",
	})

	ctx := context.Background()
	fields := make(map[string]interface{})

	p.fetchLiquidationDistance(ctx, "BTCUSDT", fields)

	// Closest liquidation: 66500 or 68500 vs current 67000
	// min distance = |66500 - 67000| / 67000 * 10000 = 74.6 bps
	val, ok := fields["liq_heatmap_distance_bps"]
	require.True(t, ok)
	assert.NotEqual(t, "", val)
}

func TestV13CrossAssetPoller_FetchLiquidationDistance_NoSecret(t *testing.T) {
	srv := newMockAPIServer(t)
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:     []string{"BTCUSDT"},
		FapiBaseURL: srv.URL,
		// No BinanceAPISecret → should skip silently
	})

	ctx := context.Background()
	fields := make(map[string]interface{})

	p.fetchLiquidationDistance(ctx, "BTCUSDT", fields)

	_, ok := fields["liq_heatmap_distance_bps"]
	assert.False(t, ok, "liq_heatmap_distance_bps should NOT be set without API secret")
}

func TestV13CrossAssetPoller_PollGlobal_BTCDominance(t *testing.T) {
	srv := newMockAPIServer(t)
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		CGBaseURL: srv.URL,
	})

	ctx := context.Background()

	// First call: seed
	p.pollGlobal(ctx)
	assert.True(t, p.btcDomHasData)
	assert.InDelta(t, 52.5, p.btcDomPrev, 0.1)

	// Momentum should be 0 after first call (no delta yet)
	assert.InDelta(t, 0.0, p.btcDomMomentum, 0.01)

	// Second call: should compute momentum
	p.pollGlobal(ctx)
	// Same value → delta = 0 → momentum stays 0
	assert.InDelta(t, 0.0, p.btcDomMomentum, 0.01)
}

func TestV13CrossAssetPoller_FullPollPerSymbol(t *testing.T) {
	srv := newMockAPIServer(t)
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:          []string{"BTCUSDT"},
		FapiBaseURL:      srv.URL,
		CGBaseURL:        srv.URL,
		BinanceAPISecret: "test-secret-key",
	})

	ctx := context.Background()

	// Seed BTC dominance first
	p.pollGlobal(ctx)

	// No Redis configured — just verifies no panics
	p.pollPerSymbol(ctx)

	// Verify OI state was populated
	p.oiMu.Lock()
	st, ok := p.oiState["BTCUSDT"]
	p.oiMu.Unlock()

	assert.True(t, ok)
	assert.True(t, st.hasOI)
	assert.InDelta(t, 12500.50, st.emaOI, 0.1)
}

func TestV13CrossAssetPoller_HTTPError(t *testing.T) {
	// Server that returns 500 for everything
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:     []string{"BTCUSDT"},
		FapiBaseURL: srv.URL,
		CGBaseURL:   srv.URL,
	})

	ctx := context.Background()

	// Should not panic on HTTP errors
	p.pollPerSymbol(ctx)
	p.pollGlobal(ctx)
}

func TestV13CrossAssetPoller_HTTP418Backoff(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(418)
		// Simulate Binance ban response with epoch millis ~2 seconds from now
		banUntil := time.Now().Add(2 * time.Second).UnixMilli()
		fmt.Fprintf(w, `{"code":-1003,"msg":"Way too many requests; IP banned until %d."}`, banUntil)
	}))
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:           []string{"BTCUSDT"},
		FapiBaseURL:       srv.URL,
		CGBaseURL:         srv.URL,
		InterRequestDelay: 1 * time.Millisecond, // fast for tests
	})

	ctx := context.Background()

	// First poll: should hit the server and get banned
	p.pollPerSymbol(ctx)
	assert.True(t, p.isBanned(), "poller should be banned after 418")
	firstCallCount := callCount

	// Second poll: should be skipped entirely (banned)
	p.pollPerSymbol(ctx)
	assert.Equal(t, firstCallCount, callCount, "no new HTTP calls should be made while banned")

	// Clear ban and verify polling resumes
	p.bannedUntil = 0
	assert.False(t, p.isBanned(), "poller should not be banned after clearing")
}

func TestV13CrossAssetPoller_HTTP429Backoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"code":-1003,"msg":"Too many requests."}`)
	}))
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:           []string{"BTCUSDT"},
		FapiBaseURL:       srv.URL,
		CGBaseURL:         srv.URL,
		InterRequestDelay: 1 * time.Millisecond,
	})

	ctx := context.Background()

	p.pollPerSymbol(ctx)
	assert.True(t, p.isBanned(), "poller should be banned after 429")

	// BannedUntil should be ~5 seconds from now (from Retry-After header)
	banTime := time.UnixMilli(p.bannedUntil)
	assert.WithinDuration(t, time.Now().Add(5*time.Second), banTime, 2*time.Second)
}

func TestV13CrossAssetPoller_BanTimestampParsing(t *testing.T) {
	// Test parsing "banned until 1773808079909" from error body
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(418)
		fmt.Fprint(w, `{"code":-1003,"msg":"Way too many requests; IP(1.2.3.4) banned until 1773808079909. Please use the websocket."}`)
	}))
	defer srv.Close()

	p := NewV13CrossAssetPoller(nil, V13CrossAssetPollerConfig{
		Symbols:           []string{"BTCUSDT"},
		FapiBaseURL:       srv.URL,
		CGBaseURL:         srv.URL,
		InterRequestDelay: 1 * time.Millisecond,
	})

	ctx := context.Background()
	p.pollPerSymbol(ctx)

	// The timestamp is from the real Binance error, so it may be in the past.
	// We only verify that the exact epoch was parsed correctly.
	assert.Equal(t, int64(1773808079909), p.bannedUntil,
		"should parse exact Binance ban timestamp from error body")
}
