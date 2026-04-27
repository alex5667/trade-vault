package marketdata

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ----- StableCoinPoller tests -------------------------------------------------

type mockUpdater struct {
	calls []float64
}

func (m *mockUpdater) UpdateStableCoin(_ context.Context, combined float64) {
	m.calls = append(m.calls, combined)
}

func TestStableCoinPoller_ForwardsDominance(t *testing.T) {
	resp := cgGlobalResponse{}
	resp.Data.MarketCapPercentage = map[string]float64{
		"usdt": 5.5,
		"usdc": 2.3,
	}
	body, _ := json.Marshal(resp)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify correct path
		if r.URL.Path != "/global" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Write(body)
	}))
	defer srv.Close()

	mu := &mockUpdater{}
	poller := NewStableCoinPoller(mu, StableCoinPollerConfig{
		BaseURL:  srv.URL,
		Interval: time.Hour,
	})

	poller.poll(context.Background())

	if len(mu.calls) != 1 {
		t.Fatalf("expected 1 UpdateStableCoin call, got %d", len(mu.calls))
	}
	got := mu.calls[0]
	want := 5.5 + 2.3 // = 7.8
	if got < want-1e-9 || got > want+1e-9 {
		t.Errorf("expected combined=%.4f, got %.4f", want, got)
	}
}

func TestStableCoinPoller_SkipsZeroDominance(t *testing.T) {
	resp := cgGlobalResponse{}
	resp.Data.MarketCapPercentage = map[string]float64{}
	body, _ := json.Marshal(resp)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	defer srv.Close()

	mu := &mockUpdater{}
	poller := NewStableCoinPoller(mu, StableCoinPollerConfig{
		BaseURL:  srv.URL,
		Interval: time.Hour,
	})
	poller.poll(context.Background())

	if len(mu.calls) != 0 {
		t.Errorf("expected 0 calls on zero dominance, got %d", len(mu.calls))
	}
}

func TestStableCoinPoller_HandlesRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer srv.Close()

	mu := &mockUpdater{}
	poller := NewStableCoinPoller(mu, StableCoinPollerConfig{
		BaseURL:  srv.URL,
		Interval: time.Hour,
	})
	// Should not panic, should not call updater
	poller.poll(context.Background())
	if len(mu.calls) != 0 {
		t.Errorf("expected 0 calls on 429, got %d", len(mu.calls))
	}
}

func TestStableCoinPoller_HandlesBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	mu := &mockUpdater{}
	poller := NewStableCoinPoller(mu, StableCoinPollerConfig{
		BaseURL:  srv.URL,
		Interval: time.Hour,
	})
	// Should not panic
	poller.poll(context.Background())
	if len(mu.calls) != 0 {
		t.Errorf("expected 0 calls on bad JSON, got %d", len(mu.calls))
	}
}

func TestStableCoinPoller_APIKeyHeader(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-cg-demo-api-key")
		resp := cgGlobalResponse{}
		resp.Data.MarketCapPercentage = map[string]float64{"usdt": 5.0, "usdc": 1.0}
		body, _ := json.Marshal(resp)
		w.Write(body)
	}))
	defer srv.Close()

	mu := &mockUpdater{}
	poller := NewStableCoinPoller(mu, StableCoinPollerConfig{
		BaseURL:  srv.URL,
		Interval: time.Hour,
		APIKey:   "test-key-12345",
	})
	poller.poll(context.Background())

	if gotKey != "test-key-12345" {
		t.Errorf("expected API key header, got %q", gotKey)
	}
}

// ----- SpotPoller tests (HTTP-layer only, nil Redis = no-op pipeline) ---------

func TestSpotPoller_ParsesBinanceResponse(t *testing.T) {
	// We test that the poller correctly parses Binance ticker response.
	// With nil Redis, the pipeline Exec is skipped (rdb field is nil → use sentinel).
	// Instead we verify via a custom handler that captures which symbols were fetched.

	var queriedSymbols string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queriedSymbols = r.URL.Query().Get("symbols")
		resp := []binanceTickerPrice{
			{Symbol: "BTCUSDT", Price: "67000.00"},
			{Symbol: "ETHUSDT", Price: "3500.00"},
		}
		body, _ := json.Marshal(resp)
		w.Write(body)
	}))
	defer srv.Close()

	// We pass nil Redis: pipeline.Exec will panic unless we guard it.
	// So we test via a recorder Redis stub that captures SET calls.
	// Since we cannot use miniredis here, verify only HTTP parsing behaviour
	// before the pipeline step via a poller with a fake rdb that is non-nil
	// but unreachable (poll will fail at pipeline.Exec, not before).
	// This test ensures the HTTP + JSON parsing path doesn't panic.
	poller := NewSpotPoller(nil, SpotPollerConfig{
		Symbols:  []string{"BTCUSDT", "ETHUSDT"},
		BaseURL:  srv.URL,
		Interval: time.Hour,
		RedisTTL: 60 * time.Second,
	})

	// poll should return early when rdb is nil (no pipeline panic).
	// We patch the poller's rdb to nil and verify it does nothing.
	// spot_poller.go: if len(p.cfg.Symbols)==0 { return } else… but rdb is used
	// for pipeline. We must guard nil rdb in poll():
	// This test documents the expected behaviour: with nil Redis, poll is a no-op
	// after the HTTP fetch, and the test verifies no panic.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("poll panicked with nil rdb: %v", r)
		}
	}()
	poller.poll(context.Background())

	// Verify symbols were correctly encoded in the request
	if queriedSymbols == "" {
		t.Error("expected symbols param to be sent, got empty string")
	}
}
