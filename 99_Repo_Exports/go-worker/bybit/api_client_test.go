package bybit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIClient_Fetch24hTickers(t *testing.T) {
	// Fake Bybit server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/tickers" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.URL.Query().Get("category"); got != "linear" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("bad category"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"retCode":0,
			"retMsg":"OK",
			"result":{
				"category":"linear",
				"list":[
					{"symbol":"BTCUSDT","lastPrice":"65000","price24hPcnt":"0.01"},
					{"symbol":"ETHUSDT","lastPrice":"3500","price24hPcnt":"-0.02"}
				]
			},
			"time":1710000000000
		}`))
	}))
	defer srv.Close()

	t.Setenv("BYBIT_BASE_URL", srv.URL)
	client := NewAPIClient()

	data, ts, err := client.Fetch24hTickers(context.Background())
	if err != nil {
		t.Fatalf("Fetch24hTickers error: %v", err)
	}
	if ts != 1710000000000 {
		t.Fatalf("unexpected ts: %d", ts)
	}
	if len(data) != 2 {
		t.Fatalf("expected 2 tickers, got %d", len(data))
	}
	if strings.ToUpper(data[0].Symbol) != "BTCUSDT" {
		t.Fatalf("unexpected symbol: %s", data[0].Symbol)
	}
}

func TestAPIClient_FetchFundingRateLatest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/funding/history" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.URL.Query().Get("category"); got != "linear" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("bad category"))
			return
		}
		if got := r.URL.Query().Get("symbol"); got != "BTCUSDT" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("bad symbol"))
			return
		}
		if got := r.URL.Query().Get("limit"); got != "1" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("bad limit"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"retCode":0,
			"retMsg":"OK",
			"result":{
				"category":"linear",
				"list":[{"symbol":"BTCUSDT","fundingRate":"0.0001","fundingRateTimestamp":"1710000000000"}]
			},
			"time":1710000001234
		}`))
	}))
	defer srv.Close()

	t.Setenv("BYBIT_BASE_URL", srv.URL)
	client := NewAPIClient()

	pt, ts, err := client.FetchFundingRateLatest(context.Background(), "btcusdt")
	if err != nil {
		t.Fatalf("FetchFundingRateLatest error: %v", err)
	}
	if ts != 1710000001234 {
		t.Fatalf("unexpected ts: %d", ts)
	}
	if pt == nil {
		t.Fatalf("expected non-nil funding point")
	}
	if pt.Symbol != "BTCUSDT" {
		t.Fatalf("unexpected symbol: %s", pt.Symbol)
	}
	if pt.FundingRate != "0.0001" {
		t.Fatalf("unexpected funding rate: %s", pt.FundingRate)
	}
}
