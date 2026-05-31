package fmp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestFMPStockNewsMapping(t *testing.T) {
	os.Setenv("FMP_API_KEY", "k")
	defer os.Unsetenv("FMP_API_KEY")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{
		  "symbol":"SPY",
		  "publishedDate":"2026-01-02 00:00:00",
		  "title":"SPX macro move",
		  "url":"https://example.com/x",
		  "text":"...",
		  "site":"test"
		}]`))
	}))
	defer srv.Close()

	src := NewStockNews(StockNewsConfig{
		BaseURL:     srv.URL,
		DedupeTTL:   7 * 24 * time.Hour,
		HTTPTimeout: 2 * time.Second,
	})

	items, err := src.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1, got %d", len(items))
	}
	it := items[0]
	if len(it.UID) != 24 {
		t.Fatalf("bad UID=%q", it.UID)
	}
	if it.SymbolsJSON != `["SPY"]` {
		t.Fatalf("bad SymbolsJSON=%q", it.SymbolsJSON)
	}
	if it.PayloadJSON == "" {
		t.Fatalf("empty PayloadJSON")
	}
}
