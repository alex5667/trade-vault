package cryptopanic

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestCryptoPanicMappingToNewsRawItem(t *testing.T) {
	os.Setenv("CRYPTOPANIC_AUTH_TOKEN", "x")
	defer os.Unsetenv("CRYPTOPANIC_AUTH_TOKEN")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
		  "results": [{
			"title": "Binance incident update",
			"url": "https://example.com/a",
			"published_at": "2026-01-02T00:00:00Z",
			"currencies": [{"code":"BTC"},{"code":"ETH"}]
		  }]
		}`))
	}))
	defer srv.Close()

	src := New(Config{
		BaseURL:     srv.URL,
		DedupeTTL:   7 * 24 * time.Hour,
		HTTPTimeout: 2 * time.Second,
	})

	items, err := src.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}

	it := items[0]
	if it.UID == "" || len(it.UID) != 24 {
		t.Fatalf("bad UID=%q", it.UID)
	}
	if it.Source != "cryptopanic" {
		t.Fatalf("bad Source=%q", it.Source)
	}
	if it.Title == "" || it.URL == "" {
		t.Fatalf("missing title/url")
	}
	if it.SymbolsJSON != `["BTC","ETH"]` {
		t.Fatalf("bad SymbolsJSON=%q", it.SymbolsJSON)
	}
	if it.PayloadJSON == "" {
		t.Fatalf("empty PayloadJSON")
	}

	// стабильность UID: повторный Fetch с тем же ответом => тот же UID
	items2, _ := src.Fetch(context.Background())
	if items2[0].UID != it.UID {
		t.Fatalf("unstable UID: %s != %s", items2[0].UID, it.UID)
	}
}
