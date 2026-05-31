package calendar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestFMPCalendarMapping(t *testing.T) {
	_ = os.Setenv("FMP_API_KEY", "k")
	defer func() { _ = os.Unsetenv("FMP_API_KEY") }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{
		  "date":"2026-01-02 13:30:00",
		  "country":"US",
		  "event":"NFP",
		  "importance":"High",
		  "forecast":"1.0",
		  "previous":"0.9",
		  "actual":"",
		  "unit":"%"
		}]`))
	}))
	defer srv.Close()

	src := NewFMPCalendar(FMPConfig{
		BaseURL:     srv.URL,
		DedupeTTL:   7 * 24 * time.Hour,
		HTTPTimeout: 2 * time.Second,
	})

	evs, err := src.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("expected 1, got %d", len(evs))
	}

	ev := evs[0]
	if len(ev.UID) != 24 {
		t.Fatalf("bad UID=%q", ev.UID)
	}
	if ev.Importance != 3 {
		t.Fatalf("importance expected 3, got %d", ev.Importance)
	}
	if ev.Country != "US" || ev.Currency != "USD" {
		t.Fatalf("bad country/currency: %s/%s", ev.Country, ev.Currency)
	}
	if ev.PayloadJSON == "" {
		t.Fatalf("empty PayloadJSON")
	}
}
