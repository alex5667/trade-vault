package liquidation

import "testing"

func TestDQPolicy_StaleAndOOO(t *testing.T) {
	p := DefaultDQPolicy([]string{"BTCUSDT"})
	now := int64(1_700_000_000_000)

	// ok
	ev1 := NormalizedEvent{Symbol: "BTCUSDT", EventTsMs: now - 1000, Price: "1", Qty: "1"}
	ok, reason := p.Validate(ev1, now)
	if !ok {
		t.Fatalf("expected ok, got %v (%s)", ok, reason)
	}

	// out-of-order beyond window
	ev2 := NormalizedEvent{Symbol: "BTCUSDT", EventTsMs: now - 10_000, Price: "1", Qty: "1"}
	ok, reason = p.Validate(ev2, now)
	if ok {
		t.Fatalf("expected not ok")
	}
	if reason != "out_of_order" && reason != "stale" {
		// depending on MaxEventAge
		t.Fatalf("unexpected reason: %s", reason)
	}

	// filtered symbol
	ev3 := NormalizedEvent{Symbol: "ETHUSDT", EventTsMs: now - 1000, Price: "1", Qty: "1"}
	ok, reason = p.Validate(ev3, now)
	if ok || reason != "filtered_symbol" {
		t.Fatalf("expected filtered_symbol, got ok=%v reason=%s", ok, reason)
	}
}

func TestDQPolicy_BadTsUnitSeconds(t *testing.T) {
	p := DefaultDQPolicy([]string{"BTCUSDT"})
	// epoch seconds (10 digits) — должны уйти в quarantine как bad_ts_unit
	nowMs := int64(1_700_000_000_000)
	ev := NormalizedEvent{Source: "binance_usdm", Symbol: "BTCUSDT", EventTsMs: 1_700_000_000, Price: "1", Qty: "1", RawSide: "BUY"}
	ok, reason := p.Validate(ev, nowMs)
	if ok {
		t.Fatalf("expected not ok")
	}
	if reason != "bad_ts_unit" {
		t.Fatalf("expected bad_ts_unit, got %s", reason)
	}
}

func TestDQPolicy_Dedup(t *testing.T) {
	p := DefaultDQPolicy([]string{"BTCUSDT"})
	// отключаем bad time для теста TTL
	p.MaxEventAge = 0
	p.MaxFutureSkew = 0
	p.MaxOutOfOrder = 0
	p.DedupEnabled = true
	p.DedupTTL = 60_000_000_000 // 60s in ns
	p.DedupMaxKeys = 100
	p.dedup = newDedupCache(p.DedupTTL, p.DedupMaxKeys)

	now := int64(1_000_000_000_000)
	ev := NormalizedEvent{Source: "binance_usdm", Symbol: "BTCUSDT", EventTsMs: now - 1, Price: "123", Qty: "0.5", RawSide: "BUY"}

	ok, reason := p.Validate(ev, now)
	if !ok {
		t.Fatalf("expected ok, got %v (%s)", ok, reason)
	}

	ok, reason = p.Validate(ev, now)
	if ok || reason != "dedup" {
		t.Fatalf("expected dedup, got ok=%v reason=%s", ok, reason)
	}

	// after TTL
	now2 := now + 60_000 + 1
	ok, reason = p.Validate(ev, now2)
	if !ok {
		t.Fatalf("expected ok after ttl, got %v (%s)", ok, reason)
	}
}

