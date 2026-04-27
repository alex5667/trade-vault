package liquidation

import "testing"

func TestParseBybitAllLiquidation(t *testing.T) {
	raw := []byte(`{"topic":"allLiquidation.BTCUSDT","ts":1710000000100,"data":[{"T":1710000000100,"S":"Buy","s":"BTCUSDT","v":"0.05","p":"49999.5"},{"T":1710000000200,"S":"Sell","s":"BTCUSDT","v":"0.07","p":"50010"}]}`)
	evs, err := ParseBybitAllLiquidation(raw, 1710000000999)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(evs) != 2 {
		t.Fatalf("expected 2 events, got %d", len(evs))
	}
	if evs[0].LiqSide != "long" {
		t.Fatalf("expected liqSide=long, got %q", evs[0].LiqSide)
	}
	if evs[1].LiqSide != "short" {
		t.Fatalf("expected liqSide=short, got %q", evs[1].LiqSide)
	}
}

func TestParseBybitAllLiquidation_IgnoresOpMessages(t *testing.T) {
	raw := []byte(`{"op":"pong"}`)
	evs, err := ParseBybitAllLiquidation(raw, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("expected 0 events, got %d", len(evs))
	}
}
