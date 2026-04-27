package liquidation

import "testing"

func TestParseBinanceForceOrder(t *testing.T) {
	// минимальный пример, приближенный к реальному payload (часть полей опущена).
	raw := []byte(`{"e":"forceOrder","E":1710000000123,"o":{"s":"BTCUSDT","S":"SELL","p":"50000","ap":"50010","q":"0.10","z":"0.08","T":1710000000456}}`)
	ev, err := ParseBinanceForceOrder(raw, 1710000000999)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ev.Symbol != "BTCUSDT" {
		t.Fatalf("bad symbol: %q", ev.Symbol)
	}
	if ev.LiqSide != "long" {
		t.Fatalf("expected liqSide=long, got %q", ev.LiqSide)
	}
	if ev.Price != "50010" {
		t.Fatalf("expected avg price, got %q", ev.Price)
	}
	if ev.Qty != "0.08" {
		t.Fatalf("expected filled qty, got %q", ev.Qty)
	}
	if ev.EventTsMs != 1710000000456 {
		t.Fatalf("expected trade time, got %d", ev.EventTsMs)
	}
}
