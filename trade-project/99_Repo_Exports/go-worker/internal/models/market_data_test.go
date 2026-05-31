package models

import (
	"encoding/json"
	"testing"
)

func TestNormalizedTickPopulateRedisValuesContract(t *testing.T) {
	tick := NormalizedTick{
		Symbol:  "btcusdt",
		Ts:      1700000000000,
		Price:   "50000.5",
		Qty:     "0.01",
		Side:    "BUY",
		Source:  "binance",
		Market:  "USDT-M",
		TradeID: 12345,
	}

	vals := map[string]any{}
	tick.PopulateRedisValues(vals)

	assertEqual(t, vals["symbol"], "BTCUSDT")
	assertEqual(t, vals["ts"], int64(1700000000000))
	assertEqual(t, vals["event_time_ms"], int64(1700000000000))
	assertEqual(t, vals["price"], "50000.5")
	assertEqual(t, vals["qty"], "0.01")
	assertEqual(t, vals["quantity"], "0.01")
	assertEqual(t, vals["side"], "BUY")
	assertEqual(t, vals["source"], "binance")
	assertEqual(t, vals["market"], "USDT-M")
	assertEqual(t, vals["trade_id"], int64(12345))
	assertEqual(t, vals["schema_version"], "1")
	assertEqual(t, vals["v"], 1)
	assertEqual(t, vals["event_id"], "binance:BTCUSDT:1700000000000")
	assertEqual(t, vals["quality_flags"], "ok")
	assertEqual(t, vals["ingest_time_ms"], vals["written_at"])

	if got := vals["trace_id"]; got == nil || len(got.(string)) != 32 {
		t.Fatalf("trace_id length mismatch: %v", got)
	}
}

func TestNormalizedDepthPopulateRedisValuesContract(t *testing.T) {
	depth := NormalizedDepth{
		Symbol:    "ethusdt",
		Ts:        1700000000001,
		FirstID:   10,
		FinalID:   20,
		PrevFinal: 9,
		Bids:      [][]string{{"3500", "1.2"}},
		Asks:      [][]string{{"3501", "0.8"}},
		Source:    "bybit-linear",
		Market:    "USDT-M",
	}

	vals := map[string]any{}
	depth.PopulateRedisValues(vals)

	assertEqual(t, vals["symbol"], "ETHUSDT")
	assertEqual(t, vals["ts"], int64(1700000000001))
	assertEqual(t, vals["event_time_ms"], int64(1700000000001))
	assertEqual(t, vals["first_id"], int64(10))
	assertEqual(t, vals["final_id"], int64(20))
	assertEqual(t, vals["prev_final"], int64(9))
	assertEqual(t, vals["source"], "bybit-linear")
	assertEqual(t, vals["market"], "USDT-M")
	assertEqual(t, vals["schema_version"], "1")
	assertEqual(t, vals["v"], 1)
	assertEqual(t, vals["event_id"], "bybit-linear:ETHUSDT:1700000000001")
	assertEqual(t, vals["quality_flags"], "ok")
	assertEqual(t, vals["ingest_time_ms"], vals["written_at"])

	var bids [][]string
	if err := json.Unmarshal([]byte(vals["bids"].(string)), &bids); err != nil {
		t.Fatalf("bids JSON invalid: %v", err)
	}
	assertEqual(t, bids[0][0], "3500")

	var asks [][]string
	if err := json.Unmarshal([]byte(vals["asks"].(string)), &asks); err != nil {
		t.Fatalf("asks JSON invalid: %v", err)
	}
	assertEqual(t, asks[0][0], "3501")
}

func assertEqual(t *testing.T, got any, want any) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v (%T), want %v (%T)", got, got, want, want)
	}
}
