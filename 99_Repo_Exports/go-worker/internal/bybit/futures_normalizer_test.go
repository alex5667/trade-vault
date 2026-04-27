package bybit

import "testing"

func TestParsePublicTrade_OK(t *testing.T) {
	raw := []byte(`{
		"topic": "publicTrade.BTCUSDT",
		"type": "snapshot",
		"ts": 1672304486868,
		"data": [
			{
				"T": 1672304486865,
				"s": "BTCUSDT",
				"S": "Buy",
				"v": "0.001",
				"p": "16578.50",
				"i": "20f43950-d8dd-5b31-9112-a178eb6023af",
				"seq": 1783284617
			}
		]
	}`)

	ticks, err := ParsePublicTrade(raw)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("expected 1 tick, got %d", len(ticks))
	}
	tk := ticks[0]
	if tk.Symbol != "BTCUSDT" {
		t.Fatalf("symbol mismatch: %s", tk.Symbol)
	}
	if tk.Side != "BUY" {
		t.Fatalf("side mismatch: %s", tk.Side)
	}
	if tk.Price != "16578.50" || tk.Qty != "0.001" {
		t.Fatalf("price/qty mismatch: %s/%s", tk.Price, tk.Qty)
	}
	if tk.Ts != 1672304486865 {
		t.Fatalf("ts mismatch: %d", tk.Ts)
	}
	if tk.TradeID == 0 {
		t.Fatalf("expected non-zero TradeID hash")
	}
	if tk.TradeIDRaw == "" {
		t.Fatalf("expected trade_id_raw")
	}
}

func TestParseOrderbook_Snapshot_OK(t *testing.T) {
	raw := []byte(`{
		"topic": "orderbook.50.BTCUSDT",
		"type": "snapshot",
		"ts": 1672304484978,
		"data": {
			"s": "BTCUSDT",
			"b": [["16493.50","0.006"],["16493.00","0.100"]],
			"a": [["16611.00","0.029"],["16612.00","0.213"]],
			"u": 18521288,
			"seq": 7961638724
		},
		"cts": 1672304484976
	}`)

	upd, err := ParseOrderbook(raw)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if upd == nil {
		t.Fatalf("expected update")
	}
	if upd.Symbol != "BTCUSDT" {
		t.Fatalf("symbol mismatch: %s", upd.Symbol)
	}
	if !upd.IsSnapshot {
		t.Fatalf("expected snapshot")
	}
	if upd.UpdateID != 18521288 {
		t.Fatalf("u mismatch: %d", upd.UpdateID)
	}
	if upd.TsMs != 1672304484976 {
		t.Fatalf("cts mismatch: %d", upd.TsMs)
	}
	if len(upd.Bids) != 2 || len(upd.Asks) != 2 {
		t.Fatalf("levels mismatch")
	}
}

func TestBookState_ApplySnapshotAndDelta(t *testing.T) {
	bs := newBookState()

	snap := &OrderbookUpdate{
		Symbol:     "BTCUSDT",
		IsSnapshot: true,
		UpdateID:   10,
		TsMs:       1000,
		Bids:       [][]string{{"100", "1"}, {"99", "2"}},
		Asks:       [][]string{{"101", "1"}, {"102", "2"}},
	}
	bids, asks, prev, gapDetected, _, _ := bs.ApplyUpdate(snap, 50)
	if prev != 0 {
		t.Fatalf("prev should be 0, got %d", prev)
	}
	if gapDetected {
		t.Fatal("snapshot should never trigger gap detection")
	}
	if len(bids) != 2 || bids[0][0] != "100" {
		t.Fatalf("bids snapshot wrong: %#v", bids)
	}
	if len(asks) != 2 || asks[0][0] != "101" {
		t.Fatalf("asks snapshot wrong: %#v", asks)
	}

	delta := &OrderbookUpdate{
		Symbol:     "BTCUSDT",
		IsSnapshot: false,
		UpdateID:   11,
		TsMs:       1010,
		Bids:       [][]string{{"99", "0"}, {"98", "5"}},
		Asks:       [][]string{{"101", "0"}, {"103", "3"}},
	}
	bids, asks, prev, gapDetected, _, _ = bs.ApplyUpdate(delta, 50)
	if prev != 10 {
		t.Fatalf("prev should be 10, got %d", prev)
	}
	if gapDetected {
		t.Fatal("sequential delta should not trigger gap")
	}
	// bid 99 removed, bid 98 added
	if len(bids) != 2 || bids[0][0] != "100" || bids[1][0] != "98" {
		t.Fatalf("bids delta wrong: %#v", bids)
	}
	// ask 101 removed, ask 103 added
	if len(asks) != 2 || asks[0][0] != "102" || asks[1][0] != "103" {
		t.Fatalf("asks delta wrong: %#v", asks)
	}
}

// TestBookState_GapDetection verifies that a non-sequential delta UpdateID
// triggers gap detection, flushes the local book, and blocks further deltas.
func TestBookState_GapDetection(t *testing.T) {
	bs := newBookState()

	// Step 1: apply initial snapshot (UpdateID=100).
	snap := &OrderbookUpdate{
		IsSnapshot: true,
		UpdateID:   100,
		TsMs:       1000,
		Bids:       [][]string{{"50000", "1"}},
		Asks:       [][]string{{"50001", "1"}},
	}
	_, _, _, gapDetected, _, _ := bs.ApplyUpdate(snap, 50)
	if gapDetected {
		t.Fatal("snapshot must never produce a gap event")
	}
	if bs.LastUpdateID != 100 {
		t.Fatalf("expected LastUpdateID=100, got %d", bs.LastUpdateID)
	}

	// Step 2: apply sequential delta (UpdateID=101) — must succeed.
	delta101 := &OrderbookUpdate{
		IsSnapshot: false,
		UpdateID:   101,
		TsMs:       1001,
		Bids:       [][]string{{"49999", "2"}},
		Asks:       [][]string{},
	}
	bids, _, _, gapDetected, _, _ := bs.ApplyUpdate(delta101, 50)
	if gapDetected {
		t.Fatal("sequential delta 101 should not trigger gap")
	}
	// Book should contain both levels from snapshot + delta.
	if len(bids) != 2 {
		t.Fatalf("expected 2 bid levels after sequential delta, got %d: %#v", len(bids), bids)
	}

	// Step 3: apply delta with skipped UpdateID=103 (gap: expected 102, got 103).
	delta103 := &OrderbookUpdate{
		IsSnapshot: false,
		UpdateID:   103,
		TsMs:       1003,
		Bids:       [][]string{{"49998", "3"}},
		Asks:       [][]string{},
	}
	bids, asks, prevU, gapDetected, gapExpected, gapActual := bs.ApplyUpdate(delta103, 50)
	if !gapDetected {
		t.Fatal("skipped UpdateID must trigger gap detection")
	}
	if gapExpected != 102 {
		t.Fatalf("gapExpected should be 102, got %d", gapExpected)
	}
	if gapActual != 103 {
		t.Fatalf("gapActual should be 103, got %d", gapActual)
	}
	if prevU != 101 {
		t.Fatalf("prevU should be 101 (the last good UpdateID), got %d", prevU)
	}
	// Gap must return empty bids/asks — stale book must NOT be published.
	if len(bids) != 0 || len(asks) != 0 {
		t.Fatalf("gap event must return empty bids/asks, got bids=%v asks=%v", bids, asks)
	}

	// Step 4: after flush, the book must be empty (LastUpdateID reset to 0).
	if bs.LastUpdateID != 0 {
		t.Fatalf("after gap Reset() LastUpdateID must be 0, got %d", bs.LastUpdateID)
	}
	// A subsequent delta (no snapshot yet) must be silently ignored due to empty book guard.
	delta104 := &OrderbookUpdate{
		IsSnapshot: false,
		UpdateID:   104,
		TsMs:       1004,
		Bids:       [][]string{{"49997", "1"}},
		Asks:       [][]string{},
	}
	bids, asks, _, gapDetected, _, _ = bs.ApplyUpdate(delta104, 50)
	if gapDetected {
		t.Fatal("delta after flush (before resnapshot) must not produce another gap event")
	}
	if len(bids) != 0 || len(asks) != 0 {
		t.Fatalf("delta before resnapshot must produce empty book, got bids=%v asks=%v", bids, asks)
	}

	// Step 5: new snapshot recovers the book.
	snapRecovery := &OrderbookUpdate{
		IsSnapshot: true,
		UpdateID:   105,
		TsMs:       1005,
		Bids:       [][]string{{"50000", "5"}},
		Asks:       [][]string{{"50001", "5"}},
	}
	bids, asks, _, gapDetected, _, _ = bs.ApplyUpdate(snapRecovery, 50)
	if gapDetected {
		t.Fatal("recovery snapshot must not produce a gap event")
	}
	if len(bids) != 1 || len(asks) != 1 {
		t.Fatalf("recovery snapshot must restore 1+1 levels, got bids=%v asks=%v", bids, asks)
	}
	if bs.LastUpdateID != 105 {
		t.Fatalf("LastUpdateID after recovery must be 105, got %d", bs.LastUpdateID)
	}
}

