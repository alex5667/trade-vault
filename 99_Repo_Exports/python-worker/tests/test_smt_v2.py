import pytest
from core.smt_symbol_snapshot import SymbolSnapshot
from services.smt_logic import (
    leader_confirm_reject,
    detect_smt_divergence,
    decide_smt,
    SMTDecision,
    Ranked,
    rank_satellites
)

def test_snapshot_serialization():
    snap = SymbolSnapshot(symbol="BTCUSDT", ts_ms=1000, trend_dir="UP", close_px=50000.0, close_cross=1, of_strong=1)
    d = snap.to_dict()
    assert d["symbol"] == "BTCUSDT"
    assert d["ts_ms"] == 1000
    assert d["trend_dir"] == "UP"
    
    j = snap.to_json()
    assert '"symbol":"BTCUSDT"' in j
    
    snap2 = SymbolSnapshot.from_dict(d)
    assert snap2.symbol == "BTCUSDT"
    assert snap2.close_cross == 1
    assert snap2.of_strong == 1

def test_leader_confirm():
    # Setup: Confirmed Leader
    # close_cross=1, of_strong=1, no reclaim_opp
    l = SymbolSnapshot(symbol="L", trend_dir="UP", of_dir="LONG", close_cross=1, of_strong=1, reclaim=0)
    conf, rej, cr, rr = leader_confirm_reject(l, {})
    assert conf is True
    assert rej is False
    assert "closeCross" in cr

    # Setup: Reclaim Opp (veto confirmation)
    # reclaim=1, reclaim_dir="SHORT" vs of_dir="LONG"
    l2 = SymbolSnapshot(symbol="L", trend_dir="UP", of_dir="LONG", close_cross=1, of_strong=1, reclaim=1, reclaim_dir="SHORT")
    conf, rej, _, _ = leader_confirm_reject(l2, {})
    assert conf is False # vetoed by reclaim_opp

def test_leader_reject():
    # Setup: Rejected Leader
    # sweep=1, reclaim=1, weak_progress=1
    l = SymbolSnapshot(symbol="L", sweep=1, sweep_dir="LONG", reclaim=1, reclaim_dir="LONG", weak_progress=1)
    conf, rej, cr, rr = leader_confirm_reject(l, {})
    assert rej is True
    assert "weak" in rr

    # Setup: Rejected Leader via Divergence
    # sweep "SHORT" (Bearish context) + Bearish Regular Div
    l2 = SymbolSnapshot(symbol="L", sweep=1, sweep_dir="SHORT", reclaim=1, weak_progress=0, div_kind="bearish_regular_rsi")
    conf, rej, _, _ = leader_confirm_reject(l2, {})
    assert rej is True # sweep + reclaim + div

def test_smt_divergence_bullish():
    # Leader makes Lower Low (LL): low0 < low1
    # Satellite makes Higher Low (HL): low0 > low1
    leader = SymbolSnapshot(symbol="BTC", ts_ms=200, swing_low_0=49000, swing_low_1=49500) # LL
    sat = SymbolSnapshot(symbol="ETH", ts_ms=200, swing_low_0=3050, swing_low_1=3000)    # HL
    
    div = detect_smt_divergence(leader, sat)
    assert div is not None
    assert div.kind == "bullish_smt"
    assert div.leader == "BTC"
    assert div.satellite == "ETH"

def test_smt_divergence_bearish():
    # Leader makes Higher High (HH): high0 > high1
    # Satellite makes Lower High (LH): high0 < high1
    leader = SymbolSnapshot(symbol="BTC", ts_ms=200, swing_high_0=51000, swing_high_1=50000) # HH
    sat = SymbolSnapshot(symbol="ETH", ts_ms=200, swing_high_0=3100, swing_high_1=3200)    # LH
    
    div = detect_smt_divergence(leader, sat)
    assert div is not None
    assert div.kind == "bearish_smt"

def test_ranking():
    # Sat1: RSI=70 (high), CVD=10 (high), retrace=0.1
    # Sat2: RSI=30 (low), CVD=-5 (low), retrace=0.5
    s1 = SymbolSnapshot(symbol="S1", rsi14=70, cvd_slope=10, retrace_atr=0.1)
    s2 = SymbolSnapshot(symbol="S2", rsi14=30, cvd_slope=-5, retrace_atr=0.5)
    
    ranked = rank_satellites([s1, s2], "L", "UP")
    assert ranked[0].symbol == "S1" # expect S1 to be stronger -> higher rank
    assert ranked[1].symbol == "S2"

def test_decide_continuation():
    # Leader confirmed, high coherence -> Continuation pick best sat
    leader = SymbolSnapshot(symbol="L", trend_dir="UP", of_dir="LONG", close_cross=1, of_strong=1)
    s1 = SymbolSnapshot(symbol="S1", rsi14=60, cvd_slope=5)
    s2 = SymbolSnapshot(symbol="S2", rsi14=40, cvd_slope=1)
    
    dec = decide_smt(leader, [leader, s1, s2], coh=0.8, cfg={"smt_coh_threshold": 0.6})
    
    assert dec.kind == "continuation"
    assert dec.pick == "S1"

def test_decide_reversal():
    # Leader rejected (sweep+reclaim+weak), SMT divergence present -> Reversal
    leader = SymbolSnapshot(symbol="L", trend_dir="UP", sweep=1, sweep_dir="LONG", reclaim=1, weak_progress=1, 
                            swing_low_0=100, swing_low_1=105) # LL (unexpected for UP trend? context dependent)
                            # Actually sweep LONG usually means sweeping LOWs, so LL is consistent.
    
    # Sat: HL (Bullish SMT)
    s1 = SymbolSnapshot(symbol="S1", swing_low_0=55, swing_low_1=50) # HL
    
    dec = decide_smt(leader, [leader, s1], coh=0.5, cfg={})
    
    assert dec.kind == "reversal"
    assert dec.div == "bullish_smt"
    assert dec.pick == "S1" # bullish reversal picks strongest satellite
