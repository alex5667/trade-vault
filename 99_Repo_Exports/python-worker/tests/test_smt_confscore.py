from __future__ import annotations

from core.smt_symbol_snapshot import SymbolSnapshot
from services.smt_logic import decide_smt


def test_leader_conf_score_formula():
    """Test SMT leader conf score calculation."""
    leader = SymbolSnapshot(
        symbol="BTCUSDT",
        trend_dir="UP",
        close_cross=1,
        of_strong=1,
        of_dir="LONG",
        of_confirm_score=1.0,
        reclaim=0,
        zone_dist_bp=5.0,
        delta_eff_norm=0.8,
        rsi14=50,
        cvd_slope=0.0,
        retrace_atr=0.1,
    )
    sat = SymbolSnapshot(symbol="ETHUSDT", rsi14=55, cvd_slope=0.1, retrace_atr=0.05)
    dec = decide_smt(leader, [leader, sat], coh=0.9, cfg={"smt_coh_threshold":0.65, "smt_zone_max_bp":15.0, "smt_leader_min_of_score":1.0})
    assert dec.kind == "continuation"
    
    # zone score: 1 - 5/15 = 0.6666
    # conf = 0.6*0.8 + 0.4*0.6666 = 0.48 + 0.2666 = 0.7466
    assert 0.74 <= float(dec.conf_score) <= 0.76

def test_leader_reject_low_score():
    """Test that low conf score prevents confirmation even if other conditions met."""
    leader = SymbolSnapshot(
        symbol="BTCUSDT",
        trend_dir="UP",
        close_cross=1,
        of_strong=1,
        of_dir="LONG",
        of_confirm_score=1.0,
        reclaim=0,
        zone_dist_bp=20.0, # too far -> zone_score=0
        delta_eff_norm=0.1, # weak -> 0.6*0.1 = 0.06
        # total score = 0.06
        rsi14=50,
        cvd_slope=0.0,
        retrace_atr=0.1,
    )
    sat = SymbolSnapshot(symbol="ETHUSDT", rsi14=55, cvd_slope=0.1, retrace_atr=0.05)
    
    # Default min score is 0.65
    dec = decide_smt(leader, [leader, sat], coh=0.9, cfg={"smt_coh_threshold":0.65, "smt_zone_max_bp":15.0})
    
    # Should not be continuation because score 0.06 < 0.65
    assert dec.kind != "continuation"
    assert dec.reason == "confirm_but_weak"
