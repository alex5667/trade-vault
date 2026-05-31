from __future__ import annotations

"""Unit tests for triple-barrier labeling core logic."""

from core.tb_labeling import Barriers, barrier_stats, exec_cost_r, infer_tp_sl_bps


def test_infer_stop_first():
    """Test that stop_bps takes precedence over atr_bps."""
    b = infer_tp_sl_bps(
        {"stop_bps": 50.0, "atr_bps": 100.0},
        tp_k_atr=1.0,
        sl_k_atr=1.0,
        fallback_tp_bps=30,
        fallback_sl_bps=30,
    )
    assert isinstance(b, Barriers)
    assert b.scale_bps == 50.0
    assert b.tp_bps == 50.0
    assert b.sl_bps == 50.0


def test_infer_atr_fallback():
    """Test that atr_bps is used when stop_bps is missing."""
    b = infer_tp_sl_bps(
        {"atr_bps": 100.0},
        tp_k_atr=1.0,
        sl_k_atr=1.0,
        fallback_tp_bps=30,
        fallback_sl_bps=30,
    )
    assert b.scale_bps == 100.0
    assert b.tp_bps == 100.0
    assert b.sl_bps == 100.0


def test_infer_fallback():
    """Test that fallback values are used when no indicators."""
    b = infer_tp_sl_bps(
        {},
        tp_k_atr=1.0,
        sl_k_atr=1.0,
        fallback_tp_bps=30,
        fallback_sl_bps=30,
    )
    assert b.scale_bps == 0.0
    assert b.tp_bps == 30.0
    assert b.sl_bps == 30.0


def test_tp_hit_long_mae_mfe():
    """Test TP hit with MAE/MFE tracking for LONG position."""
    b = Barriers(tp_bps=50.0, sl_bps=50.0, scale_bps=50.0)
    # Path: entry at 100, dips to 99.8 (adverse), then hits TP at 100.6
    path = [(0, 100.0), (100, 99.8), (200, 100.6)]
    r = barrier_stats(
        ts0=0,
        direction="LONG",
        entry_px=100.0,
        path=path,
        b=b,
        h_ms=1000,
        adv_max=9.9,
    )
    assert r["label"] == "TP"
    assert r["mfe_bps"] > 50.0  # Hit TP
    assert r["mae_bps"] > 0.0  # Had adverse move
    assert r["y_edge"] == 1  # TP hit and adverse_proxy <= adv_max


def test_sl_hit_short():
    """Test SL hit for SHORT position."""
    b = Barriers(tp_bps=30.0, sl_bps=30.0, scale_bps=30.0)
    # Path: entry at 100, moves up to 100.35 (SL for SHORT)
    path = [(0, 100.0), (50, 100.35)]
    r = barrier_stats(
        ts0=0,
        direction="SHORT",
        entry_px=100.0,
        path=path,
        b=b,
        h_ms=1000,
        adv_max=9.9,
    )
    assert r["label"] == "SL"
    assert r["y_edge"] == 0  # Not TP


def test_timeout():
    """Test timeout when no barrier is hit."""
    b = Barriers(tp_bps=50.0, sl_bps=50.0, scale_bps=50.0)
    # Path: entry at 100, small move but no barrier hit
    path = [(0, 100.0), (500, 100.1)]
    r = barrier_stats(
        ts0=0,
        direction="LONG",
        entry_px=100.0,
        path=path,
        b=b,
        h_ms=1000,
        adv_max=1.2,
    )
    assert r["label"] == "TIMEOUT"
    assert r["hit_ms"] == 1000  # End of horizon


def test_no_ticks():
    """Test handling of empty tick path."""
    b = Barriers(tp_bps=50.0, sl_bps=50.0, scale_bps=50.0)
    r = barrier_stats(
        ts0=0,
        direction="LONG",
        entry_px=100.0,
        path=[],
        b=b,
        h_ms=1000,
        adv_max=1.2,
    )
    assert r["label"] == "NO_TICKS"
    assert r["ret_bps"] == 0.0
    assert r["r_mult"] == 0.0


def test_exec_cost_r():
    """Test execution cost calculation."""
    indicators = {"spread_bps": 5.0, "expected_slippage_bps": 3.0}
    scale_bps = 50.0
    cost_r = exec_cost_r(indicators, scale_bps)
    assert cost_r == (5.0 + 3.0) / 50.0  # 0.16 R-multiples


def test_exec_cost_r_zero_scale():
    """Test execution cost with zero scale."""
    indicators = {"spread_bps": 5.0}
    cost_r = exec_cost_r(indicators, 0.0)
    assert cost_r == 0.0

