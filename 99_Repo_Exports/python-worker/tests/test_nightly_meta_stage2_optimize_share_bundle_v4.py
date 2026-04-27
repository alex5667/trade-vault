#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_nightly_meta_stage2_optimize_share_bundle_v4.py

Unit tests for nightly_meta_stage2_optimize_share_bundle_v4.py:
- Share simulation logic
- Multi-objective best share selection
- Regime bucket classification
- Data validation (meta_veto, meta_enforce_key)
- Step regularization
- Turnover proxy constraints
- Group caps (trend/range)
- Per-symbol budget (sum exec_rate_drop)
- Optional coupling (if trend < threshold -> range cap)
- Adaptive budgets (health factor calculation)
- Global budget selection (greedy downgrade)
"""

from __future__ import annotations

import json
import os
import sys
import time
from unittest.mock import MagicMock, patch, Mock

import pytest

# Import the module functions
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from tools.nightly_meta_stage2_optimize_share_bundle_v4 import (
    now_ms,
    sign,
    _f,
    _i,
    _event_ts_ms,
    regime_bucket,
    _hash01,
    pctl,
    stats,
    simulate_share,
    objective,
    build_options,
    enumerate_symbol_combos,
    calc_health_factor,
    summarize_metrics,
    select_under_global_budget,
    clamp01,
)


def test_regime_bucket():
    """Test regime bucket classification."""
    # News
    assert regime_bucket({"regime_group": "news_fomc"}) == "news"
    assert regime_bucket({"regime": "news_cpi_release"}) == "news"
    
    # Trend
    assert regime_bucket({"regime_group": "trend_bull"}) == "trend"
    assert regime_bucket({"regime": "bear_market"}) == "trend"
    assert regime_bucket({"regime": "bull"}) == "trend"
    
    # Range
    assert regime_bucket({"regime_group": "range_bound"}) == "range"
    assert regime_bucket({"regime": "chop"}) == "range"
    assert regime_bucket({"regime": "meanrev"}) == "range"
    
    # Thin
    assert regime_bucket({"regime": "thin_liquidity"}) == "thin"
    assert regime_bucket({"regime": "illiquid"}) == "thin"
    
    # Other (default)
    assert regime_bucket({"regime": "unknown"}) == "other"
    assert regime_bucket({}) == "other"


def test_stats():
    """Test stats calculation."""
    # Empty list
    s0 = stats([])
    assert s0["n"] == 0.0
    
    # Normal case
    rs = [0.5, -0.3, -1.5, 0.2, -2.0, 0.1]
    s = stats(rs)
    assert s["n"] == 6.0
    assert abs(s["meanR"] - (0.5 - 0.3 - 1.5 + 0.2 - 2.0 + 0.1) / 6.0) < 1e-6
    # Two values <= -1.0
    assert abs(s["tail_rate"] - 2.0 / 6.0) < 1e-6
    # Three values > 0.0
    assert abs(s["winrate"] - 3.0 / 6.0) < 1e-6
    # Percentiles
    assert "p05" in s
    assert "p95" in s
    assert "medianR" in s


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert abs(pctl(xs, 0.0) - 1.0) < 1e-6
    assert abs(pctl(xs, 0.5) - 3.0) < 1e-6
    assert abs(pctl(xs, 1.0) - 5.0) < 1e-6
    assert abs(pctl(xs, 0.25) - 2.0) < 1e-6
    assert abs(pctl(xs, 0.05) - 1.0) < 1e-6


def test_hash01():
    """Test deterministic hash function."""
    h1 = _hash01("test_key")
    h2 = _hash01("test_key")
    assert abs(h1 - h2) < 1e-10  # Deterministic
    assert 0.0 <= h1 < 1.0
    assert 0.0 <= h2 < 1.0
    
    h3 = _hash01("different_key")
    assert abs(h1 - h3) > 1e-6  # Different keys produce different hashes


def test_simulate_share():
    """Test share simulation logic."""
    # Create test rows with meta_veto and meta_enforce_key
    rows = [
        {"meta_enforce_key": "key1", "meta_veto": 1, "r_mult": -1.5},  # Would be blocked
        {"meta_enforce_key": "key2", "meta_veto": 0, "r_mult": 0.5},   # Would pass
        {"meta_enforce_key": "key3", "meta_veto": 1, "r_mult": -0.3},   # Would be blocked
        {"meta_enforce_key": "key4", "meta_veto": 0, "r_mult": 0.2},   # Would pass
    ]
    
    salt = "test_salt"
    
    # Test with share=0.0 (no blocking)
    rep0 = simulate_share(rows, share=0.0, salt=salt)
    assert rep0["share"] == 0.0
    assert rep0["used"] == 4
    assert rep0["blocked"] == 0
    assert rep0["exec_rate"] == 1.0
    assert "meanR" in rep0["opp"]
    assert "meanR" in rep0["exec"]
    assert isinstance(rep0["opp"]["meanR"], (int, float))
    
    # Test with share=1.0 (all vetoed trades blocked)
    rep1 = simulate_share(rows, share=1.0, salt=salt)
    assert rep1["share"] == 1.0
    assert rep1["used"] == 4
    # Blocked count depends on hash distribution
    assert rep1["blocked"] >= 0
    assert rep1["exec_rate"] <= 1.0


def test_simulate_share_missing_key():
    """Test that rows without meta_enforce_key are skipped."""
    rows = [
        {"meta_enforce_key": "key1", "meta_veto": 1, "r_mult": -1.5},
        {"meta_enforce_key": "", "meta_veto": 1, "r_mult": -0.5},  # Missing key
        {"meta_veto": 0, "r_mult": 0.3},  # Missing key
    ]
    
    salt = "test_salt"
    rep = simulate_share(rows, share=1.0, salt=salt)
    
    # Only first row should be processed
    assert rep["used"] == 1  # Only rows with keys are counted


def test_objective():
    """Test multi-objective function."""
    rep = {
        "opp": {"meanR": 0.2, "p05": -0.1},
        "exec": {"tail_rate": 0.15},
        "exec_rate": 0.5,
    }
    
    exec_rate_ref = 0.6
    cur_share = 0.25
    share = 0.35
    
    obj, drop = objective(
        rep,
        exec_rate_ref=exec_rate_ref,
        cur_share=cur_share,
        share=share,
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )
    
    # Should return tuple (obj, drop)
    assert isinstance(obj, (int, float))
    assert isinstance(drop, (int, float))
    assert drop == 0.1  # exec_rate_ref - exec_rate = 0.6 - 0.5
    
    # Components:
    # opp_mean = 0.2
    # exec_tail = 0.15, penalty = 0.50 * 0.15 = 0.075
    # opp_p05 = -0.1, penalty = 0.10 * 0.1 = 0.01
    # drop = 0.6 - 0.5 = 0.1, penalty = 0.30 * 0.1 = 0.03
    # step = |0.35 - 0.25| = 0.1, penalty = 0.05 * 0.1 = 0.005
    # obj ≈ 0.2 - 0.075 - 0.01 - 0.03 - 0.005 = 0.08
    expected = 0.2 - 0.50 * 0.15 - 0.10 * 0.1 - 0.30 * 0.1 - 0.05 * 0.1
    assert abs(obj - expected) < 1e-6


def test_build_options():
    """Test build_options function."""
    # Create test rows
    rows = []
    for i in range(500):
        key = f"key{i}"
        veto = 1 if (i % 10) < 3 else 0
        r_mult = -1.5 if veto == 1 else 0.3
        rows.append({
            "meta_enforce_key": key,
            "meta_veto": veto,
            "r_mult": r_mult,
        })
    
    salt = "test_salt"
    cur_share = 0.10
    grid = [0.10, 0.25, 0.35, 0.50]
    share_cap = 0.50
    max_up_step = 0.25
    max_down_step = 0.00
    min_exec_rate = 0.30
    max_exec_rate_drop = 0.20
    tail_exec_cap = 0.18
    lam_tail = 0.50
    lam_p05 = 0.10
    lam_turn = 0.30
    lam_step = 0.05
    
    opts = build_options(
        rows,
        salt=salt,
        cur_share=cur_share,
        grid=grid,
        share_cap=share_cap,
        max_up_step=max_up_step,
        max_down_step=max_down_step,
        min_exec_rate=min_exec_rate,
        max_exec_rate_drop=max_exec_rate_drop,
        tail_exec_cap=tail_exec_cap,
        lam_tail=lam_tail,
        lam_p05=lam_p05,
        lam_turn=lam_turn,
        lam_step=lam_step,
    )
    
    # Should return at least cur_share option
    assert len(opts) >= 1
    assert any(o["is_cur"] for o in opts)
    
    # All options should have required fields
    for o in opts:
        assert "share" in o
        assert "exec_rate" in o
        assert "drop" in o
        assert "obj" in o
        assert "rep" in o
        assert "is_cur" in o
        assert "exec_rate_ref" in o
    
    # Current share option should have drop=0.0
    cur_opt = next((o for o in opts if o["is_cur"]), None)
    assert cur_opt is not None
    assert abs(cur_opt["drop"]) < 1e-9


def test_enumerate_symbol_combos():
    """Test enumerate_symbol_combos function."""
    trend_opts = [
        {"share": 0.25, "drop": 0.10, "obj": 0.15, "rep": None, "is_cur": False},
        {"share": 0.35, "drop": 0.15, "obj": 0.20, "rep": None, "is_cur": False},
        {"share": 0.10, "drop": 0.0, "obj": 0.10, "rep": None, "is_cur": True},
    ]
    
    range_opts = [
        {"share": 0.25, "drop": 0.08, "obj": 0.12, "rep": None, "is_cur": False},
        {"share": 0.35, "drop": 0.12, "obj": 0.18, "rep": None, "is_cur": False},
        {"share": 0.10, "drop": 0.0, "obj": 0.08, "rep": None, "is_cur": True},
    ]
    
    symbol_budget = 0.25  # Max total drop = 0.25
    
    combos = enumerate_symbol_combos(
        trend_opts,
        range_opts,
        symbol_budget=symbol_budget,
        coupling_trend_lt=None,
        coupling_range_cap=None,
    )
    
    # Should return list of combos
    assert isinstance(combos, list)
    assert len(combos) > 0
    
    # All combos should respect budget
    for combo in combos:
        assert "trend" in combo
        assert "range" in combo
        assert "sum_drop" in combo
        assert "sum_obj" in combo
        assert combo["sum_drop"] <= symbol_budget + 1e-12
    
    # Should be sorted by sum_obj desc, then sum_drop asc
    for i in range(len(combos) - 1):
        assert combos[i]["sum_obj"] >= combos[i + 1]["sum_obj"]


def test_enumerate_symbol_combos_coupling():
    """Test optional coupling: if trend < threshold -> range cap."""
    trend_opts = [
        {"share": 0.20, "drop": 0.05, "obj": 0.10, "rep": None, "is_cur": False},  # trend < 0.25
        {"share": 0.30, "drop": 0.10, "obj": 0.15, "rep": None, "is_cur": False},  # trend >= 0.25
    ]
    
    range_opts = [
        {"share": 0.40, "drop": 0.08, "obj": 0.12, "rep": None, "is_cur": False},  # range > 0.35
        {"share": 0.30, "drop": 0.06, "obj": 0.10, "rep": None, "is_cur": False},  # range <= 0.35
    ]
    
    symbol_budget = 0.25
    trend_threshold = 0.25
    range_cap_when_trend_lt = 0.35
    
    combos = enumerate_symbol_combos(
        trend_opts,
        range_opts,
        symbol_budget=symbol_budget,
        coupling_trend_lt=trend_threshold,
        coupling_range_cap=range_cap_when_trend_lt,
    )
    
    # If trend=0.20 (< 0.25), then range must be <= 0.35
    # So combo (trend=0.20, range=0.40) should be rejected
    for combo in combos:
        trend_share = combo["trend"]["share"]
        range_share = combo["range"]["share"]
        if trend_share is not None and range_share is not None:
            if float(trend_share) < trend_threshold:
                assert float(range_share) <= range_cap_when_trend_lt + 1e-9


def test_summarize_metrics():
    """Test summarize_metrics function."""
    rows = [
        {"ok": 1, "ok_soft": 0, "latency_us": 1000.0, "exec_risk_norm": 0.5},
        {"ok": 1, "ok_soft": 1, "latency_us": 2000.0, "exec_risk_norm": 0.6},
        {"ok": 0, "ok_soft": 0, "latency_us": 5000.0, "exec_risk_norm": 0.8},
        {"ok": 1, "ok_soft": 0, "latency_us": 1500.0, "exec_risk_norm": 0.4},
    ]
    
    st = summarize_metrics(rows)
    
    assert st["n"] == 4.0
    assert st["ok_rate"] == 0.75  # 3 out of 4
    assert st["soft_rate"] == 0.25  # 1 out of 4
    assert st["lat_p99_us"] > 0.0
    assert st["exec_p90"] > 0.0
    
    # Empty rows
    st_empty = summarize_metrics([])
    assert st_empty["n"] == 0.0


def test_calc_health_factor():
    """Test calc_health_factor function."""
    # Healthy stats
    st_healthy = {
        "exec_p90": 0.70,  # below target 0.75
        "lat_p99_us": 3000.0,  # below target 4000
        "soft_rate": 0.30,  # below target 0.35
        "ok_rate": 0.25,  # above target 0.20
    }
    
    factor = calc_health_factor(
        st_healthy,
        exec_target=0.75, exec_span=0.25,
        lat_target_us=4000, lat_span_us=6000,
        soft_target=0.35, soft_span=0.35,
        ok_target=0.20, ok_span=0.20,
        w_exec=0.35, w_lat=0.25, w_soft=0.25, w_ok=0.15,
        floor=0.35, cap=1.00,
    )
    
    # Should be close to 1.0 (healthy)
    assert 0.35 <= factor <= 1.00
    assert factor > 0.8  # Healthy case
    
    # Unhealthy stats
    st_unhealthy = {
        "exec_p90": 0.95,  # above target
        "lat_p99_us": 10000.0,  # above target
        "soft_rate": 0.60,  # above target
        "ok_rate": 0.10,  # below target
    }
    
    factor_unhealthy = calc_health_factor(
        st_unhealthy,
        exec_target=0.75, exec_span=0.25,
        lat_target_us=4000, lat_span_us=6000,
        soft_target=0.35, soft_span=0.35,
        ok_target=0.20, ok_span=0.20,
        w_exec=0.35, w_lat=0.25, w_soft=0.25, w_ok=0.15,
        floor=0.35, cap=1.00,
    )
    
    # Should be lower (unhealthy)
    assert 0.35 <= factor_unhealthy <= 1.00
    assert factor_unhealthy < factor  # Unhealthy should be lower than healthy


def test_select_under_global_budget():
    """Test select_under_global_budget function."""
    # Create symbol plans with multiple combos
    symbol_plans = {
        "BTCUSDT": [
            {"sum_drop": 0.30, "sum_obj": 0.25, "trend": {"share": 0.35}, "range": {"share": 0.25}},
            {"sum_drop": 0.20, "sum_obj": 0.20, "trend": {"share": 0.25}, "range": {"share": 0.20}},
            {"sum_drop": 0.10, "sum_obj": 0.15, "trend": {"share": 0.15}, "range": {"share": 0.15}},
        ],
        "ETHUSDT": [
            {"sum_drop": 0.25, "sum_obj": 0.22, "trend": {"share": 0.30}, "range": {"share": 0.20}},
            {"sum_drop": 0.15, "sum_obj": 0.18, "trend": {"share": 0.20}, "range": {"share": 0.15}},
            {"sum_drop": 0.05, "sum_obj": 0.12, "trend": {"share": 0.10}, "range": {"share": 0.10}},
        ],
    }
    
    global_budget = 0.30  # Total drop should be <= 0.30
    
    chosen = select_under_global_budget(symbol_plans, global_budget)
    
    # Should return one combo per symbol
    assert len(chosen) == 2
    assert "BTCUSDT" in chosen
    assert "ETHUSDT" in chosen
    
    # Total drop should be within budget
    total_drop = chosen["BTCUSDT"]["sum_drop"] + chosen["ETHUSDT"]["sum_drop"]
    assert total_drop <= global_budget + 1e-12
    
    # If we start with best (0.30 + 0.25 = 0.55 > 0.30), should downgrade
    # Best combo for BTCUSDT has drop=0.30, which alone exceeds budget
    # So should pick lower drop combos


def test_select_under_global_budget_within_budget():
    """Test select_under_global_budget when already within budget."""
    symbol_plans = {
        "BTCUSDT": [
            {"sum_drop": 0.10, "sum_obj": 0.20, "trend": {"share": 0.25}, "range": {"share": 0.20}},
        ],
        "ETHUSDT": [
            {"sum_drop": 0.10, "sum_obj": 0.18, "trend": {"share": 0.20}, "range": {"share": 0.15}},
        ],
    }
    
    global_budget = 0.30  # Total drop = 0.20 < 0.30
    
    chosen = select_under_global_budget(symbol_plans, global_budget)
    
    # Should pick best combos (first in list)
    assert chosen["BTCUSDT"]["sum_obj"] == 0.20
    assert chosen["ETHUSDT"]["sum_obj"] == 0.18


def test_clamp01():
    """Test clamp01 function."""
    assert clamp01(0.5) == 0.5
    assert clamp01(0.0) == 0.0
    assert clamp01(1.0) == 1.0
    assert clamp01(-0.5) == 0.0
    assert clamp01(1.5) == 1.0


def test_event_ts_ms():
    """Test event timestamp extraction."""
    # Test exit_ts_ms (milliseconds)
    assert _event_ts_ms({"exit_ts_ms": 1609459200000}) == 1609459200000
    
    # Test ts_ms (milliseconds)
    assert _event_ts_ms({"ts_ms": 1609459200000}) == 1609459200000
    
    # Test ts (seconds) - should be converted to milliseconds
    assert _event_ts_ms({"ts": 1609459200}) == 1609459200000
    
    # Test missing timestamp
    assert _event_ts_ms({}) == 0


def test_safe_conversions():
    """Test safe conversion functions."""
    assert _f("1.5") == 1.5
    assert _f(1.5) == 1.5
    assert _f(None, 0.0) == 0.0
    assert _f("invalid", 0.0) == 0.0
    
    assert _i("42") == 42
    assert _i(42) == 42
    assert _i(None, 0) == 0
    assert _i("invalid", 0) == 0


def test_sign():
    """Test HMAC signature generation."""
    bid = "test_bundle_id"
    secret = "test_secret"
    
    sig1 = sign(bid, secret)
    sig2 = sign(bid, secret)
    
    assert sig1 == sig2  # Deterministic
    assert len(sig1) == 8  # 8 hex characters
    assert all(c in "0123456789abcdef" for c in sig1)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

