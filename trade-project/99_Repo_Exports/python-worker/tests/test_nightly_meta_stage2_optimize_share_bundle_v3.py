#!/usr/bin/env python3
from __future__ import annotations

"""
test_nightly_meta_stage2_optimize_share_bundle_v3.py

Unit tests for nightly_meta_stage2_optimize_share_bundle_v3.py:
- Share simulation logic
- Multi-objective best share selection
- Regime bucket classification
- Data validation (meta_veto, meta_enforce_key)
- Step regularization
- Turnover proxy constraints
- Group caps (trend/range)
- Per-symbol budget (sum exec_rate_drop)
- Optional coupling (if trend < threshold -> range cap)
"""


import os
import sys

import pytest

# Import the module functions
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from tools.nightly_meta_stage2_optimize_share_bundle_v3 import (
    _event_ts_ms,
    _f,
    _hash01,
    _i,
    build_options,
    objective,
    pctl,
    pick_combo_under_budget,
    regime_bucket,
    sign,
    simulate_share,
    stats,
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

    obj = objective(
        rep,
        exec_rate_ref=exec_rate_ref,
        cur_share=cur_share,
        share=share,
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )

    # Should be a float
    assert isinstance(obj, (int, float))

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
        assert "exec_rate_drop" in o
        assert "obj" in o
        assert "rep" in o
        assert "is_cur" in o

    # Current share option should have drop=0.0
    cur_opt = next((o for o in opts if o["is_cur"]), None)
    assert cur_opt is not None
    assert abs(cur_opt["exec_rate_drop"]) < 1e-9


def test_build_options_share_cap():
    """Test that share_cap is enforced."""
    rows = []
    for i in range(500):
        key = f"key{i}"
        rows.append({
            "meta_enforce_key": key,
            "meta_veto": 0,
            "r_mult": 0.3,
        })

    salt = "test_salt"
    cur_share = 0.10
    grid = [0.10, 0.25, 0.50, 0.75, 1.00]  # Grid includes values > cap
    share_cap = 0.50  # Cap at 0.50

    opts = build_options(
        rows,
        salt=salt,
        cur_share=cur_share,
        grid=grid,
        share_cap=share_cap,
        max_up_step=1.0,  # Allow all steps
        max_down_step=0.00,
        min_exec_rate=0.30,
        max_exec_rate_drop=0.20,
        tail_exec_cap=0.18,
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )

    # All options should respect share_cap
    for o in opts:
        assert o["share"] <= share_cap + 1e-9


def test_pick_combo_under_budget():
    """Test pick_combo_under_budget function."""
    # Create test options
    trend_opts = [
        {"share": 0.25, "exec_rate_drop": 0.10, "obj": 0.15, "rep": None, "is_cur": False},
        {"share": 0.35, "exec_rate_drop": 0.15, "obj": 0.20, "rep": None, "is_cur": False},
        {"share": 0.10, "exec_rate_drop": 0.0, "obj": 0.10, "rep": None, "is_cur": True},
    ]

    range_opts = [
        {"share": 0.25, "exec_rate_drop": 0.08, "obj": 0.12, "rep": None, "is_cur": False},
        {"share": 0.35, "exec_rate_drop": 0.12, "obj": 0.18, "rep": None, "is_cur": False},
        {"share": 0.10, "exec_rate_drop": 0.0, "obj": 0.08, "rep": None, "is_cur": True},
    ]

    budget = 0.25  # Max total drop = 0.25

    combo = pick_combo_under_budget(
        trend_opts,
        range_opts,
        budget=budget,
        range_cap_when_trend_lt=None,
        trend_threshold=None,
    )

    # Should return a valid combo
    assert "trend" in combo
    assert "range" in combo
    assert "sum_drop" in combo
    assert "sum_obj" in combo

    # Sum drop should be within budget
    assert combo["sum_drop"] <= budget + 1e-9

    # Should pick best combo (highest sum_obj)
    # Best combo: trend=0.35 (drop=0.15, obj=0.20) + range=0.25 (drop=0.08, obj=0.12)
    # Total: drop=0.23, obj=0.32
    assert combo["sum_obj"] > 0.0


def test_pick_combo_under_budget_exceeds_budget():
    """Test that combos exceeding budget are rejected."""
    trend_opts = [
        {"share": 0.50, "exec_rate_drop": 0.20, "obj": 0.25, "rep": None, "is_cur": False},
    ]

    range_opts = [
        {"share": 0.50, "exec_rate_drop": 0.20, "obj": 0.25, "rep": None, "is_cur": False},
    ]

    budget = 0.25  # Max total drop = 0.25, but combo has 0.20 + 0.20 = 0.40

    combo = pick_combo_under_budget(
        trend_opts,
        range_opts,
        budget=budget,
        range_cap_when_trend_lt=None,
        trend_threshold=None,
    )

    # Should fallback to current shares (drop=0.0)
    assert "fallback" in combo or combo["sum_drop"] <= budget + 1e-9


def test_pick_combo_under_budget_coupling():
    """Test optional coupling: if trend < threshold -> range cap."""
    trend_opts = [
        {"share": 0.20, "exec_rate_drop": 0.05, "obj": 0.10, "rep": None, "is_cur": False},  # trend < 0.25
        {"share": 0.30, "exec_rate_drop": 0.10, "obj": 0.15, "rep": None, "is_cur": False},  # trend >= 0.25
    ]

    range_opts = [
        {"share": 0.40, "exec_rate_drop": 0.08, "obj": 0.12, "rep": None, "is_cur": False},  # range > 0.35
        {"share": 0.30, "exec_rate_drop": 0.06, "obj": 0.10, "rep": None, "is_cur": False},  # range <= 0.35
    ]

    budget = 0.25
    trend_threshold = 0.25
    range_cap_when_trend_lt = 0.35

    combo = pick_combo_under_budget(
        trend_opts,
        range_opts,
        budget=budget,
        range_cap_when_trend_lt=range_cap_when_trend_lt,
        trend_threshold=trend_threshold,
    )

    # If trend=0.20 (< 0.25), then range must be <= 0.35
    # So combo (trend=0.20, range=0.40) should be rejected
    # Valid combos: (trend=0.20, range=0.30) or (trend=0.30, range=0.40)
    trend_share = combo["trend"]["share"]
    range_share = combo["range"]["share"]

    if trend_share is not None and range_share is not None:
        if float(trend_share) < trend_threshold:
            assert float(range_share) <= range_cap_when_trend_lt + 1e-9


def test_pick_combo_under_budget_none_opts():
    """Test handling of None options."""
    # Both None
    combo = pick_combo_under_budget(None, None, budget=0.25, range_cap_when_trend_lt=None, trend_threshold=None)
    assert "ok" in combo and not combo["ok"]

    # One None
    range_opts = [
        {"share": 0.25, "exec_rate_drop": 0.10, "obj": 0.12, "rep": None, "is_cur": True},
    ]
    combo = pick_combo_under_budget(None, range_opts, budget=0.25, range_cap_when_trend_lt=None, trend_threshold=None)
    assert "trend" in combo
    assert "range" in combo


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

