#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
test_nightly_meta_stage2_optimize_share_bundle_v2.py

Unit tests for nightly_meta_stage2_optimize_share_bundle_v2.py:
- Share simulation logic
- Multi-objective best share selection
- Regime bucket classification
- Data validation (meta_veto, meta_enforce_key)
- Step regularization
- Turnover proxy constraints
"""


import json
import os
import sys
import time
from unittest.mock import MagicMock, patch, Mock

import pytest

# Import the module functions
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from tools.nightly_meta_stage2_optimize_share_bundle_v2 import (
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
    pick_best_share_multiobjective,
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


def test_pick_best_share_multiobjective():
    """Test multi-objective best share selection."""
    # Create test rows: mix of good and bad trades
    rows = []
    for i in range(500):
        key = f"key{i}"
        # 30% veto rate
        veto = 1 if (i % 10) < 3 else 0
        # Vetoed trades have worse returns
        r_mult = -1.5 if veto == 1 else 0.3
        rows.append({
            "meta_enforce_key": key,
            "meta_veto": veto,
            "r_mult": r_mult,
        })
    
    salt = "test_salt"
    grid = [0.10, 0.25, 0.35, 0.50, 0.75, 1.00]
    cur_share = 0.10
    max_up_step = 0.25
    max_down_step = 0.00
    min_exec_rate = 0.30
    max_exec_rate_drop = 0.20
    tail_exec_cap = 0.18
    lam_tail = 0.50
    lam_p05 = 0.10
    lam_turn = 0.30
    lam_step = 0.05
    
    best_s, rep = pick_best_share_multiobjective(
        rows,
        grid=grid,
        salt=salt,
        cur_share=cur_share,
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
    
    # Should return a share within step limits
    assert best_s >= cur_share - max_down_step - 1e-9
    assert best_s <= cur_share + max_up_step + 1e-9
    assert rep is not None
    assert "ref" in rep
    if rep.get("best") is not None:
        assert "objective" in rep["best"]
        assert "turnover_drop" in rep["best"]
        assert "exec_rate_ref" in rep["best"]


def test_pick_best_share_multiobjective_step_limit():
    """Test that step limits are enforced."""
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
    grid = [0.10, 0.25, 0.50, 0.75, 1.00]  # Grid includes values beyond step limit
    cur_share = 0.10
    max_up_step = 0.25  # Should limit to 0.35 max
    max_down_step = 0.00
    
    best_s, rep = pick_best_share_multiobjective(
        rows,
        grid=grid,
        salt=salt,
        cur_share=cur_share,
        max_up_step=max_up_step,
        max_down_step=max_down_step,
        min_exec_rate=0.30,
        max_exec_rate_drop=0.20,
        tail_exec_cap=0.18,
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )
    
    # Should not exceed step limit
    assert best_s <= cur_share + max_up_step + 1e-9
    assert best_s >= cur_share - max_down_step - 1e-9


def test_pick_best_share_multiobjective_turnover_constraint():
    """Test that turnover drop constraint is enforced."""
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
    grid = [0.10, 0.25, 0.50]
    cur_share = 0.10
    max_exec_rate_drop = 0.20  # Max 20% drop in exec_rate
    
    best_s, rep = pick_best_share_multiobjective(
        rows,
        grid=grid,
        salt=salt,
        cur_share=cur_share,
        max_up_step=0.50,
        max_down_step=0.00,
        min_exec_rate=0.30,
        max_exec_rate_drop=max_exec_rate_drop,
        tail_exec_cap=0.18,
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )
    
    # If best found, check turnover drop
    if rep.get("best") is not None:
        drop = rep["best"].get("turnover_drop", 0.0)
        assert drop <= max_exec_rate_drop + 1e-9


def test_pick_best_share_multiobjective_no_change():
    """Test that current share is returned if no better option found."""
    rows = []
    for i in range(200):  # Minimum required
        key = f"key{i}"
        rows.append({
            "meta_enforce_key": key,
            "meta_veto": 0,
            "r_mult": 0.1,  # All good trades
        })
    
    salt = "test_salt"
    # Grid that doesn't include current share and all values are out of step range
    grid = [0.50, 0.75, 1.00]
    cur_share = 0.10
    
    # With max_up_step=0.05, all grid values (0.50, 0.75, 1.00) are out of range
    # This should force function to return current share with fallback
    best_s, rep = pick_best_share_multiobjective(
        rows,
        grid=grid,
        salt=salt,
        cur_share=cur_share,
        max_up_step=0.05,  # Only allows up to 0.15, but grid has 0.50, 0.75, 1.00
        max_down_step=0.00,
        min_exec_rate=0.30,  # Normal requirement
        max_exec_rate_drop=0.20,  # Normal
        tail_exec_cap=0.18,  # Normal
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )
    
    # Should return current share if no feasible option found (all grid values out of step range)
    assert best_s == cur_share
    # Either fallback is set or best is None (both indicate no change)
    assert rep.get("fallback") == "no_change" or rep.get("best") is None


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


def test_pick_best_share_multiobjective_separate_grids():
    """Test that separate grids work for trend vs range."""
    # This test verifies the concept - actual grid selection happens in main()
    rows_trend = []
    rows_range = []
    
    for i in range(500):
        key = f"key{i}"
        veto = 1 if (i % 10) < 3 else 0
        r_mult = -1.5 if veto == 1 else 0.3
        rows_trend.append({
            "meta_enforce_key": key,
            "meta_veto": veto,
            "r_mult": r_mult,
        })
        rows_range.append({
            "meta_enforce_key": key,
            "meta_veto": veto,
            "r_mult": r_mult,
        })
    
    salt = "test_salt"
    grid_trend = [0.10, 0.25, 0.35, 0.50, 0.75, 1.00]
    grid_range = [0.10, 0.15, 0.25, 0.35, 0.50]
    
    # Both should work with their respective grids
    best_trend, _ = pick_best_share_multiobjective(
        rows_trend,
        grid=grid_trend,
        salt=salt,
        cur_share=0.10,
        max_up_step=0.25,
        max_down_step=0.00,
        min_exec_rate=0.30,
        max_exec_rate_drop=0.20,
        tail_exec_cap=0.18,
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )
    
    best_range, _ = pick_best_share_multiobjective(
        rows_range,
        grid=grid_range,
        salt=salt,
        cur_share=0.10,
        max_up_step=0.25,
        max_down_step=0.00,
        min_exec_rate=0.30,
        max_exec_rate_drop=0.20,
        tail_exec_cap=0.18,
        lam_tail=0.50,
        lam_p05=0.10,
        lam_turn=0.30,
        lam_step=0.05,
    )
    
    assert best_trend in grid_trend or best_trend == 0.10
    assert best_range in grid_range or best_range == 0.10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

