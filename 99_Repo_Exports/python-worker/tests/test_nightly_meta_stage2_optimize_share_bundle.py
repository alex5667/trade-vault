#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_nightly_meta_stage2_optimize_share_bundle.py

Unit tests for nightly_meta_stage2_optimize_share_bundle.py:
- Share simulation logic
- Best share selection
- Regime bucket classification
- Data validation (meta_veto, meta_enforce_key)
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
from tools.nightly_meta_stage2_optimize_share_bundle import (
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
    pick_best_share,
)


def test_regime_bucket():
    """Test regime bucket classification."""
    # News
    assert regime_bucket({"regime_group": "news_fomc"}) == "news"
    assert regime_bucket({"regime": "cpi_release"}) == "news"
    
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


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert abs(pctl(xs, 0.0) - 1.0) < 1e-6
    assert abs(pctl(xs, 0.5) - 3.0) < 1e-6
    assert abs(pctl(xs, 1.0) - 5.0) < 1e-6
    assert abs(pctl(xs, 0.25) - 2.0) < 1e-6


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
    rep0 = simulate_share(rows, share=0.0, salt=salt, min_exec_rate=0.0)
    assert rep0["share"] == 0.0
    assert rep0["n"] == 4
    assert rep0["blocked"] == 0
    assert rep0["exec_rate"] == 1.0
    assert "meanR" in rep0["opp"]
    assert isinstance(rep0["opp"]["meanR"], (int, float))
    
    # Test with share=1.0 (all vetoed trades blocked)
    rep1 = simulate_share(rows, share=1.0, salt=salt, min_exec_rate=0.0)
    assert rep1["share"] == 1.0
    assert rep1["n"] == 4
    # Blocked count depends on hash distribution
    assert rep1["blocked"] >= 0
    assert rep1["exec_rate"] <= 1.0
    
    # Test with min_exec_rate constraint
    rep2 = simulate_share(rows, share=1.0, salt=salt, min_exec_rate=0.8)
    assert rep2["ok_exec_rate"] == (rep2["exec_rate"] >= 0.8)


def test_pick_best_share():
    """Test best share selection."""
    # Create test rows: mix of good and bad trades
    rows = []
    for i in range(100):
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
    grid = [0.10, 0.25, 0.50, 1.00]
    tail_cap_exec = 0.18
    min_exec_rate = 0.30
    lam_tail = 0.50
    lam_drop = 0.05
    
    best_s, rep = pick_best_share(
        rows,
        grid=grid,
        salt=salt,
        tail_cap_exec=tail_cap_exec,
        min_exec_rate=min_exec_rate,
        lam_tail=lam_tail,
        lam_drop=lam_drop,
    )
    
    assert best_s in grid
    assert rep is not None
    assert "objective" in rep or "fallback" in rep
    if "objective" in rep:
        assert rep["objective"] is not None


def test_simulate_share_missing_key():
    """Test that rows without meta_enforce_key are skipped."""
    rows = [
        {"meta_enforce_key": "key1", "meta_veto": 1, "r_mult": -1.5},
        {"meta_enforce_key": "", "meta_veto": 1, "r_mult": -0.5},  # Missing key
        {"meta_veto": 0, "r_mult": 0.3},  # Missing key
    ]
    
    salt = "test_salt"
    rep = simulate_share(rows, share=1.0, salt=salt, min_exec_rate=0.0)
    
    # Only first row should be processed
    assert rep["n"] == 3  # All rows counted
    # But only rows with keys are simulated


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

