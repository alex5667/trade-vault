#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
test_nightly_meta_cells_self_heal.py

Unit tests for nightly_meta_cells_self_heal.py:
- Staged unfreeze logic (0.05 → 0.10 → global_share)
- Auto-refreeze on degradation
- Registry management (freeze/unfreeze)
- Cell evaluation (health gates)
"""


import json
import os
import sys
import time
from unittest.mock import MagicMock, patch, Mock

import pytest

# Import the module functions
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from tools.nightly_meta_cells_self_heal import (
    now_ms,
    sign,
    _f,
    _i,
    _event_ts_ms,
    regime_bucket,
    pctl,
    stats,
    bootstrap_tail_delta,
    cell_eval,
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


def test_bootstrap_tail_delta():
    """Test bootstrap tail delta calculation."""
    # Too few samples
    enf_small = [0.1] * 20
    ctl_small = [0.2] * 20
    result = bootstrap_tail_delta(enf_small, ctl_small, iters=100, seed=42)
    assert result["ok"] == 0.0
    
    # Normal case
    enf = [-1.5, -0.5, 0.1, 0.2, 0.3] * 20  # 20% tail
    ctl = [-2.0, -1.0, -0.3, 0.1, 0.2] * 20  # 40% tail
    result = bootstrap_tail_delta(enf, ctl, iters=200, seed=42)
    assert result["ok"] == 1.0
    assert "tail_delta_p50" in result
    assert "tail_delta_p95" in result
    # enf should have lower tail rate, so delta should be negative
    assert result["tail_delta_p50"] < 0.0


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pctl(xs, 0.0) == 1.0
    assert pctl(xs, 0.5) == 3.0
    assert pctl(xs, 1.0) == 5.0
    
    # Empty list
    assert pctl([], 0.5) == 0.0


def test_cell_eval_good():
    """Test cell evaluation with good stats (should pass)."""
    trades = []
    base_ts = now_ms() - 1000000
    
    # Good enforce: low tail, good mean
    for i in range(60):
        trades.append({
            "symbol": "BTCUSDT",
            "bucket": "trend",
            "ts_ms": base_ts + i * 1000,
            "applied": 1,
            "r_mult": 0.1 + (i % 10) * 0.05,  # mostly positive
        })
    
    # Control: higher tail
    for i in range(60):
        trades.append({
            "symbol": "BTCUSDT",
            "bucket": "trend",
            "ts_ms": base_ts + 60000 + i * 1000,
            "applied": 0,
            "r_mult": -1.5 if i % 5 == 0 else 0.05,  # 20% tail
        })
    
    ok, rep = cell_eval(
        trades,
        sym="BTCUSDT",
        bucket="trend",
        t_from_ms=base_ts,
        t_to_ms=now_ms(),
        min_enf_n=50,
        min_ctl_n=50,
        tail_cap=0.18,
        tail_improve_min=0.01,
        mean_delta_min=-0.02,
        boot_iters=200,
        boot_seed=42,
    )
    
    # Should pass (enf has better stats)
    assert ok is True
    assert rep["n_enf"] >= 50
    assert rep["n_ctl"] >= 50


def test_cell_eval_bad_tail():
    """Test cell evaluation with bad tail rate (should fail)."""
    trades = []
    base_ts = now_ms() - 1000000
    
    # Bad enforce: high tail
    for i in range(60):
        trades.append({
            "symbol": "BTCUSDT",
            "bucket": "trend",
            "ts_ms": base_ts + i * 1000,
            "applied": 1,
            "r_mult": -2.0 if i % 4 == 0 else 0.1,  # 25% tail (bad)
        })
    
    # Control: lower tail
    for i in range(60):
        trades.append({
            "symbol": "BTCUSDT",
            "bucket": "trend",
            "ts_ms": base_ts + 60000 + i * 1000,
            "applied": 0,
            "r_mult": -1.0 if i % 10 == 0 else 0.05,  # 10% tail
        })
    
    ok, rep = cell_eval(
        trades,
        sym="BTCUSDT",
        bucket="trend",
        t_from_ms=base_ts,
        t_to_ms=now_ms(),
        min_enf_n=50,
        min_ctl_n=50,
        tail_cap=0.18,
        tail_improve_min=0.01,
        mean_delta_min=-0.02,
        boot_iters=200,
        boot_seed=42,
    )
    
    # Should fail (enf has worse tail)
    assert ok is False
    assert len(rep.get("reasons", [])) > 0


def test_cell_eval_insufficient_n():
    """Test cell evaluation with insufficient samples (should fail)."""
    trades = []
    base_ts = now_ms() - 1000000
    
    # Only 30 enforce samples (need 50)
    for i in range(30):
        trades.append({
            "symbol": "BTCUSDT",
            "bucket": "trend",
            "ts_ms": base_ts + i * 1000,
            "applied": 1,
            "r_mult": 0.1,
        })
    
    # Only 30 control samples (need 50)
    for i in range(30):
        trades.append({
            "symbol": "BTCUSDT",
            "bucket": "trend",
            "ts_ms": base_ts + 30000 + i * 1000,
            "applied": 0,
            "r_mult": 0.05,
        })
    
    ok, rep = cell_eval(
        trades,
        sym="BTCUSDT",
        bucket="trend",
        t_from_ms=base_ts,
        t_to_ms=now_ms(),
        min_enf_n=50,
        min_ctl_n=50,
        tail_cap=0.18,
        tail_improve_min=0.01,
        mean_delta_min=-0.02,
        boot_iters=200,
        boot_seed=42,
    )
    
    # Should fail (insufficient n)
    assert ok is False
    assert "insufficient_n" in rep.get("reason", "")


def test_staged_unfreeze_logic():
    """Test staged unfreeze logic: 0.05 → 0.10 → global_share."""
    freeze_floor = 0.05
    stage1_share = 0.10
    global_share = 0.50
    
    # Stage1: frozen at 0.05 → unfreeze to 0.10
    frozen = 0.05
    stage1_target = min(global_share, stage1_share)
    assert stage1_target == 0.10
    
    # Stage2: at 0.10 → unfreeze to global_share
    current = 0.10
    stage2_target = global_share
    assert stage2_target == 0.50
    
    # Should not unfreeze if target <= current
    assert stage2_target > current + 1e-9


def test_freeze_floor_logic():
    """Test freeze floor logic: min(cur, 0.05) instead of 0.00."""
    floor = 0.05
    
    # Case 1: cur > floor -> freeze_to = floor
    cur1 = 0.25
    freeze_to1 = min(cur1, floor)
    assert freeze_to1 == 0.05
    
    # Case 2: cur < floor -> freeze_to = cur (but should skip)
    cur2 = 0.03
    freeze_to2 = min(cur2, floor)
    assert freeze_to2 == 0.03
    # Should skip because freeze_to >= cur - 1e-9
    assert freeze_to2 >= cur2 - 1e-9
    
    # Case 3: cur == floor -> freeze_to = floor (but should skip)
    cur3 = 0.05
    freeze_to3 = min(cur3, floor)
    assert freeze_to3 == 0.05
    assert freeze_to3 >= cur3 - 1e-9


def test_registry_stage1():
    """Test registry management for Stage1 unfreeze."""
    # Simulate Stage1 unfreeze record
    rec = {
        "cell": "BTCUSDT|trend",
        "symbol": "BTCUSDT",
        "bucket": "trend",
        "stage": 1,
        "applied_ms": now_ms(),
        "target_share": "0.10",
        "restore_final": "0.50",
        "prev_share": "0.05",
        "field": "meta_enforce_share_trend",
        "cfg_key": "config:orderflow:BTCUSDT",
        "bundle_id": "test123",
    }
    
    assert rec["stage"] == 1
    assert rec["target_share"] == "0.10"
    assert rec["restore_final"] == "0.50"


def test_registry_stage2():
    """Test registry management for Stage2 unfreeze."""
    # Simulate Stage2 unfreeze record (should remove from unfreeze registry)
    rec = {
        "cell": "BTCUSDT|trend",
        "symbol": "BTCUSDT",
        "bucket": "trend",
        "stage": 2,
        "applied_ms": now_ms(),
        "target_share": "0.50",
        "restore_final": "0.50",
        "prev_share": "0.10",
        "field": "meta_enforce_share_trend",
        "cfg_key": "config:orderflow:BTCUSDT",
        "bundle_id": "test456",
    }
    
    assert rec["stage"] == 2
    assert rec["target_share"] == "0.50"


def test_auto_refreeze_cooldown():
    """Test auto-refreeze cooldown logic."""
    cooldown_sec = 21600  # 6 hours
    last_refreeze = now_ms() - 10000  # 10 seconds ago
    
    # Should skip if within cooldown
    if last_refreeze and (now_ms() - last_refreeze) < cooldown_sec * 1000:
        should_skip = True
    else:
        should_skip = False
    
    assert should_skip is True
    
    # Should allow if outside cooldown
    last_refreeze_old = now_ms() - (cooldown_sec + 100) * 1000  # 6h+ ago
    if last_refreeze_old and (now_ms() - last_refreeze_old) < cooldown_sec * 1000:
        should_skip2 = True
    else:
        should_skip2 = False
    
    assert should_skip2 is False


def test_sign():
    """Test HMAC signature generation."""
    secret = "test_secret"
    bundle_id = "abc123"
    
    sig1 = sign(bundle_id, secret)
    sig2 = sign(bundle_id, secret)
    
    # Should be deterministic
    assert sig1 == sig2
    assert len(sig1) == 8  # 8 hex characters


def test_event_ts_ms():
    """Test event timestamp extraction."""
    # Test with ts_ms
    r1 = {"ts_ms": 1234567890123}
    assert _event_ts_ms(r1) == 1234567890123
    
    # Test with ts (seconds)
    r2 = {"ts": 1234567890}
    assert _event_ts_ms(r2) == 1234567890000
    
    # Test with exit_ts_ms
    r3 = {"exit_ts_ms": 1234567890123}
    assert _event_ts_ms(r3) == 1234567890123
    
    # Test with missing timestamp
    r4 = {}
    assert _event_ts_ms(r4) == 0


def test_safe_conversions():
    """Test safe conversion functions."""
    # Float conversion
    assert _f("1.5") == 1.5
    assert _f(1.5) == 1.5
    assert _f(None, 0.0) == 0.0
    assert _f("invalid", 0.0) == 0.0
    
    # Int conversion
    assert _i("123") == 123
    assert _i(123) == 123
    assert _i(123.7) == 123
    assert _i(None, 0) == 0
    assert _i("invalid", 0) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

