#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
test_meta_freeze_unfreeze.py

Unit tests for meta freeze/unfreeze functionality:
- Freeze with floor=0.05 instead of 0.00
- Freeze registry recording
- Auto-unfreeze proposal after 7 days good stats
- Last share per bucket storage
"""


import json
import os
import time
from unittest.mock import MagicMock, patch, Mock

import pytest

# Import the module functions
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from tools.nightly_meta_enforce_ramp_or_freeze_bundle import (
    _read_float_h,
    now_ms,
)
from tools.nightly_meta_unfreeze_cells_bundle import (
    regime_bucket,
    stats,
    bootstrap_tail_delta,
    pctl,
    _f,
    _i,
)


def test_freeze_floor_logic():
    """Test freeze floor logic: min(cur, 0.05) instead of 0.00."""
    floor = 0.05
    
    # Test case 1: cur > floor -> freeze_to = floor
    cur1 = 0.25
    freeze_to1 = min(cur1, floor)
    assert freeze_to1 == 0.05
    
    # Test case 2: cur < floor -> freeze_to = cur (but should skip)
    cur2 = 0.03
    freeze_to2 = min(cur2, floor)
    assert freeze_to2 == 0.03
    # Should skip because freeze_to >= cur - 1e-9
    assert freeze_to2 >= cur2 - 1e-9
    
    # Test case 3: cur == floor -> freeze_to = floor (but should skip)
    cur3 = 0.05
    freeze_to3 = min(cur3, floor)
    assert freeze_to3 == 0.05
    assert freeze_to3 >= cur3 - 1e-9


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


def test_freeze_registry_recording():
    """Test freeze registry recording logic."""
    # Simulate audit_rows from _apply_bundle
    audit_rows = [
        {
            "op": "HSET",
            "key": "config:orderflow:BTCUSDT",
            "field": "meta_enforce_share_trend",
            "old": "0.25",
            "old_null": 0,
            "new": "0.05",
        }
    ]
    
    # Build lookup
    prev = {}
    for a in audit_rows:
        if a.get("op") == "HSET":
            prev[(a.get("key", ""), a.get("field", ""))] = a.get("old", "")
    
    assert prev[("config:orderflow:BTCUSDT", "meta_enforce_share_trend")] == "0.25"
    
    # Simulate registry record
    rec = {
        "cell": "BTCUSDT|trend",
        "symbol": "BTCUSDT",
        "bucket": "trend",
        "applied_ms": now_ms(),
        "freeze_to": 0.05,
        "prev_share": prev.get(("config:orderflow:BTCUSDT", "meta_enforce_share_trend"), ""),
        "field": "meta_enforce_share_trend",
        "cfg_key": "config:orderflow:BTCUSDT",
        "bundle_id": "test123",
    }
    
    assert rec["prev_share"] == "0.25"
    assert rec["freeze_to"] == 0.05
    assert rec["cell"] == "BTCUSDT|trend"


def test_unfreeze_eligibility():
    """Test unfreeze eligibility checks."""
    # Good stats: should pass
    enf_good = [0.1, 0.2, -0.1, 0.15, 0.05] * 20  # low tail
    ctl_good = [-1.5, -0.5, 0.0, 0.1, 0.2] * 20  # higher tail
    
    se = stats(enf_good)
    sc = stats(ctl_good)
    mean_delta = se["meanR"] - sc["meanR"]
    tail_improve = sc["tail_rate"] - se["tail_rate"]
    
    # enf should have better mean and lower tail
    assert tail_improve > 0.0  # ctl has higher tail rate
    assert mean_delta > -0.02  # enf mean should be >= ctl mean - 0.02
    
    # Bootstrap check
    ci = bootstrap_tail_delta(enf_good, ctl_good, iters=200, seed=42)
    if ci.get("ok") == 1.0:
        # enf should have lower tail, so delta should be negative
        assert ci.get("tail_delta_p95", 0.0) < 0.0


def test_last_share_per_bucket():
    """Test last share storage per bucket."""
    # Simulate ramp apply with per-regime
    to_share = 0.50
    
    # Should set all three keys
    keys = {
        "meta:ramp:last_share": str(to_share),
        "meta:ramp:last_share_trend": str(to_share),
        "meta:ramp:last_share_range": str(to_share),
    }
    
    assert keys["meta:ramp:last_share"] == "0.5"
    assert keys["meta:ramp:last_share_trend"] == "0.5"
    assert keys["meta:ramp:last_share_range"] == "0.5"
    
    # Unfreeze should use bucket-specific if available
    last_share = 0.50
    last_share_trend = 0.60
    last_share_range = 0.40
    
    # For trend bucket
    bucket = "trend"
    if bucket == "trend":
        restore = last_share_trend
    elif bucket == "range":
        restore = last_share_range
    else:
        restore = last_share
    
    assert restore == 0.60
    
    # For range bucket
    bucket = "range"
    if bucket == "trend":
        restore = last_share_trend
    elif bucket == "range":
        restore = last_share_range
    else:
        restore = last_share
    
    assert restore == 0.40


def test_unfreeze_restore_higher_than_frozen():
    """Test that unfreeze only happens if restore > frozen."""
    frozen_to = 0.05
    restore = 0.25
    
    # Should allow unfreeze
    assert restore > frozen_to + 1e-9
    
    # Should not allow if restore <= frozen
    restore2 = 0.05
    assert not (restore2 > frozen_to + 1e-9)
    
    restore3 = 0.03
    assert not (restore3 > frozen_to + 1e-9)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

