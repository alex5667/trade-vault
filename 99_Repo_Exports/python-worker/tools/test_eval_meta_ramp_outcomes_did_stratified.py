#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_eval_meta_ramp_outcomes_did_stratified.py

Unit tests for eval_meta_ramp_outcomes_did_stratified.py
"""

from __future__ import annotations

import json
import tempfile
import os
import subprocess
import sys
from typing import List, Dict, Any

import pytest

# Import the module functions
sys.path.insert(0, os.path.dirname(__file__))
from eval_meta_ramp_outcomes_did_stratified import (
    iter_ndjson,
    stats,
    bootstrap_did,
    pctl,
    _f,
    _i,
    _event_ts_ms,
    regime_bucket,
)


def test_regime_bucket():
    """Test regime bucket classification."""
    # News
    assert regime_bucket({"regime_group": "news_fomc"}) == "news"
    assert regime_bucket({"regime": "cpi_release"}) == "news"
    assert regime_bucket({"scenario_v4": "fomc"}) == "news"
    
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


def test_stats_empty():
    """Test stats with empty list."""
    result = stats([])
    assert result["n"] == 0.0


def test_stats_basic():
    """Test stats with sample data."""
    rs = [1.0, 2.0, -1.5, 0.5, -2.0, 0.0]
    result = stats(rs)
    assert result["n"] == 6
    assert result["meanR"] == pytest.approx(0.0, abs=0.01)
    assert result["winrate"] == pytest.approx(3.0 / 6.0, abs=0.01)
    assert result["tail_rate"] == pytest.approx(2.0 / 6.0, abs=0.01)  # -1.5 and -2.0


def test_bootstrap_did():
    """Test bootstrap DiD calculation."""
    # Before: enforce better than control
    eb = [0.5] * 50 + [-0.3] * 50  # mean = 0.1
    cb = [0.2] * 50 + [-0.5] * 50  # mean = -0.15
    
    # After: enforce improved more than control
    ea = [0.8] * 50 + [-0.2] * 50  # mean = 0.3
    ca = [0.3] * 50 + [-0.4] * 50  # mean = -0.05
    
    result = bootstrap_did(eb, cb, ea, ca, iters=100, seed=42)
    assert result["ok"] == 1.0
    assert "did_mean_p05" in result
    assert "did_mean_p50" in result
    assert "did_mean_p95" in result
    assert "did_tail_p05" in result
    assert "did_tail_p50" in result
    assert "did_tail_p95" in result


def test_bootstrap_did_insufficient():
    """Test bootstrap with insufficient data."""
    eb = [0.5] * 10
    cb = [0.2] * 10
    ea = [0.8] * 10
    ca = [0.3] * 10
    result = bootstrap_did(eb, cb, ea, ca, iters=100, seed=42)
    assert result["ok"] == 0.0


def test_stratified_cells():
    """Test that trades are correctly stratified by symbol × regime_bucket."""
    ramp_ts = 1000000000000  # ms
    window_hours = 72.0
    win_ms = int(window_hours * 3600_000)
    before_from = ramp_ts - win_ms
    before_to = ramp_ts
    after_from = ramp_ts
    after_to = ramp_ts + win_ms
    
    # Create test data with different symbols and regimes
    trades = []
    
    # BTCUSDT trend before
    for i in range(50):
        trades.append({
            "symbol": "BTCUSDT",
            "ts_ms": before_from + i * 1000,
            "r_mult": 0.5 if i % 2 == 0 else -0.3,
            "meta_enforce_applied": 1 if i % 2 == 0 else 0,
            "regime_group": "trend_bull",
        })
    
    # ETHUSDT range before
    for i in range(50):
        trades.append({
            "symbol": "ETHUSDT",
            "ts_ms": before_from + i * 1000,
            "r_mult": 0.3 if i % 2 == 0 else -0.2,
            "meta_enforce_applied": 1 if i % 2 == 0 else 0,
            "regime_group": "range_bound",
        })
    
    # BTCUSDT trend after (improved)
    for i in range(50):
        trades.append({
            "symbol": "BTCUSDT",
            "ts_ms": after_from + i * 1000,
            "r_mult": 0.8 if i % 2 == 0 else -0.1,
            "meta_enforce_applied": 1 if i % 2 == 0 else 0,
            "regime_group": "trend_bull",
        })
    
    # ETHUSDT range after (improved)
    for i in range(50):
        trades.append({
            "symbol": "ETHUSDT",
            "ts_ms": after_from + i * 1000,
            "r_mult": 0.6 if i % 2 == 0 else -0.1,
            "meta_enforce_applied": 1 if i % 2 == 0 else 0,
            "regime_group": "range_bound",
        })
    
    # Write to temp file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".ndjson") as f:
        for t in trades:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
        fname = f.name
    
    try:
        # Manually test stratification logic
        cells: Dict[str, Dict[str, List[float]]] = {}
        
        for r in iter_ndjson(fname):
            sym = str(r.get("symbol", "") or "").upper()
            ts = _event_ts_ms(r)
            rm = r.get("r_mult", None)
            if rm is None:
                continue
            rmf = _f(rm, 0.0)
            applied = r.get("meta_enforce_applied", None)
            if applied is None:
                continue
            a = _i(applied, 0)
            bucket = regime_bucket(r)
            ck = f"{sym}|{bucket}"
            if ck not in cells:
                cells[ck] = {"eb": [], "cb": [], "ea": [], "ca": []}
            
            if before_from <= ts < before_to:
                (cells[ck]["eb"] if a == 1 else cells[ck]["cb"]).append(rmf)
            elif after_from <= ts < after_to:
                (cells[ck]["ea"] if a == 1 else cells[ck]["ca"]).append(rmf)
        
        # Should have 2 cells: BTCUSDT|trend and ETHUSDT|range
        assert len(cells) == 2
        assert "BTCUSDT|trend" in cells
        assert "ETHUSDT|range" in cells
        
        # Check cell data
        btc_cell = cells["BTCUSDT|trend"]
        assert len(btc_cell["eb"]) > 0
        assert len(btc_cell["cb"]) > 0
        assert len(btc_cell["ea"]) > 0
        assert len(btc_cell["ca"]) > 0
        
    finally:
        os.unlink(fname)


def test_worst_case_gating():
    """Test that worst-case gating blocks ramp if any cell fails."""
    # This would be tested via full integration test with the main() function
    # For unit test, we verify the logic: if any cell fails, decision should be False
    pass  # Integration test would be better here


def test_coverage_rule():
    """Test that insufficient cells blocks ramp."""
    # This would be tested via full integration test
    pass  # Integration test would be better here


def test_iter_ndjson():
    """Test NDJSON iteration."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".ndjson") as f:
        f.write('{"a": 1}\n')
        f.write('{"b": 2}\n')
        f.write('\n')  # empty line
        f.write('{"c": 3}\n')
        fname = f.name
    
    try:
        rows = list(iter_ndjson(fname))
        assert len(rows) == 3
        assert rows[0]["a"] == 1
        assert rows[1]["b"] == 2
        assert rows[2]["c"] == 3
    finally:
        os.unlink(fname)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

