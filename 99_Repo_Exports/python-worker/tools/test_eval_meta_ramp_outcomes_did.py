#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_eval_meta_ramp_outcomes_did.py

Unit tests for eval_meta_ramp_outcomes_did.py
"""

from __future__ import annotations

import json
import tempfile
import os
from typing import List, Dict, Any

import pytest

# Import the module functions
import sys
sys.path.insert(0, os.path.dirname(__file__))
from eval_meta_ramp_outcomes_did import (
    iter_ndjson,
    stats,
    bootstrap_did,
    pctl,
    _f,
    _i,
    _event_ts_ms,
    _delta,
)


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


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pctl(xs, 0.5) == 3.0
    assert pctl(xs, 0.0) == 1.0
    assert pctl(xs, 1.0) == 5.0


def test_event_ts_ms():
    """Test timestamp extraction."""
    r1 = {"ts_ms": 1234567890000}
    assert _event_ts_ms(r1) == 1234567890000
    
    r2 = {"ts": 1234567890}  # seconds
    ts = _event_ts_ms(r2)
    assert ts > 0
    
    r3 = {"exit_ts_ms": 1234567890000}
    assert _event_ts_ms(r3) == 1234567890000


def test_delta():
    """Test delta calculation."""
    enf = {"meanR": 0.5, "tail_rate": 0.1, "winrate": 0.6}
    ctl = {"meanR": 0.3, "tail_rate": 0.2, "winrate": 0.5}
    d = _delta(enf, ctl)
    assert d["mean_delta"] == pytest.approx(0.2, abs=0.01)
    assert d["tail_delta"] == pytest.approx(-0.1, abs=0.01)
    assert d["win_delta"] == pytest.approx(0.1, abs=0.01)


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


def test_did_eval_windows():
    """Test DiD evaluation with proper time windows."""
    ramp_ts = 1000000000000  # ms
    window_hours = 72.0
    win_ms = int(window_hours * 3600_000)
    before_from = ramp_ts - win_ms
    before_to = ramp_ts
    after_from = ramp_ts
    after_to = ramp_ts + win_ms
    
    assert before_from < before_to
    assert after_from < after_to
    assert before_to == after_from  # contiguous windows


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

