"""Tests for ml_metrics_agg_v3 module."""
from __future__ import annotations

from tools.ml_metrics_agg_v3 import (
    agg_health_ml_confirm,
    agg_selected,
    pick_threshold,
    pctl,
)


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pctl(xs, 0.0) == 1.0
    assert pctl(xs, 0.5) == 3.0
    assert pctl(xs, 1.0) == 5.0
    assert pctl([], 0.5) == 0.0


def test_agg_health_ml_confirm():
    """Test health aggregation."""
    rows = [
        {"missing": "0", "err": "", "latency_ms": "1.0"},
        {"missing": "1", "err": "", "latency_ms": "2.0"},
        {"missing": "0", "err": "error", "latency_ms": "3.0"},
        {"missing": "0", "err": "", "latency_ms": "4.0"},
    ]
    h = agg_health_ml_confirm(rows)
    assert h["n"] == 4
    assert h["missing_rate"] == 0.25
    assert h["err_rate"] == 0.25
    assert h["lat_p99_ms"] >= 3.0

    # empty
    h_empty = agg_health_ml_confirm([])
    assert h_empty["n"] == 0


def test_agg_selected():
    """Test selected rows aggregation."""
    rows = [
        {"p_edge": "0.50", "r_mult": "0.5", "y": "1"},
        {"p_edge": "0.60", "r_mult": "-1.5", "y": "0"},
        {"p_edge": "0.55", "r_mult": "1.0", "y": "1"},
        {"p_edge": "0.45", "r_mult": "0.3", "y": "0"},  # below threshold
    ]
    s = agg_selected(rows, 0.50)
    assert s["n"] == 3
    assert s["meanR"] == (0.5 + (-1.5) + 1.0) / 3.0
    assert s["tail_rate"] == 1.0 / 3.0  # one with r_mult <= -1.0
    assert s["win_rate"] == 2.0 / 3.0

    # no matches
    s_empty = agg_selected(rows, 0.70)
    assert s_empty["n"] == 0


def test_pick_threshold():
    """Test threshold picking."""
    rows_short = [
        {"p_edge": "0.50", "r_mult": "0.5", "y": "1"},
        {"p_edge": "0.55", "r_mult": "0.3", "y": "1"},
        {"p_edge": "0.60", "r_mult": "-0.5", "y": "0"},
    ] * 30  # 90 rows
    rows_long = rows_short * 4  # 360 rows

    grid = [0.45, 0.50, 0.55, 0.60]
    t, s_stat, l_stat = pick_threshold(
        rows_short,
        rows_long,
        grid=grid,
        min_n_short=80,
        min_n_long=300,
        tail_max=0.5,
        meanR_min=-1.0,
        es05_min=-2.0,
    )
    assert t > 0.0
    assert s_stat["n"] >= 80
    assert l_stat["n"] >= 300

