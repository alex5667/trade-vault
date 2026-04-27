"""Tests for ml_promotion_ladder helper functions."""

import pytest
from tools.ml_promotion_ladder import pctl, sign, agg_outcomes, agg_health_ml_confirm


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pctl(xs, 0.0) == 1.0
    assert pctl(xs, 0.5) == 3.0
    assert pctl(xs, 1.0) == 5.0
    assert pctl([], 0.5) == 0.0


def test_sign():
    """Test HMAC signature generation."""
    secret = "test_secret"
    bundle_id = "abc123"
    sig = sign(bundle_id, secret)
    assert len(sig) == 8
    assert isinstance(sig, str)
    # Should be deterministic
    assert sign(bundle_id, secret) == sign(bundle_id, secret)


def test_agg_outcomes():
    """Test outcome aggregation."""
    rows = [
        {"y": "1", "brier": "0.20", "r_mult": "1.5"},
        {"y": "0", "brier": "0.25", "r_mult": "0.3"},
        {"y": "1", "brier": "0.18", "r_mult": "2.0", "brier_chal": "0.15", "r_mult": "2.0"},
    ]
    result = agg_outcomes(rows)
    assert result["n"] == 3
    assert result["win_rate"] == pytest.approx(2.0 / 3.0, abs=0.01)
    assert result["brier"] == pytest.approx((0.20 + 0.25 + 0.18) / 3.0, abs=0.01)
    assert "brier_ch" in result
    assert result["n_ch"] == 1


def test_agg_health_ml_confirm():
    """Test health metrics aggregation."""
    rows = [
        {"missing": "0", "err": "", "latency_ms": "5.0", "p_edge": "0.6"},
        {"missing": "1", "err": "test", "latency_ms": "10.0", "p_edge": "0.7"},
        {"missing": "0", "err": "", "latency_ms": "3.0", "p_edge": "0.5"},
    ]
    result = agg_health_ml_confirm(rows)
    assert result["n"] == 3
    assert result["missing_rate"] == pytest.approx(1.0 / 3.0, abs=0.01)
    assert result["err_rate"] == pytest.approx(1.0 / 3.0, abs=0.01)
    assert result["lat_p99_ms"] > 0.0
    assert result["p_edge_p50"] > 0.0
    
    # Empty rows
    result_empty = agg_health_ml_confirm([])
    assert result_empty["n"] == 0

