from __future__ import annotations

import pytest

from tools.ml_metrics_agg import agg_exec_risk, agg_health_ml_confirm, agg_outcomes


def test_agg_outcomes_utility_metrics():
    """Test that agg_outcomes includes meanR, tail_rate, ES05."""
    rows = [
        {"y": "1", "r_mult": "0.5", "p_edge": "0.7", "brier": "0.09"},
        {"y": "0", "r_mult": "-1.5", "p_edge": "0.6", "brier": "0.36"},
        {"y": "1", "r_mult": "1.0", "p_edge": "0.8", "brier": "0.04"},
        {"y": "0", "r_mult": "-2.0", "p_edge": "0.5", "brier": "0.25"},
        {"y": "1", "r_mult": "0.3", "p_edge": "0.7", "brier": "0.09"},
    ]
    result = agg_outcomes(rows)
    assert "meanR" in result
    assert "tail_rate" in result
    assert "es05" in result
    assert result["n"] == 5
    # tail_rate: 2 out of 5 have r_mult <= -1.0
    assert result["tail_rate"] == pytest.approx(0.4, abs=0.01)


def test_agg_exec_risk():
    """Test agg_exec_risk computes exec_p90."""
    rows = [
        {"exec_risk_norm": "0.5"},
        {"exec_risk_norm": "0.7"},
        {"exec_risk_norm": "0.9"},
        {"exec_risk_norm": "0.8"},
        {"exec_risk_norm": "0.6"},
    ]
    result = agg_exec_risk(rows)
    assert result["n"] == 5
    assert "exec_p90" in result
    assert result["exec_p90"] > 0.0


def test_agg_health_ml_confirm():
    """Test agg_health_ml_confirm."""
    rows = [
        {"missing": "0", "err": "", "latency_ms": "2.0", "p_edge": "0.6"},
        {"missing": "1", "err": "error", "latency_ms": "5.0", "p_edge": "0.7"},
        {"missing": "0", "err": "", "latency_ms": "3.0", "p_edge": "0.5"},
    ]
    result = agg_health_ml_confirm(rows)
    assert result["n"] == 3
    assert result["missing_rate"] == pytest.approx(1.0 / 3.0, abs=0.01)
    assert result["err_rate"] == pytest.approx(1.0 / 3.0, abs=0.01)

