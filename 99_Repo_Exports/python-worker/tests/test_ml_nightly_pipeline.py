from __future__ import annotations
\
"""
Tests for ML nightly pipeline components.
"""


import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest

from core.ml_feature_schema import build_features
from core.ml_metrics_utils import brier_score, ece_score, quantiles, ks_statistic


def test_build_features():
    """Test feature building from payload."""
    payload = {
        "sid": "test_123",
        "symbol": "BTCUSDT",
        "ts_ms": 1609459200000,  # 2021-01-01 00:00:00 UTC
        "direction": "LONG",
        "scenario": "reversal",
        "indicators": {
            "delta_z": 1.5,
            "ofi_z": 0.8,
            "exec_risk_norm": 0.3,
            "obi_stable": 1,
            "iceberg_strict": 0,
        },
        "rule_score": 0.75,
        "rule_have": 3,
        "rule_need": 4,
        "cancel_spike_veto": 0,
    }
    
    feat = build_features(payload)
    assert feat is not None
    assert hasattr(feat, "x")
    assert hasattr(feat, "feature_names")
    assert len(feat.x) > 0
    assert len(feat.feature_names) == len(feat.x)


def test_brier_score():
    """Test Brier score calculation."""
    y = [1, 0, 1, 0, 1]
    p = [0.9, 0.1, 0.8, 0.2, 0.7]
    score = brier_score(y, p)
    assert score >= 0.0
    assert score <= 1.0


def test_ece_score():
    """Test Expected Calibration Error calculation."""
    y = [1, 0, 1, 0, 1, 0, 1, 0, 1, 0]
    p = [0.9, 0.1, 0.8, 0.2, 0.7, 0.3, 0.6, 0.4, 0.5, 0.5]
    ece = ece_score(y, p)
    assert ece >= 0.0
    assert ece <= 1.0


def test_quantiles():
    """Test quantile calculation."""
    xs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    qs = quantiles(xs, [0.5, 0.9, 0.99])
    assert len(qs) == 3
    assert qs[0] == 0.5  # median
    assert qs[1] == 0.9  # p90
    assert qs[2] == 1.0  # p99


def test_ks_statistic():
    """Test KS statistic calculation."""
    a = [0.1, 0.2, 0.3, 0.4, 0.5]
    b = [0.2, 0.3, 0.4, 0.5, 0.6]
    ks = ks_statistic(a, b)
    assert ks >= 0.0
    assert ks <= 1.0


def test_ml_metrics_utils_empty():
    """Test metrics utils with empty inputs."""
    assert brier_score([], []) == 0.0
    assert ece_score([], []) == 0.0
    assert quantiles([], [0.5]) == [0.0]
    assert ks_statistic([], []) == 0.0
    assert ks_statistic([1.0], []) == 0.0
    assert ks_statistic([], [1.0]) == 0.0


