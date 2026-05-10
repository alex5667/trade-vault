#!/usr/bin/env python3
from __future__ import annotations

"""
test_ml_confirm_edge_stack_decide.py

Test that _decide_edge_stack_v1 correctly works with dict-pack models and predict_proba.
"""


import json
import os
import tempfile
from typing import Any

import numpy as np
import pytest

try:
    import joblib
except ImportError:
    joblib = None
    pytest.skip("joblib not available", allow_module_level=True)

try:
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
except ImportError:
    LogisticRegression = None
    HistGradientBoostingClassifier = None
    pytest.skip("sklearn not available", allow_module_level=True)

try:
    from services.ml_confirm import MLConfirmGate, _DictPackModelView
except ImportError as e:
    pytest.skip(f"Required modules not available: {e}", allow_module_level=True)


class MockRedis:
    """Mock Redis for testing."""
    def __init__(self):
        self.data = {}
        self.streams = {}

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, value: str):
        self.data[key] = value

    def xadd(self, stream: str, fields: dict[str, str], **kwargs):
        if stream not in self.streams:
            self.streams[stream] = []
        self.streams[stream].append(fields)


def create_test_edge_stack_model(n_features: int = 10) -> dict[str, Any]:
    """Create a test edge_stack_v1 model pack."""
    # Create synthetic training data
    n_samples = 100
    X = np.random.randn(n_samples, n_features).astype(np.float32)
    y = (np.random.rand(n_samples) > 0.5).astype(int)

    # Train base models
    lr = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=200,
        class_weight="balanced",
        random_state=42
    )
    gbdt = HistGradientBoostingClassifier(
        max_depth=3,
        learning_rate=0.06,
        max_iter=100,
        random_state=42
    )

    lr.fit(X, y)
    gbdt.fit(X, y)

    # Get base predictions for meta training
    p_lr = lr.predict_proba(X)[:, 1]
    p_gbdt = gbdt.predict_proba(X)[:, 1]
    Z = np.stack([p_lr, p_gbdt], axis=1).astype(np.float32)

    # Train meta model
    meta = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=200,
        class_weight="balanced",
        random_state=42
    )
    meta.fit(Z, y)

    # Create feature_cols
    feature_cols = [
        "f_delta_z", "f_ofi_z", "f_obi_z", "f_spread_bps", "f_expected_slippage_bps",
        "f_exec_risk_norm", "f_liq_score",
        "mul_delta_z__liq_score",
        "direction_LONG", "direction_SHORT",
        "scenario_v4_trend", "scenario_v4_range", "scenario_v4_other",
    ]

    return {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "created_ms": 1700000000000,
        "feature_cols": feature_cols,
        "lr": lr,
        "gbdt": gbdt,
        "meta": meta,
    }


def test_decide_edge_stack_v1_with_dict_pack():
    """Test that _decide_edge_stack_v1 works correctly with dict-pack model."""
    # Create test model
    model_pack = create_test_edge_stack_model()

    # Save model to temp file
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "model.joblib")
        joblib.dump(model_pack, model_path)

        # Create champion cfg
        cfg = {
            "schema_version": 1,
            "kind": "edge_stack_v1",
            "run_id": "test_run",
            "created_ms": 1700000000000,
            "model_path": model_path,
            "mode": "SHADOW",
            "enforce_share": 0.0,
            "p_min": 0.55,
            "p_min_by_bucket": {"trend": 0.55, "range": 0.60, "other": 0.52},
            "calibrate_p_edge": False,
        }

        # Setup mock Redis
        r = MockRedis()
        r.set("cfg:ml_confirm:champion", json.dumps(cfg))

        # Create gate
        gate = MLConfirmGate(
            r=r,
            mode="SHADOW",
            fail_policy="OPEN",
            champion_key="cfg:ml_confirm:champion",
            challenger_key="cfg:ml_confirm:challenger"
        )

        # Load cfg and model
        gate._cfg, gate._model = gate._load_cfg_and_model()

        # Test decision with valid indicators
        indicators = {
            "spread_bps": 1.2,
            "expected_slippage_bps": 0.8,
            "exec_risk_norm": 0.15,
            "delta_z": 0.7,
            "obi_z": -0.2,
            "ofi_z": 1.1,
            "liq_score": 0.35,
        }

        dec = gate._decide_edge_stack_v1(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="BUY",
            scenario="trend",
            indicators=indicators,
        )

        # Validate decision
        assert dec is not None
        assert dec.kind == "edge_stack_v1"
        assert dec.mode == "SHADOW"
        assert dec.allow is True  # SHADOW mode always allows
        assert dec.missing == []  # All critical features present
        assert 0.0 <= dec.p_edge <= 1.0
        assert dec.p_min >= 0.0
        assert dec.p_margin == dec.p_edge - dec.p_min
        assert dec.conf >= 0.0


def test_decide_edge_stack_v1_missing_critical_features():
    """Test that missing critical features are detected."""
    model_pack = create_test_edge_stack_model()

    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "model.joblib")
        joblib.dump(model_pack, model_path)

        cfg = {
            "schema_version": 1,
            "kind": "edge_stack_v1",
            "run_id": "test_run",
            "created_ms": 1700000000000,
            "model_path": model_path,
            "mode": "ENFORCE",
            "enforce_share": 1.0,
            "p_min": 0.55,
        }

        r = MockRedis()
        r.set("cfg:ml_confirm:champion", json.dumps(cfg))

        gate = MLConfirmGate(
            r=r,
            mode="ENFORCE",
            fail_policy="CLOSED",
            champion_key="cfg:ml_confirm:champion",
            challenger_key="cfg:ml_confirm:challenger"
        )

        gate._cfg, gate._model = gate._load_cfg_and_model()

        # Missing critical features
        indicators = {
            "delta_z": 0.7,
            # Missing: spread_bps, expected_slippage_bps, exec_risk_norm
        }

        dec = gate._decide_edge_stack_v1(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="BUY",
            scenario="trend",
            indicators=indicators,
        )

        # Should detect missing features
        assert dec is not None
        assert len(dec.missing) > 0
        assert "spread_bps" in dec.missing or "expected_slippage_bps" in dec.missing


def test_dict_pack_model_view():
    """Test that _DictPackModelView correctly exposes model attributes."""
    pack = {
        "feature_cols": ["f_x", "f_y"],
        "feature_transforms": {"x": "log"},
        "robust_scaler": {"x": {"center": 10.0, "scale": 2.0}},
        "session_cfg": {"timezone": "UTC"},
        "spread_bucket_edges": [2.0, 5.0],
        "liq_cfg": {"threshold": 0.5},
    }

    view = _DictPackModelView(pack)

    assert view.feature_cols == ["f_x", "f_y"]
    assert view.feature_transforms == {"x": "log"}
    assert view.robust_scaler == {"x": {"center": 10.0, "scale": 2.0}}
    assert view.session_cfg == {"timezone": "UTC"}
    assert view.spread_bucket_edges == [2.0, 5.0]
    assert view.liq_cfg == {"threshold": 0.5}


if __name__ == "__main__":
    test_decide_edge_stack_v1_with_dict_pack()
    test_decide_edge_stack_v1_missing_critical_features()
    test_dict_pack_model_view()
    print("All tests passed!")
