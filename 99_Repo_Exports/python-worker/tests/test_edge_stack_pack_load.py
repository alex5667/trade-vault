#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
test_edge_stack_pack_load.py

Unit test for edge_stack_v1 model pack loading and prediction.
Tests that model.joblib can be loaded and predict_proba works on synthetic data.
"""


import json
import os
import tempfile
from pathlib import Path

import numpy as np
import pytest

try:
    import joblib
except ImportError:
    joblib = None
    pytest.skip("joblib not available", allow_module_level=True)

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline

try:
    from sklearn.ensemble import HistGradientBoostingClassifier
except ImportError:
    HistGradientBoostingClassifier = None

from services.ml_calibration import PlattLogitCalibrator


def make_lr() -> Pipeline:
    """Create LR pipeline."""
    return Pipeline([
        ("scaler", RobustScaler(with_centering=True, with_scaling=True, quantile_range=(25.0, 75.0))),
        ("lr", LogisticRegression(
            C=1.0,
            solver="lbfgs",
            max_iter=500,
            class_weight="balanced",
            random_state=42
        ))
    ])


def make_gbdt():
    """Create GBDT model."""
    if HistGradientBoostingClassifier is None:
        pytest.skip("HistGradientBoostingClassifier not available")
    return HistGradientBoostingClassifier(
        max_depth=6,
        learning_rate=0.05,
        max_iter=400,
        l2_regularization=1e-3,
        random_state=42
    )


def test_edge_stack_pack_load():
    """Test that edge_stack_v1 model pack can be loaded and used."""
    # Create synthetic data
    n_samples = 100
    n_features = 10
    X = np.random.randn(n_samples, n_features).astype(np.float32)
    y = (np.random.rand(n_samples) > 0.5).astype(int)
    
    # Train base models
    lr = make_lr()
    gbdt = make_gbdt()
    lr.fit(X, y)
    gbdt.fit(X, y)
    
    # Get base predictions
    p_lr = lr.predict_proba(X)[:, 1]
    p_gbdt = gbdt.predict_proba(X)[:, 1]
    
    # Train meta model
    Z = np.stack([p_lr, p_gbdt], axis=1).astype(np.float32)
    meta = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=500,
        class_weight="balanced",
        random_state=42
    )
    meta.fit(Z, y)
    
    # Create calibrator
    p_meta = meta.predict_proba(Z)[:, 1]
    cal = PlattLogitCalibrator(a=1.0, b=0.0)
    
    # Create model pack
    feature_cols = [f"f_{i}" for i in range(n_features)]
    pack = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "feature_cols": feature_cols,
        "lr": lr,
        "gbdt": gbdt,
        "meta": meta,
        "calibrator": cal.to_dict(),
    }
    
    # Save and load
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "model.joblib")
        joblib.dump(pack, model_path, compress=3)
        
        # Load model
        loaded_pack = joblib.load(model_path)
        
        # Validate structure
        assert isinstance(loaded_pack, dict)
        assert loaded_pack.get("kind") == "edge_stack_v1"
        assert loaded_pack.get("schema_version") == 1
        assert "feature_cols" in loaded_pack
        assert "lr" in loaded_pack
        assert "gbdt" in loaded_pack
        assert "meta" in loaded_pack
        assert "calibrator" in loaded_pack
        
        # Validate feature_cols
        assert len(loaded_pack["feature_cols"]) == n_features
        assert loaded_pack["feature_cols"] == feature_cols
        
        # Test prediction
        X_test = np.random.randn(5, n_features).astype(np.float32)
        
        # Base predictions
        p_lr_test = loaded_pack["lr"].predict_proba(X_test)[:, 1]
        p_gbdt_test = loaded_pack["gbdt"].predict_proba(X_test)[:, 1]
        
        # Meta prediction
        Z_test = np.stack([p_lr_test, p_gbdt_test], axis=1).astype(np.float32)
        p_meta_test = loaded_pack["meta"].predict_proba(Z_test)[:, 1]
        
        # Calibration
        cal_loaded = PlattLogitCalibrator.from_dict(loaded_pack["calibrator"])
        p_cal_test = np.array([cal_loaded.apply_one(float(p)) for p in p_meta_test])
        
        # Validate outputs
        assert len(p_lr_test) == 5
        assert len(p_gbdt_test) == 5
        assert len(p_meta_test) == 5
        assert len(p_cal_test) == 5
        
        assert np.all((p_lr_test >= 0) & (p_lr_test <= 1))
        assert np.all((p_gbdt_test >= 0) & (p_gbdt_test <= 1))
        assert np.all((p_meta_test >= 0) & (p_meta_test <= 1))
        assert np.all((p_cal_test >= 0) & (p_cal_test <= 1))
        
        assert np.all(np.isfinite(p_lr_test))
        assert np.all(np.isfinite(p_gbdt_test))
        assert np.all(np.isfinite(p_meta_test))
        assert np.all(np.isfinite(p_cal_test))


def test_edge_stack_pack_missing_keys():
    """Test that missing required keys are detected."""
    pack_incomplete = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "feature_cols": ["f1", "f2"],
        # Missing: lr, gbdt, meta
    }
    
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "model.joblib")
        joblib.dump(pack_incomplete, model_path)
        
        loaded = joblib.load(model_path)
        assert "lr" not in loaded
        assert "gbdt" not in loaded
        assert "meta" not in loaded


if __name__ == "__main__":
    test_edge_stack_pack_load()
    test_edge_stack_pack_missing_keys()
    print("All tests passed!")







