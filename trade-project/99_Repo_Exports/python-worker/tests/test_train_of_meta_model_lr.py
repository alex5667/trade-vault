# python-worker/tests/test_train_of_meta_model_lr.py
"""
Unit tests for train_of_meta_model_lr.py
"""
import json
import os
import tempfile

import pytest

try:
    import numpy as np

    from tools.train_of_meta_model_lr import (
        build_xy,
        iter_ndjson,
        main,
    )
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


@pytest.mark.skipif(not SKLEARN_AVAILABLE, reason="scikit-learn not available")
def test_build_xy():
    """Test feature matrix building."""
    rows = [
        {"y": 1, "base_score": 0.8, "exec_risk_norm": 0.3, "have": 2, "need": 2},
        {"y": 0, "base_score": 0.5, "exec_risk_norm": 0.8, "have": 1, "need": 2},
        {"y": 1, "base_score": 0.9, "exec_risk_norm": 0.2, "have": 3, "need": 2},
    ]

    feat = ["base_score", "exec_risk_norm", "have", "need"]
    X, y, w = build_xy(rows, feat)

    assert X.shape == (3, 4)
    assert y.shape == (3,)
    assert w.shape == (3,)
    # Rows without ips_weight default to 1.0.
    assert w.tolist() == [1.0, 1.0, 1.0]
    assert y[0] == 1
    assert y[1] == 0
    assert X[0, 0] == 0.8  # base_score
    assert X[0, 1] == 0.3  # exec_risk_norm


@pytest.mark.skipif(not SKLEARN_AVAILABLE, reason="scikit-learn not available")
def test_main_end_to_end():
    """Test end-to-end training."""
    # Create dataset file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        # Generate synthetic dataset with enough samples
        for i in range(400):
            row = {
                "y": 1 if i % 3 != 0 else 0,
                "base_score": 0.7 + (i % 10) * 0.02,
                "exec_risk_norm": 0.3 + (i % 5) * 0.1,
                "exec_risk_bps": 10.0 + (i % 5) * 2.0,
                "have": 2 if i % 2 == 0 else 1,
                "need": 2,
                "ok_soft": 0,
                "leg_ofi_leg": 1 if i % 2 == 0 else 0,
                "leg_fp_edge_absorb": 1 if i % 3 == 0 else 0,
                "leg_obi_stable": 1 if i % 2 == 0 else 0,
                "leg_iceberg_strict": 0,
                "leg_abs_lvl_ok": 1 if i % 4 == 0 else 0,
                "leg_reclaim_recent": 1 if i % 3 == 0 else 0,
                "leg_weak_progress": 1 if i % 2 == 0 else 0,
                "leg_sweep_recent": 1 if i % 2 == 0 else 0,
            }
            f.write(json.dumps(row) + "\n")
        dataset_path = f.name

    # Create output files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        model_path = f.name

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        report_path = f.name

    try:
        import sys
        old_argv = sys.argv
        sys.argv = [
            "test",
            "--dataset", dataset_path,
            "--out-model", model_path,
            "--out-report", report_path,
            "--test-size", "0.25",
            "--seed", "42",
            "--threshold", "0.5",
        ]

        try:
            main()
        finally:
            sys.argv = old_argv

        # Check model file
        with open(model_path) as f:
            model = json.load(f)
            assert "kind" in model
            assert model["kind"] == "logreg_v1"
            assert "features" in model
            assert "intercept" in model
            assert "coef" in model
            assert len(model["coef"]) == len(model["features"])
            assert model["threshold"] == 0.5

        # Check report file
        with open(report_path) as f:
            report = json.load(f)
            assert "n" in report
            assert report["n"] == 400
            assert "auc" in report
            assert "precision" in report
            assert "recall" in report
            assert "f1" in report
    finally:
        os.unlink(dataset_path)
        if os.path.exists(model_path):
            os.unlink(model_path)
        if os.path.exists(report_path):
            os.unlink(report_path)

