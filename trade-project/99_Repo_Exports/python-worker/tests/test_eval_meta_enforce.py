"""Unit tests for eval_meta_enforce.py.

Tests evaluation of meta model thresholds for ENFORCE mode.
"""

import json
import os
import tempfile

import pytest

from tools.eval_meta_enforce import (
    iter_ndjson,
    load_lr_model,
    metrics,
    predict_p,
    sigmoid,
)


def test_sigmoid():
    """Test sigmoid function."""
    assert sigmoid(0.0) == pytest.approx(0.5, abs=0.01)
    assert sigmoid(10.0) > 0.99
    assert sigmoid(-10.0) < 0.01


def test_load_lr_model():
    """Test loading logistic regression model."""
    model_data = {
        "kind": "logreg_v1",
        "features": ["feat1", "feat2"],
        "coef": [0.5, -0.3],
        "intercept": 0.1,
        "threshold": 0.5,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(model_data, f)
        fpath = f.name

    try:
        model = load_lr_model(fpath)
        assert model["features"] == ["feat1", "feat2"]
        assert model["coef"] == [0.5, -0.3]
        assert model["intercept"] == 0.1
        assert model["threshold"] == 0.5
    finally:
        os.unlink(fpath)


def test_load_lr_model_invalid_kind():
    """Test loading model with invalid kind."""
    model_data = {
        "kind": "invalid",
        "features": ["feat1"],
        "coef": [0.5],
        "intercept": 0.1,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(model_data, f)
        fpath = f.name

    try:
        with pytest.raises(ValueError, match="unexpected model kind"):
            load_lr_model(fpath)
    finally:
        os.unlink(fpath)


def test_predict_p():
    """Test prediction using logistic regression model."""
    model = {
        "features": ["feat1", "feat2"],
        "coef": [1.0, -0.5],
        "intercept": 0.0,
        "threshold": 0.5,
    }

    row = {"feat1": 1.0, "feat2": 0.0}
    p = predict_p(model, row)
    # score = 0.0 + 1.0 * 1.0 + (-0.5) * 0.0 = 1.0
    # sigmoid(1.0) ≈ 0.73
    assert p > 0.5
    assert p < 1.0

    row2 = {"feat1": -1.0, "feat2": 0.0}
    p2 = predict_p(model, row2)
    # score = 0.0 + 1.0 * (-1.0) + (-0.5) * 0.0 = -1.0
    # sigmoid(-1.0) ≈ 0.27
    assert p2 < 0.5


def test_metrics():
    """Test metrics computation."""
    rows = [
        {"r_mult": 1.5},
        {"r_mult": 0.5},
        {"r_mult": -0.3},
        {"r_mult": -1.5},  # tail loss
        {"r_mult": 2.0},
    ]

    m = metrics(rows)
    assert m["n"] == 5.0
    assert m["meanR"] == pytest.approx((1.5 + 0.5 - 0.3 - 1.5 + 2.0) / 5.0, abs=0.01)
    assert m["tail_rate"] == pytest.approx(1.0 / 5.0, abs=0.01)  # one tail loss


def test_metrics_empty():
    """Test metrics with empty rows."""
    m = metrics([])
    assert m["n"] == 0


def test_iter_ndjson():
    """Test NDJSON iteration."""
    data = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        for row in data:
            f.write(json.dumps(row) + "\n")
        fpath = f.name

    try:
        rows = list(iter_ndjson(fpath))
        assert len(rows) == 2
        assert rows[0] == {"a": 1, "b": 2}
        assert rows[1] == {"a": 3, "b": 4}
    finally:
        os.unlink(fpath)


def test_eval_meta_enforce_integration(tmp_path):
    """Integration test for eval_meta_enforce."""
    # Create model file
    model_data = {
        "kind": "logreg_v1",
        "features": ["score"],
        "coef": [2.0],
        "intercept": -1.0,
        "threshold": 0.5,
    }
    model_path = tmp_path / "model.json"
    with open(model_path, "w") as f:
        json.dump(model_data, f)

    # Create dataset file
    dataset_path = tmp_path / "dataset.ndjson"
    with open(dataset_path, "w") as f:
        # High score -> good outcome
        f.write(json.dumps({"ok": 1, "score": 1.0, "r_mult": 1.5}) + "\n")
        f.write(json.dumps({"ok": 1, "score": 0.8, "r_mult": 1.0}) + "\n")
        # Low score -> bad outcome
        f.write(json.dumps({"ok": 1, "score": 0.0, "r_mult": -0.5}) + "\n")
        f.write(json.dumps({"ok": 1, "score": -0.5, "r_mult": -1.5}) + "\n")  # tail
        f.write(json.dumps({"ok": 1, "score": 0.3, "r_mult": 0.2}) + "\n")

    # Run eval
    import sys
    from io import StringIO

    from tools.eval_meta_enforce import main

    out_path = tmp_path / "eval.json"

    old_argv = sys.argv
    old_stdout = sys.stdout
    try:
        sys.argv = [
            "eval_meta_enforce",
            "--dataset", str(dataset_path),
            "--model", str(model_path),
            "--out", str(out_path),
            "--grid", "0.50,0.60,0.70",
            "--min-pass-rate", "0.20",
            "--tail-max", "0.30",
            "--tail-improve-min", "0.01",
            "--meanr-drop-max", "0.10",
        ]
        sys.stdout = StringIO()
        main()
        sys.stdout = old_stdout
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout

    # Check output
    assert os.path.exists(out_path)
    with open(out_path) as f:
        result = json.load(f)

    assert "best" in result
    if result["best"] is not None:
        assert "meta_p_min" in result["best"]
        assert "baseline" in result["best"]
        assert "filtered" in result["best"]
        assert "delta" in result["best"]

