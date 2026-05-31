"""Plan 1 — artifact loader / scorer tests.

We never store a model artifact in the repo; tests build the JSON on the fly
and round-trip it through load_artifact() to assert the wire format.
"""
from __future__ import annotations

import json
import math
import os
import tempfile

from services.confidence_meta_gate.model import (
    CalibrationSpec,
    MetaGateArtifact,
    Thresholds,
    TrainingSummary,
    _hash_feature_cols,
    _piecewise_isotonic,
    load_artifact,
)


def _make_artifact_file(payload: dict, suffix: str = ".json") -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    return path


def test_load_artifact_missing_path_returns_none() -> None:
    assert load_artifact("/no/such/path.json") is None


def test_load_artifact_empty_string_returns_none() -> None:
    assert load_artifact("") is None


def test_load_artifact_bad_json_returns_none() -> None:
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        with open(path, "w") as f:
            f.write("{this is not json")
        assert load_artifact(path) is None
    finally:
        os.unlink(path)


def test_load_artifact_coef_len_mismatch_returns_none() -> None:
    payload = {
        "model_ver": "v1",
        "schema": "conf_meta_gate_schema_v1",
        "feature_cols": ["a", "b", "c"],
        "model": {"type": "logistic_regression", "intercept": 0.0,
                  "coef": [0.1, 0.2]},  # only 2 — mismatch
    }
    path = _make_artifact_file(payload)
    try:
        assert load_artifact(path) is None
    finally:
        os.unlink(path)


def test_load_artifact_unknown_model_type_returns_none() -> None:
    payload = {
        "model_ver": "v1",
        "schema": "conf_meta_gate_schema_v1",
        "feature_cols": ["a"],
        "model": {"type": "magic_box", "intercept": 0.0, "coef": [0.1]},
    }
    path = _make_artifact_file(payload)
    try:
        assert load_artifact(path) is None
    finally:
        os.unlink(path)


def test_load_artifact_minimal_valid_payload() -> None:
    payload = {
        "model_ver": "v1",
        "schema": "conf_meta_gate_schema_v1",
        "feature_cols": ["a", "b"],
        "model": {
            "type": "logistic_regression",
            "intercept": -0.5,
            "coef": [1.5, -0.8],
        },
        "calibrator": {"type": "platt", "a": 1.1, "b": 0.05, "ece": 0.03},
        "thresholds": {
            "min_p_win": 0.6,
            "min_expected_r": 0.04,
            "min_expected_edge_bps": 2.0,
        },
        "training_summary": {
            "n_rows": 5000,
            "pos_rate": 0.4,
            "oos_auc": 0.61,
            "oos_brier": 0.21,
            "oos_ece": 0.04,
            "top5_expectancy_r": 0.18,
            "created_ms": 1700000000000,
        },
    }
    path = _make_artifact_file(payload)
    try:
        art = load_artifact(path)
    finally:
        os.unlink(path)

    assert art is not None
    assert art.model_ver == "v1"
    assert art.feature_cols == ("a", "b")
    assert art.intercept == -0.5
    assert art.coef == (1.5, -0.8)
    assert art.calibrator.type == "platt"
    assert art.calibrator.ece == 0.03
    assert art.thresholds.min_p_win == 0.6
    assert art.feature_cols_hash == _hash_feature_cols(("a", "b"))


def test_predict_raw_with_missing_feature_uses_default_zero() -> None:
    art = MetaGateArtifact(
        model_ver="t", schema="t",
        feature_cols=("a", "b"),
        model_type="logistic_regression",
        intercept=0.0, coef=(1.0, 1.0),
        calibrator=CalibrationSpec(type="identity"),
        thresholds=Thresholds(),
        training_summary=TrainingSummary(),
        loaded_at_ms=0, source_path="mem",
        feature_cols_hash="h",
    )
    # only "a" present → z = 1*5 + 1*0 = 5 → σ(5) ≈ 0.993
    p = art.predict_raw({"a": 5.0})
    assert 0.99 < p < 1.0


def test_calibrator_identity_passes_through() -> None:
    spec = CalibrationSpec(type="identity")
    art = MetaGateArtifact(
        model_ver="t", schema="t",
        feature_cols=(), model_type="logistic_regression",
        intercept=0.0, coef=(),
        calibrator=spec, thresholds=Thresholds(),
        training_summary=TrainingSummary(), loaded_at_ms=0,
        source_path="mem", feature_cols_hash="h",
    )
    assert math.isclose(art.calibrate(0.3), 0.3, abs_tol=1e-9)
    assert math.isclose(art.calibrate(1.5), 1.0, abs_tol=1e-9)
    assert math.isclose(art.calibrate(-0.2), 0.0, abs_tol=1e-9)


def test_piecewise_isotonic_endpoints_and_interp() -> None:
    points = ((0.0, 0.1), (0.5, 0.4), (1.0, 0.9))
    assert _piecewise_isotonic(points, -1.0) == 0.1
    assert _piecewise_isotonic(points, 2.0) == 0.9
    # Midpoint of last segment: 0.75 → between (0.5, 0.4) and (1.0, 0.9)
    mid = _piecewise_isotonic(points, 0.75)
    assert math.isclose(mid, 0.4 + 0.5 * (0.9 - 0.4), abs_tol=1e-9)


def test_feature_cols_hash_changes_with_order() -> None:
    assert _hash_feature_cols(("a", "b")) != _hash_feature_cols(("b", "a"))


def test_feature_cols_hash_is_short_and_stable() -> None:
    h1 = _hash_feature_cols(("alpha", "beta", "gamma"))
    h2 = _hash_feature_cols(("alpha", "beta", "gamma"))
    assert h1 == h2
    assert len(h1) == 16
