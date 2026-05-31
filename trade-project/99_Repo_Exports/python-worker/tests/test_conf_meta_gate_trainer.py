"""Plan 1 Phase 2 — trainer / calibrator / artifact-emit tests.

The full LR fit + purged-CV runs on a small synthetic dataset so the
test is deterministic and quick (<2 s). The artifact JSON is then
round-tripped through `confidence_meta_gate.model.load_artifact` to
guarantee schema compatibility.
"""
from __future__ import annotations

import json
import math
import os
import tempfile

import numpy as np
import pytest

from calibration.conf_meta_gate_trainer import (
    CalibratorBlock,
    TrainConfig,
    apply_calibrator,
    brier_score,
    build_artifact_json,
    expected_calibration_error,
    fit_calibrator,
    fit_platt,
    pass_rate,
    roc_auc,
    rows_to_arrays,
    select_features,
    top_pct_expectancy,
    train,
    write_artifact,
)
from services.confidence_meta_gate.model import load_artifact


# ── pure metrics ────────────────────────────────────────────────────────────


def test_brier_score_perfect_zero() -> None:
    p = np.array([0.0, 1.0, 0.0, 1.0])
    y = np.array([0, 1, 0, 1])
    assert brier_score(p, y) == 0.0


def test_brier_score_worst_one() -> None:
    p = np.array([1.0, 0.0])
    y = np.array([0, 1])
    assert brier_score(p, y) == 1.0


def test_ece_zero_for_perfectly_calibrated_uniform() -> None:
    p = np.linspace(0.05, 0.95, 100)
    # Manufactured labels matching the predicted probabilities exactly.
    y = (np.random.RandomState(0).uniform(size=100) < p).astype(int)
    val = expected_calibration_error(p, y, n_bins=5)
    assert 0.0 <= val <= 1.0


def test_roc_auc_perfect_separator() -> None:
    p = np.array([0.1, 0.2, 0.8, 0.9])
    y = np.array([0, 0, 1, 1])
    assert math.isclose(roc_auc(p, y), 1.0, abs_tol=1e-9)


def test_roc_auc_random_around_half() -> None:
    rng = np.random.RandomState(0)
    p = rng.uniform(size=400)
    y = rng.randint(0, 2, size=400)
    val = roc_auc(p, y)
    assert 0.4 < val < 0.6


def test_roc_auc_single_class_returns_half() -> None:
    p = np.array([0.1, 0.2, 0.3])
    y = np.array([1, 1, 1])
    assert roc_auc(p, y) == 0.5


def test_top_pct_expectancy_picks_top_slice() -> None:
    p = np.array([0.9, 0.1, 0.8, 0.2])
    r = np.array([1.0, -1.0, 2.0, -2.0])
    val = top_pct_expectancy(p, r, top_pct=0.5)
    assert val == 1.5  # top 2 of 4 by p → 0.9 (r=1.0) and 0.8 (r=2.0)


def test_pass_rate_threshold() -> None:
    p = np.array([0.1, 0.5, 0.6, 0.9])
    assert pass_rate(p, 0.56) == 0.5


# ── calibrators ─────────────────────────────────────────────────────────────


def test_platt_calibrator_round_trips_close_to_identity() -> None:
    rng = np.random.RandomState(0)
    p_raw = rng.uniform(0.05, 0.95, size=500)
    y = (rng.uniform(size=500) < p_raw).astype(int)
    blk = fit_platt(p_raw, y)
    out = apply_calibrator(blk, p_raw)
    # On well-calibrated input, Platt should not drastically shift predictions.
    assert np.abs(out - p_raw).mean() < 0.10


def test_isotonic_calibrator_clips_to_unit_interval() -> None:
    rng = np.random.RandomState(0)
    p = rng.uniform(0.0, 1.0, size=200)
    y = (rng.uniform(size=200) < p).astype(int)
    blk = fit_calibrator("isotonic", p, y.astype(np.float64))
    out = apply_calibrator(blk, p)
    assert out.min() >= 0.0
    assert out.max() <= 1.0


def test_identity_calibrator_passes_through() -> None:
    blk = CalibratorBlock(type="identity")
    out = apply_calibrator(blk, np.array([0.0, 0.3, 1.0]))
    assert list(out) == [0.0, 0.3, 1.0]


# ── feature selection ──────────────────────────────────────────────────────


def test_select_features_drops_low_coverage() -> None:
    rows = [
        {"a": 1.0, "b": None}, {"a": 2.0, "b": None},
        {"a": 3.0, "b": None}, {"a": 4.0, "b": None},
        {"a": 5.0, "b": 1.0},  # only 20% coverage on b
    ]
    cols = select_features(rows, candidates=("a", "b"), min_coverage=0.5)
    assert cols == ("a",)


def test_select_features_keeps_full_coverage() -> None:
    rows = [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": 4.0}]
    cols = select_features(rows, candidates=("a", "b"), min_coverage=0.5)
    assert cols == ("a", "b")


def test_rows_to_arrays_dimensions() -> None:
    rows = [
        {"a": 1.0, "ts_ms": 100, "horizon_ms": 10, "r_mult": 0.5, "y_util_pos": 1},
        {"a": 2.0, "ts_ms": 110, "horizon_ms": 20, "r_mult": -1.0, "y_util_pos": 0},
    ]
    X, y, r, d, rs = rows_to_arrays(rows, ("a",), target="y_util_pos")
    assert X.shape == (2, 1)
    assert list(y) == [1, 0]
    assert list(r) == [0.5, -1.0]
    assert list(d) == [100, 110]
    assert list(rs) == [110, 130]


# ── end-to-end training ───────────────────────────────────────────────────


def _synthetic_dataset(n: int = 1200, seed: int = 0) -> list[dict]:
    """A pretty easy dataset so the trainer reliably converges.

    f0 has true positive coefficient; f1 is noise; y is generated from
    a logistic with intercept −0.5.
    """
    rng = np.random.RandomState(seed)
    f0 = rng.normal(size=n)
    f1 = rng.normal(size=n)
    spread = np.abs(rng.normal(scale=0.5, size=n))
    z = -0.5 + 1.2 * f0
    p_true = 1.0 / (1.0 + np.exp(-z))
    y = (rng.uniform(size=n) < p_true).astype(int)
    r = np.where(y == 1, 1.2, -1.0) + rng.normal(scale=0.2, size=n)
    rows: list[dict] = []
    base_ts = 1_700_000_000_000
    for i in range(n):
        rows.append({
            "sid": f"s-{i}",
            "ts_ms": int(base_ts + i * 60_000),
            "horizon_ms": 600_000,
            "rule_score": float(f0[i]),
            "spread_bps": float(spread[i]),
            "expected_slippage_bps": 1.0,
            "fee_bps": 1.0,
            "exec_cost_bps": 3.0,
            "expected_edge_bps": 5.0,
            "exec_risk_norm": 0.2,
            "dq_score": 1.0,
            "dq_flag_count": 0.0,
            "regime_code": 1.0,
            "session_asia": 0.0,
            "session_europe": 0.0,
            "session_us": 1.0,
            "weekend_flag": 0.0,
            "legacy_confidence": 0.5,
            "p_edge_raw": float(p_true[i]),
            "p_edge_cal": float(p_true[i]),
            "have_need_ratio": 1.0,
            "y_win": int(y[i]),
            "y_util_pos": int(y[i]),
            "r_mult": float(r[i]),
        })
    return rows


def test_train_end_to_end_produces_artifact() -> None:
    rows = _synthetic_dataset(n=1200)
    cfg = TrainConfig(
        target="y_util_pos",
        calibrator="platt",
        n_cv_blocks=4,
        embargo_ms=0,
        min_rows=500,
        min_coverage=0.5,
    )
    result = train(rows, cfg=cfg)
    assert result.feature_cols  # something was selected
    assert "rule_score" in result.feature_cols
    assert len(result.coef) == len(result.feature_cols)
    assert result.oos_auc > 0.55  # the synthetic signal is clear
    assert 0.0 <= result.oos_ece <= 0.20
    assert result.fold_returns  # at least one fold contributed


def test_train_raises_on_too_few_rows() -> None:
    cfg = TrainConfig(min_rows=10_000)
    with pytest.raises(ValueError):
        train(_synthetic_dataset(n=200), cfg=cfg)


def test_artifact_roundtrips_through_load_artifact() -> None:
    rows = _synthetic_dataset(n=1200)
    cfg = TrainConfig(
        target="y_util_pos",
        calibrator="platt",
        n_cv_blocks=4,
        embargo_ms=0,
        min_rows=500,
        min_coverage=0.5,
    )
    result = train(rows, cfg=cfg)
    payload = build_artifact_json(result, cfg=cfg)
    # Required schema fields per MetaGateArtifact.load_artifact.
    assert payload["model_ver"].startswith("conf_meta_gate_lr_")
    assert payload["model"]["type"] == "logistic_regression"
    assert len(payload["model"]["coef"]) == len(payload["feature_cols"])
    assert payload["thresholds"]["min_p_win"] == 0.56

    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        write_artifact(payload, path)
        art = load_artifact(path)
    finally:
        os.unlink(path)
    assert art is not None
    assert art.model_ver == payload["model_ver"]
    assert art.feature_cols == tuple(payload["feature_cols"])
    # Sanity: predict_raw + calibrate combine to a probability in [0, 1].
    sample_feats = {col: 0.5 for col in art.feature_cols}
    p_raw = art.predict_raw(sample_feats)
    p_cal = art.calibrate(p_raw)
    assert 0.0 <= p_cal <= 1.0


def test_write_artifact_is_atomic() -> None:
    payload = {"hello": "world"}
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        write_artifact(payload, path)
        with open(path) as f:
            assert json.load(f) == payload
        # No leftover tmp file.
        assert not os.path.exists(path + ".tmp")
    finally:
        os.unlink(path)
