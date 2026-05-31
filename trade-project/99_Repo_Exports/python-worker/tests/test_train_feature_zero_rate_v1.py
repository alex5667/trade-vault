"""test_train_feature_zero_rate_v1.py — audit-2026-05-29 item 6.

Pure-Python tests for `tools/train_feature_zero_rate_v1.py`:
  * column-family taxonomy is consistent with the registry one-hots.
  * `compute_zero_rates` reports per-column rates and per-family aggregates
    correctly, including the `all_zero_cols` list.
  * `assert_categorical_families_alive` raises `CategoricalAllZeroError`
    when a v15_of categorical family is fully zero, and is a no-op for
    other schemas.
  * `write_report_json` produces a deterministic, schema-stable artefact.
  * `emit_prometheus` is best-effort (returns False when prometheus_client
    not installed; True otherwise).
  * Alerts file declares `V15TrainingCategoricalFeaturesAllZero` and
    `V15TrainingCategoricalFamilyZeroRateInflated`.
"""
from __future__ import annotations

import json
import pathlib
import tempfile

import numpy as np
import pytest


# ── Column family taxonomy ───────────────────────────────────────────────────

def test_column_family_taxonomy():
    from tools.train_feature_zero_rate_v1 import _column_family

    assert _column_family("bucket:trend") == "bucket"
    assert _column_family("hour:10") == "hour"
    assert _column_family("dow:2") == "dow"
    assert _column_family("session_asia") == "session"
    assert _column_family("dir:LONG") == "dir"
    assert _column_family("f_delta_z") == "f"
    assert _column_family("mul_delta_z__liq_score") == "other"
    assert _column_family("scenario_v4_trend") == "other"


# ── compute_zero_rates ───────────────────────────────────────────────────────

def test_compute_zero_rates_all_nonzero_families():
    from tools.train_feature_zero_rate_v1 import compute_zero_rates

    cols = ["bucket:trend", "hour:10", "dow:2", "session_asia", "f_delta_z"]
    X = np.array([
        [1.0, 1.0, 1.0, 1.0, 0.5],
        [1.0, 1.0, 1.0, 1.0, 0.7],
    ], dtype=np.float32)
    rep = compute_zero_rates(X, cols)

    assert rep["rows"] == 2
    assert rep["cols"] == 5
    assert rep["all_zero_cols"] == []
    for c in cols:
        assert rep["per_column"][c] == 0.0
    for fam in ("bucket", "hour", "dow", "session", "f"):
        assert rep["per_family"][fam]["all_zero_cols"] == 0
        assert rep["per_family"][fam]["mean_zero_rate"] == 0.0


def test_compute_zero_rates_all_zero_category():
    from tools.train_feature_zero_rate_v1 import compute_zero_rates

    cols = ["bucket:trend", "bucket:range", "bucket:other", "f_delta_z"]
    # Whole bucket family encoded as 0
    X = np.array([
        [0.0, 0.0, 0.0, 1.0],
        [0.0, 0.0, 0.0, 0.5],
        [0.0, 0.0, 0.0, 0.7],
    ], dtype=np.float32)
    rep = compute_zero_rates(X, cols)

    assert rep["all_zero_cols"] == ["bucket:other", "bucket:range", "bucket:trend"]
    assert rep["per_family"]["bucket"]["cols"] == 3
    assert rep["per_family"]["bucket"]["all_zero_cols"] == 3
    assert rep["per_family"]["bucket"]["mean_zero_rate"] == 1.0
    assert rep["per_family"]["f"]["all_zero_cols"] == 0


def test_compute_zero_rates_shape_mismatch_raises():
    from tools.train_feature_zero_rate_v1 import compute_zero_rates

    with pytest.raises(ValueError):
        compute_zero_rates(np.zeros((3, 2)), ["a", "b", "c"])


def test_compute_zero_rates_partial_zero_rate():
    from tools.train_feature_zero_rate_v1 import compute_zero_rates

    cols = ["hour:0", "hour:1"]
    X = np.array([
        [1.0, 0.0],
        [0.0, 0.0],
        [0.0, 1.0],
        [1.0, 0.0],
    ], dtype=np.float32)
    rep = compute_zero_rates(X, cols)
    assert rep["per_column"]["hour:0"] == 0.5  # 2/4 zero
    assert rep["per_column"]["hour:1"] == 0.75  # 3/4 zero
    assert rep["all_zero_cols"] == []  # neither fully zero


# ── assert_categorical_families_alive ────────────────────────────────────────

def test_assert_families_alive_raises_on_dead_bucket():
    from tools.train_feature_zero_rate_v1 import (
        CategoricalAllZeroError,
        assert_categorical_families_alive,
        compute_zero_rates,
    )

    cols = ["bucket:trend", "bucket:range", "hour:10", "f_x"]
    X = np.zeros((5, 4), dtype=np.float32)
    X[:, 2] = 1.0  # hour:10 alive
    X[:, 3] = 0.5  # f_x alive
    rep = compute_zero_rates(X, cols)
    with pytest.raises(CategoricalAllZeroError) as exc:
        assert_categorical_families_alive(rep, schema_ver="v15_of")
    assert "bucket" in str(exc.value)


def test_assert_families_alive_noop_for_non_v15():
    from tools.train_feature_zero_rate_v1 import (
        assert_categorical_families_alive,
        compute_zero_rates,
    )

    cols = ["bucket:trend"]
    X = np.zeros((3, 1), dtype=np.float32)
    rep = compute_zero_rates(X, cols)
    # v14_of is NOT in enabled_for_schemas — must be a no-op.
    assert_categorical_families_alive(rep, schema_ver="v14_of")


def test_assert_families_alive_passes_when_all_alive():
    from tools.train_feature_zero_rate_v1 import (
        assert_categorical_families_alive,
        compute_zero_rates,
    )

    cols = ["bucket:trend", "hour:10", "dow:2", "session_asia", "dir:LONG"]
    X = np.ones((4, 5), dtype=np.float32)
    rep = compute_zero_rates(X, cols)
    # Should not raise
    assert_categorical_families_alive(rep, schema_ver="v15_of")


# ── write_report_json ────────────────────────────────────────────────────────

def test_write_report_json_deterministic():
    from tools.train_feature_zero_rate_v1 import write_report_json

    rep = {"rows": 5, "per_family": {"bucket": {"cols": 3}}}
    with tempfile.TemporaryDirectory() as tmp:
        out = pathlib.Path(tmp) / "subdir" / "report.json"
        write_report_json(rep, str(out))
        assert out.exists()
        loaded = json.loads(out.read_text(encoding="utf-8"))
        assert loaded["tool"] == "train_feature_zero_rate_v1"
        assert loaded["rows"] == 5
        # Sorted keys → stable diffs in CI
        text = out.read_text(encoding="utf-8")
        keys_in_order = [
            text.find('"per_family"'),
            text.find('"rows"'),
            text.find('"tool"'),
        ]
        assert keys_in_order == sorted(keys_in_order)


def test_write_report_json_skips_when_path_empty():
    from tools.train_feature_zero_rate_v1 import write_report_json
    # Empty path → no-op, no exception.
    write_report_json({"rows": 1}, "")


# ── emit_prometheus ──────────────────────────────────────────────────────────

def test_emit_prometheus_returns_bool():
    """Best-effort: returns True/False depending on whether
    prometheus_client is importable. Doesn't raise either way."""
    from tools.train_feature_zero_rate_v1 import compute_zero_rates, emit_prometheus

    rep = compute_zero_rates(
        np.array([[1.0, 0.0]], dtype=np.float32),
        ["f_x", "bucket:trend"],
    )
    result = emit_prometheus(rep, schema_ver="v15_of")
    assert result in (True, False)


# ── Alerts file integration ──────────────────────────────────────────────────

_ALERT_FILE = pathlib.Path(__file__).parent.parent / "monitoring" / "prometheus_alerts_ml_pipeline_failopen.yml"


def test_training_alerts_present():
    if not _ALERT_FILE.exists():
        pytest.skip("alert file missing")
    src = _ALERT_FILE.read_text(encoding="utf-8")
    assert "V15TrainingCategoricalFeaturesAllZero" in src, (
        "audit-2026-05-29 item 6: V15TrainingCategoricalFeaturesAllZero "
        "alert must be declared so a build_feature_row regression in "
        "production training fires immediately."
    )
    assert "V15TrainingCategoricalFamilyZeroRateInflated" in src
    # Both alerts must reference the new training metric, not a placeholder.
    assert "ml_train_feature_all_zero_cols" in src
    assert "ml_train_feature_zero_rate" in src


def test_training_alerts_scoped_to_v15_of():
    """The alerts must scope to schema=v15_of so v14_of training (which
    intentionally has narrower one-hots) does NOT page on the same metric."""
    if not _ALERT_FILE.exists():
        pytest.skip("alert file missing")
    src = _ALERT_FILE.read_text(encoding="utf-8")
    # Both rules must label-filter to v15_of.
    assert 'schema="v15_of"' in src
