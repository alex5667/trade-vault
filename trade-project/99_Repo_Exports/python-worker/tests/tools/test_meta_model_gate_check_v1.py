"""Tests for tools/meta_model_gate_check_v1.py"""
from __future__ import annotations

import json
import os
import tempfile
from unittest import mock

import pytest

from tools.meta_model_gate_check_v1 import check_model, _now_ms


NOW_MS = 1_778_700_000_000  # fixed reference ≈ 2026-05-13
MS_DAY = 86_400_000


def _write(path: str, obj: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def _model(tmp: str, age_days: float = 1.0, schema: str = "meta_feat_v3") -> str:
    path = os.path.join(tmp, "model.json")
    _write(path, {
        "created_ms": int(NOW_MS - age_days * MS_DAY),
        "schema_name": schema,
        "schema_version": 3,
        "intercept": 0.0,
        "coef": [],
        "features": [],
        "threshold": 0.5,
    })
    return path


def _report(tmp: str, *, auc: float = 0.65, exp: float = 0.05, ece: float = 0.07, stub: bool = False) -> str:
    path = os.path.join(tmp, "model.report.json")
    doc: dict = {
        "version": "3.0.0-stub" if stub else "1.0.0",
        "n": 3000,
        "holdout_metrics": {
            "n": 900,
            "auc": auc,
            "expectancy_r_top5pct": exp,
            "ece": ece,
        },
        "validation": {
            "pass": not stub,
            "reasons": ["insufficient_samples(10<2000)"] if stub else [],
        },
    }
    _write(path, doc)
    return path


# ── tests ──────────────────────────────────────────────────────────────────

def test_pass_all_gates():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp)
        r = _report(tmp, auc=0.65, exp=0.08, ece=0.07)
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, ev = check_model(m, r, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert ok, blockers
    assert blockers == []
    assert ev["age_days"] == pytest.approx(1.0, abs=0.01)


def test_fail_stale_artifact():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp, age_days=31)
        r = _report(tmp)
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, _ = check_model(m, r, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert not ok
    assert any("stale_artifact" in b for b in blockers)


def test_fail_stub_version():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp)
        r = _report(tmp, stub=True)
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, _ = check_model(m, r, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert not ok
    assert any("stub_artifact" in b for b in blockers)


def test_fail_low_auc():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp)
        r = _report(tmp, auc=0.55, exp=0.05, ece=0.07)
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, _ = check_model(m, r, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert not ok
    assert any("auc_below_threshold" in b for b in blockers)


def test_fail_negative_expectancy():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp)
        r = _report(tmp, auc=0.65, exp=-0.40, ece=0.07)
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, _ = check_model(m, r, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert not ok
    assert any("negative_expectancy" in b for b in blockers)


def test_fail_poor_calibration():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp)
        r = _report(tmp, auc=0.65, exp=0.05, ece=0.17)
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, _ = check_model(m, r, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert not ok
    assert any("poor_calibration" in b for b in blockers)


def test_fail_env_not_set():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp)
        r = _report(tmp)
        env = {k: v for k, v in os.environ.items() if k != "META_MODEL_PATH"}
        with mock.patch.dict(os.environ, env, clear=True):
            ok, blockers, ev = check_model(m, r, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert not ok
    assert any("META_MODEL_PATH" in b for b in blockers)
    assert ev["env_META_MODEL_PATH"] == "(unset)"


def test_fail_model_not_found():
    with tempfile.TemporaryDirectory() as tmp:
        bad_path = os.path.join(tmp, "nonexistent.json")
        ok, blockers, _ = check_model(bad_path, None, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert not ok
    assert any("model_file_not_found" in b for b in blockers)


def test_fail_report_not_found():
    with tempfile.TemporaryDirectory() as tmp:
        m = _model(tmp)
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, _ = check_model(
                m, "/nonexistent/report.json",
                max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS,
            )
    assert not ok
    assert any("report_not_found" in b for b in blockers)


def test_auto_detect_report_same_stem():
    with tempfile.TemporaryDirectory() as tmp:
        # model.json + model.report.json in same dir → auto-detected
        m = _model(tmp)
        _report(tmp, auc=0.65, exp=0.08, ece=0.07)  # writes model.report.json
        with mock.patch.dict(os.environ, {"META_MODEL_PATH": m}):
            ok, blockers, ev = check_model(m, None, max_age_days=30, auc_min=0.62, ece_max=0.10, now_ms=NOW_MS)
    assert ok, blockers
    assert ev["report_path"] is not None


def test_multiple_blockers_for_current_stub():
    """Current meta_lr_v4_nightly.json must accumulate ≥3 blockers."""
    model_path = "/var/lib/trade/of_reports/models/meta_lr_v4_nightly.json"
    if not os.path.isfile(model_path):
        pytest.skip("artifact not present in this environment")

    report_path = "/var/lib/trade/of_reports/models/meta_lr_20260214_051012.report.json"
    ok, blockers, _ = check_model(
        model_path, report_path,
        max_age_days=30, auc_min=0.62, ece_max=0.10,
        now_ms=_now_ms(),
    )
    assert not ok
    assert len(blockers) >= 3, f"expected ≥3 blockers, got: {blockers}"
    assert any("stale_artifact" in b for b in blockers)
    assert any("META_MODEL_PATH" in b for b in blockers)
