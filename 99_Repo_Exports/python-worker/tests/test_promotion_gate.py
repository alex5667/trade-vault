"""Plan 3 / Step 4 — promotion gate + manifest tests."""
from __future__ import annotations

import json

import pytest

from calibration.promotion_gate import (
    PromotionMetrics,
    PromotionThresholds,
    can_promote,
)
from calibration.promotion_manifest import build_manifest, from_json, to_json


def _good_metrics(**overrides) -> PromotionMetrics:
    base = dict(
        n_oos_trades=500,
        n_oos_days=21,
        mean_oos_profit_factor=1.25,
        mean_oos_sharpe=0.6,
        deflated_sharpe=0.3,
        pbo=0.15,
        ece=0.04,
        brier=0.08,
        pass_rate=0.07,
        slippage_residual_p95_bps=4.0,
    )
    base.update(overrides)
    return PromotionMetrics(**base)


# ─── can_promote rules ───────────────────────────────────────────────────────


def test_passes_when_all_above_bars():
    ok, reasons = can_promote(_good_metrics())
    assert ok is True
    assert reasons == []


def test_fails_when_oos_trades_too_low():
    ok, reasons = can_promote(_good_metrics(n_oos_trades=100))
    assert ok is False
    assert "oos_trades_too_low" in reasons


def test_fails_when_oos_days_too_low():
    ok, reasons = can_promote(_good_metrics(n_oos_days=3))
    assert ok is False
    assert "oos_days_too_low" in reasons


def test_fails_when_pbo_too_high():
    ok, reasons = can_promote(_good_metrics(pbo=0.5))
    assert ok is False
    assert "pbo_too_high" in reasons


def test_fails_when_deflated_sharpe_non_positive():
    ok, reasons = can_promote(_good_metrics(deflated_sharpe=0.0))
    assert ok is False
    assert "deflated_sharpe_non_positive" in reasons


def test_fails_when_ece_too_high():
    ok, reasons = can_promote(_good_metrics(ece=0.1))
    assert ok is False
    assert "ece_too_high" in reasons


def test_fails_when_pass_rate_too_low():
    ok, reasons = can_promote(_good_metrics(pass_rate=0.001))
    assert ok is False
    assert "pass_rate_too_low" in reasons


def test_accumulates_multiple_reasons():
    ok, reasons = can_promote(_good_metrics(pbo=0.5, ece=0.1, pass_rate=0.001))
    assert ok is False
    assert set(reasons) >= {"pbo_too_high", "ece_too_high", "pass_rate_too_low"}


def test_slippage_residual_gate_only_when_threshold_set():
    metrics = _good_metrics(slippage_residual_p95_bps=99.0)
    # Default thresholds: None → gate skipped
    ok, reasons = can_promote(metrics)
    assert ok is True
    # Explicit threshold → fails
    thr = PromotionThresholds(max_slippage_residual_p95_bps=10.0)
    ok2, reasons2 = can_promote(metrics, thr)
    assert ok2 is False
    assert "slippage_residual_too_high" in reasons2


def test_slippage_gate_skipped_when_metric_is_none():
    thr = PromotionThresholds(max_slippage_residual_p95_bps=10.0)
    ok, reasons = can_promote(_good_metrics(slippage_residual_p95_bps=None), thr)
    assert ok is True


def test_custom_thresholds_override_defaults():
    thr = PromotionThresholds(min_oos_trades=1000)
    ok, reasons = can_promote(_good_metrics(n_oos_trades=500), thr)
    assert ok is False
    assert "oos_trades_too_low" in reasons


# ─── PromotionManifest builder + JSON round-trip ─────────────────────────────


def _baseline_manifest_args():
    return dict(
        candidate_id="opt-2026-05-30-001",
        code_sha="abc123def",
        schema_hash="c3e1a7f29d50",
        feature_cols_hash="ff0011223344",
        data_start_ms=1_700_000_000_000,
        data_end_ms=1_700_086_400_000,
        n_trials=200,
        metrics=_good_metrics(),
    )


def test_manifest_report_only_by_default():
    m = build_manifest(**_baseline_manifest_args())
    assert m.decision == "REPORT_ONLY"
    assert m.reasons == []


def test_manifest_enforce_promotes_when_passing():
    m = build_manifest(**_baseline_manifest_args(), enforce_decision=True)
    assert m.decision == "PROMOTE_TO_SHADOW"


def test_manifest_enforce_rejects_when_failing():
    args = _baseline_manifest_args()
    args["metrics"] = _good_metrics(pbo=0.99)
    m = build_manifest(**args, enforce_decision=True)
    assert m.decision == "REJECTED"
    assert "pbo_too_high" in m.reasons


def test_manifest_carries_failure_reasons_in_report_only():
    args = _baseline_manifest_args()
    args["metrics"] = _good_metrics(pbo=0.99, ece=0.5)
    m = build_manifest(**args, enforce_decision=False)
    assert m.decision == "REPORT_ONLY"
    # reasons still populated so reviewer sees what would block
    assert "pbo_too_high" in m.reasons
    assert "ece_too_high" in m.reasons


def test_manifest_json_round_trip():
    m = build_manifest(**_baseline_manifest_args(), enforce_decision=True)
    s = to_json(m)
    parsed = json.loads(s)
    # Required fields present in JSON
    assert parsed["candidate_id"] == m.candidate_id
    assert parsed["decision"] == "PROMOTE_TO_SHADOW"
    # Round-trip restores the dataclass
    m2 = from_json(s)
    assert m2.decision == m.decision
    assert m2.metrics.n_oos_trades == m.metrics.n_oos_trades
    assert m2.thresholds.max_pbo == m.thresholds.max_pbo


def test_manifest_extras_round_trip():
    args = _baseline_manifest_args()
    m = build_manifest(**args, extras={"git_branch": "main", "n_features": 47})
    s = to_json(m)
    m2 = from_json(s)
    assert m2.extras["git_branch"] == "main"
    assert m2.extras["n_features"] == 47
