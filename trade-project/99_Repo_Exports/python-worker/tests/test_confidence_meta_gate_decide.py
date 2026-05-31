"""Plan 1 — decide_meta_gate unit tests.

The gate is pure: same (input, config, artifact) → same output. Tests build
synthetic artifacts and feed inputs that exercise every branch (fallback,
schema mismatch, ECE high, soft-cap tighten, p_win floor, expected_r floor).
"""
from __future__ import annotations

import math
import time

import pytest

from services.confidence_meta_gate.config import MetaGateConfig, MetaGateMode  # noqa: F401 — used via _build_cfg
from services.confidence_meta_gate.dto import ConfidenceMetaGateInput
from services.confidence_meta_gate.gate import (
    decide_meta_gate,
    risk_multiplier_from_p_win,
)
from services.confidence_meta_gate.model import (
    CalibrationSpec,
    MetaGateArtifact,
    Thresholds,
    TrainingSummary,
    _hash_feature_cols,
)
from services.confidence_meta_gate.reason_codes import MetaGateReason


def _build_cfg(
    *, mode: MetaGateMode = MetaGateMode.SHADOW,
    enabled: bool = True,
    canary_share: float = 0.0,
    max_calibration_ece: float = 0.07,
    risk_mult_enabled: bool = False,
) -> MetaGateConfig:
    return MetaGateConfig(
        enabled=enabled,
        mode=mode,
        model_path="/dev/null",
        calibrator_path="/dev/null",
        canary_share=canary_share,
        canary_salt="t-salt",
        fail_mode="LEGACY",
        max_model_age_hours=24.0,
        max_calibration_ece=max_calibration_ece,
        min_p_win=0.56,
        min_expected_r=0.02,
        min_expected_edge_bps=1.5,
        dq_soft_cap=0.7,
        spread_soft_cap_bps=6.0,
        slippage_soft_cap_bps=6.0,
        risk_mult_enabled=risk_mult_enabled,
        metrics_stream="m",
        decision_stream="d",
        sample_features_in_stream=False,
    )


def _build_artifact(
    *,
    intercept: float = 0.0,
    coef: tuple[float, ...] = (1.0, 1.0),
    feature_cols: tuple[str, ...] = ("f0", "f1"),
    cal_type: str = "identity",
    cal_a: float = 1.0,
    cal_b: float = 0.0,
    ece: float | None = None,
    created_ms: int | None = None,
    min_p_win: float = 0.56,
    min_expected_r: float = 0.02,
    min_expected_edge_bps: float = 1.5,
) -> MetaGateArtifact:
    cols = feature_cols
    created = created_ms if created_ms is not None else int(time.time() * 1000)
    return MetaGateArtifact(
        model_ver="test-v1",
        schema="test-schema-v1",
        feature_cols=cols,
        model_type="logistic_regression",
        intercept=intercept,
        coef=coef,
        calibrator=CalibrationSpec(type=cal_type, a=cal_a, b=cal_b, ece=ece),
        thresholds=Thresholds(min_p_win=min_p_win,
                              min_expected_r=min_expected_r,
                              min_expected_edge_bps=min_expected_edge_bps),
        training_summary=TrainingSummary(created_ms=created),
        loaded_at_ms=created,
        source_path="memory://test",
        feature_cols_hash=_hash_feature_cols(cols),
    )


def _build_input(
    *,
    sid: str = "sid-1",
    legacy_decision: str = "DENY",
    legacy_confidence: float = 0.5,
    legacy_min: float = 0.7,
    features: dict[str, float] | None = None,
    expected_edge_bps: float = 5.0,
    spread_bps: float = 1.0,
    slippage_bps: float = 1.0,
    fee_bps: float = 1.0,
    dq_score: float = 1.0,
    schema_hash: str = "",
    feature_cols_hash: str = "",
) -> ConfidenceMetaGateInput:
    return ConfidenceMetaGateInput(
        sid=sid,
        symbol="BTCUSDT",
        kind="edge_stack_v1",
        side="long",
        ts_ms=1_700_000_001_000,
        now_ms=1_700_000_001_500,
        legacy_confidence=legacy_confidence,
        legacy_min_confidence=legacy_min,
        legacy_decision=legacy_decision,
        p_edge_raw=0.5,
        p_edge_cal=0.5,
        rule_score=0.6,
        have=4,
        need=3,
        spread_bps=spread_bps,
        expected_slippage_bps=slippage_bps,
        fee_bps=fee_bps,
        expected_edge_bps=expected_edge_bps,
        exec_risk_norm=0.2,
        dq_score=dq_score,
        dq_flag_count=0,
        regime="trending_bull",
        session="us",
        schema_hash=schema_hash,
        feature_cols_hash=feature_cols_hash,
        features=features or {"f0": 1.0, "f1": 1.0, "sl_bps": 20.0},
    )


def test_disabled_mode_returns_fallback() -> None:
    cfg = _build_cfg(enabled=False)
    out = decide_meta_gate(_build_input(), cfg, _build_artifact())
    assert out.decision == "FALLBACK_LEGACY"
    assert MetaGateReason.MODE_OFF.value in out.reason_codes
    assert MetaGateReason.LEGACY_FALLBACK.value in out.reason_codes
    assert not out.active


def test_legacy_only_returns_fallback() -> None:
    cfg = _build_cfg(mode=MetaGateMode.LEGACY_ONLY)
    out = decide_meta_gate(_build_input(), cfg, _build_artifact())
    assert out.decision == "FALLBACK_LEGACY"
    assert MetaGateReason.MODE_LEGACY_ONLY.value in out.reason_codes


def test_kill_switch_returns_fallback() -> None:
    cfg = _build_cfg(mode=MetaGateMode.KILL_SWITCH)
    out = decide_meta_gate(_build_input(), cfg, _build_artifact())
    assert out.decision == "FALLBACK_LEGACY"
    assert MetaGateReason.MODE_KILL_SWITCH.value in out.reason_codes


def test_model_not_loaded_fallback() -> None:
    cfg = _build_cfg(mode=MetaGateMode.SHADOW)
    out = decide_meta_gate(_build_input(), cfg, artifact=None)
    assert out.decision == "FALLBACK_LEGACY"
    assert MetaGateReason.MODEL_NOT_LOADED.value in out.reason_codes


def test_schema_mismatch_fallback() -> None:
    cfg = _build_cfg(mode=MetaGateMode.SHADOW)
    art = _build_artifact()
    inp = _build_input(feature_cols_hash="some-other-hash")
    out = decide_meta_gate(inp, cfg, art)
    assert out.decision == "FALLBACK_LEGACY"
    assert MetaGateReason.SCHEMA_MISMATCH.value in out.reason_codes


def test_calibration_ece_high_fallback() -> None:
    cfg = _build_cfg(mode=MetaGateMode.SHADOW, max_calibration_ece=0.05)
    art = _build_artifact(ece=0.20)
    # Pin now_ms close to created_ms so MODEL_STALE does not fire first.
    out = decide_meta_gate(_build_input(), cfg, art, now_ms=1_700_000_001_000)
    assert out.decision == "FALLBACK_LEGACY"
    assert MetaGateReason.CALIBRATION_ECE_HIGH.value in out.reason_codes


def test_model_stale_fallback() -> None:
    cfg = _build_cfg(mode=MetaGateMode.SHADOW)
    # created_ms 5 days back, max_age 24h → stale.
    five_days_back = 1_700_000_000_000 - 5 * 24 * 3600 * 1000
    art = _build_artifact(created_ms=five_days_back)
    out = decide_meta_gate(_build_input(), cfg, art, now_ms=1_700_000_001_000)
    assert out.decision == "FALLBACK_LEGACY"
    assert MetaGateReason.MODEL_STALE.value in out.reason_codes


def test_allow_path_in_shadow_mode_does_not_override_legacy() -> None:
    cfg = _build_cfg(mode=MetaGateMode.SHADOW)
    # High p_win via large intercept; identity calibrator → p_cal ≈ 1.
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.decision == "SHADOW_ALLOW"
    assert not out.active
    assert MetaGateReason.MODE_SHADOW.value in out.reason_codes
    assert MetaGateReason.META_ALLOW.value in out.reason_codes


def test_deny_path_in_shadow_mode_does_not_override_legacy() -> None:
    cfg = _build_cfg(mode=MetaGateMode.SHADOW)
    art = _build_artifact(intercept=-5.0, coef=(0.0, 0.0))
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.decision == "SHADOW_DENY"
    assert not out.active
    assert MetaGateReason.P_WIN_BELOW_FLOOR.value in out.reason_codes


def test_enforce_mode_returns_active_decision() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.decision == "ALLOW"
    assert out.active is True
    assert MetaGateReason.MODE_ENFORCE.value in out.reason_codes


def test_canary_selected_overrides_legacy() -> None:
    cfg = _build_cfg(mode=MetaGateMode.CANARY, canary_share=1.0)
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.active is True
    assert MetaGateReason.MODE_CANARY_SELECTED.value in out.reason_codes


def test_canary_not_selected_does_not_override_legacy() -> None:
    cfg = _build_cfg(mode=MetaGateMode.CANARY, canary_share=0.0)
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.active is False
    assert MetaGateReason.MODE_CANARY_NOT_SELECTED.value in out.reason_codes


def test_p_win_below_floor_denies() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)
    art = _build_artifact(intercept=-5.0, coef=(0.0, 0.0))  # p ≈ 0.0067
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.decision == "DENY_SOFT"
    assert MetaGateReason.P_WIN_BELOW_FLOOR.value in out.reason_codes


def test_expected_edge_below_floor_denies() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    inp = _build_input(expected_edge_bps=0.5)  # below default min 1.5
    out = decide_meta_gate(inp, cfg, art)
    assert out.decision == "DENY_SOFT"
    assert MetaGateReason.EXPECTED_EDGE_BELOW_FLOOR.value in out.reason_codes


def test_soft_caps_produce_tightened_allow() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    inp = _build_input(dq_score=0.5, spread_bps=10.0, slippage_bps=10.0)
    out = decide_meta_gate(inp, cfg, art)
    assert out.decision == "ALLOW_TIGHTENED"
    assert MetaGateReason.DQ_DEGRADED.value in out.reason_codes
    assert MetaGateReason.META_ALLOW_TIGHTENED.value in out.reason_codes


def test_features_used_in_scoring() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)
    art = _build_artifact(intercept=0.0, coef=(10.0, 10.0))
    # All features = 1 → z = 20 → p ≈ 1.0
    inp_a = _build_input(features={"f0": 1.0, "f1": 1.0, "sl_bps": 20.0})
    out_a = decide_meta_gate(inp_a, cfg, art)
    # All features = -1 → z = -20 → p ≈ 0.0
    inp_b = _build_input(features={"f0": -1.0, "f1": -1.0, "sl_bps": 20.0})
    out_b = decide_meta_gate(inp_b, cfg, art)
    assert out_a.p_win_calibrated > 0.99
    assert out_b.p_win_calibrated < 0.01


def test_platt_calibrator_applied() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)
    # Platt with a=0, b=0 collapses to constant 0.5 regardless of raw p.
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0),
                          cal_type="platt", cal_a=0.0, cal_b=0.0)
    out = decide_meta_gate(_build_input(), cfg, art)
    assert math.isclose(out.p_win_calibrated, 0.5, abs_tol=1e-6)


def test_risk_multiplier_disabled_by_default() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)  # risk_mult_enabled=False
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.risk_multiplier == 0.0


def test_risk_multiplier_enabled_returns_bucketed_value() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE, risk_mult_enabled=True)
    art = _build_artifact(intercept=5.0, coef=(0.0, 0.0))
    out = decide_meta_gate(_build_input(), cfg, art)
    assert out.risk_multiplier > 0.0
    assert out.risk_multiplier <= 1.10


@pytest.mark.parametrize("p,expected", [
    (0.40, 0.0),  # below floor
    (0.56, 0.5),
    (0.61, 0.75),
    (0.66, 1.0),
    (0.80, 1.10),
])
def test_risk_multiplier_buckets(p: float, expected: float) -> None:
    assert risk_multiplier_from_p_win(p) == expected


def test_decision_is_deterministic() -> None:
    cfg = _build_cfg(mode=MetaGateMode.ENFORCE)
    art = _build_artifact(intercept=1.0, coef=(0.5, 0.5))
    inp = _build_input()
    outs = [decide_meta_gate(inp, cfg, art) for _ in range(3)]
    p_cals = [o.p_win_calibrated for o in outs]
    decisions = [o.decision for o in outs]
    assert len(set(p_cals)) == 1
    assert len(set(decisions)) == 1
