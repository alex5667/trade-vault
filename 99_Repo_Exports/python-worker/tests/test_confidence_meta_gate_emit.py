"""Plan 1 — Prometheus metrics + Redis stream emit smoke tests.

We don't assert exact label cardinality (the registry is process-global and
shared with other suites). We do assert that emit_decision never raises and
that the in-memory Redis fake receives the expected XADD with required keys.
"""
from __future__ import annotations

import json
from typing import Any

from services.confidence_meta_gate.config import MetaGateMode
from services.confidence_meta_gate.dto import (
    ConfidenceMetaGateInput,
    ConfidenceMetaGateOutput,
)
from services.confidence_meta_gate.metrics import emit_decision
from services.confidence_meta_gate.reason_codes import MetaGateReason


def _build_cfg():
    from services.confidence_meta_gate.config import MetaGateConfig
    return MetaGateConfig(
        enabled=True,
        mode=MetaGateMode.SHADOW,
        model_path="/dev/null",
        calibrator_path="/dev/null",
        canary_share=0.0,
        canary_salt="emit-salt",
        fail_mode="LEGACY",
        max_model_age_hours=24.0,
        max_calibration_ece=0.07,
        min_p_win=0.56,
        min_expected_r=0.02,
        min_expected_edge_bps=1.5,
        dq_soft_cap=0.7,
        spread_soft_cap_bps=6.0,
        slippage_soft_cap_bps=6.0,
        risk_mult_enabled=False,
        metrics_stream="m",
        decision_stream="stream:test:meta",
        sample_features_in_stream=False,
    )


def _build_io():
    inp = ConfidenceMetaGateInput(
        sid="sid-emit-1",
        symbol="BTCUSDT",
        kind="edge_stack_v1",
        side="long",
        ts_ms=1_700_000_000_000,
        now_ms=1_700_000_000_500,
        legacy_confidence=0.5,
        legacy_min_confidence=0.7,
        legacy_decision="DENY",
        p_edge_raw=0.5,
        p_edge_cal=0.5,
        rule_score=0.6,
        have=3, need=3,
        spread_bps=2.0,
        expected_slippage_bps=2.0,
        fee_bps=1.0,
        expected_edge_bps=5.0,
        exec_risk_norm=0.2,
        dq_score=1.0,
        dq_flag_count=0,
        regime="trending_bull",
        session="us",
        schema_hash="schema-v1",
        feature_cols_hash="cols-v1",
        features={"f0": 1.0},
    )
    out = ConfidenceMetaGateOutput(
        sid="sid-emit-1",
        model_ver="emit-v1",
        mode="SHADOW",
        decision="SHADOW_ALLOW",
        active=False,
        p_win_raw=0.61,
        p_win_calibrated=0.58,
        p_win_floor=0.56,
        expected_r=0.04,
        expected_edge_bps=5.0,
        risk_multiplier=0.0,
        canary_bucket=321,
        canary_selected=False,
        reason_codes=[
            MetaGateReason.MODE_SHADOW.value,
            MetaGateReason.PROBABILITY_OK.value,
            MetaGateReason.EDGE_OK.value,
            MetaGateReason.META_ALLOW.value,
        ],
        latency_ms=1.8,
    )
    return inp, out


class _FakeRedis:
    """Minimal sync XADD recorder."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def xadd(self, stream: str, fields: dict[str, Any], **kwargs: Any) -> str:
        self.calls.append((stream, fields))
        return "0-1"


def test_emit_without_redis_does_not_raise() -> None:
    inp, out = _build_io()
    cfg = _build_cfg()
    emit_decision(inp, out, cfg, active_decision="DENY", redis_client=None)


def test_emit_writes_to_redis_stream() -> None:
    inp, out = _build_io()
    cfg = _build_cfg()
    fake = _FakeRedis()
    emit_decision(inp, out, cfg, active_decision="DENY", redis_client=fake)
    assert len(fake.calls) == 1
    stream, fields = fake.calls[0]
    assert stream == "stream:test:meta"
    payload = json.loads(fields["payload"])
    assert payload["sid"] == "sid-emit-1"
    assert payload["meta_decision"] == "SHADOW_ALLOW"
    assert payload["active_decision"] == "DENY"
    assert payload["mode"] == "SHADOW"
    assert payload["canary_bucket"] == 321
    assert payload["canary_selected"] is False
    assert payload["p_win_calibrated"] == 0.58
    assert MetaGateReason.META_ALLOW.value in payload["reason_codes"]
    # Default config does not include the features payload.
    assert "features" not in payload


def test_emit_includes_features_when_configured() -> None:
    from services.confidence_meta_gate.config import MetaGateConfig
    inp, out = _build_io()
    cfg = MetaGateConfig(**{**_build_cfg().__dict__, "sample_features_in_stream": True})
    fake = _FakeRedis()
    emit_decision(inp, out, cfg, active_decision="DENY", redis_client=fake)
    payload = json.loads(fake.calls[0][1]["payload"])
    assert payload["features"] == {"f0": 1.0}


def test_emit_swallows_redis_exception() -> None:
    class BoomRedis(_FakeRedis):
        def xadd(self, *a: Any, **kw: Any) -> str:  # noqa: D401
            raise RuntimeError("redis down")

    inp, out = _build_io()
    cfg = _build_cfg()
    # Must not raise even though XADD throws.
    emit_decision(inp, out, cfg, active_decision="DENY", redis_client=BoomRedis())
