from __future__ import annotations

from domain.evidence_keys import CtxKeys
from services.orderflow.ofc_contextual_decision_writer_v1 import _normalize_row


def test_normalize_row_accepts_ctx_event():
    row, reason = _normalize_row(
        {
            "sid": "s1",
            "symbol": "BTCUSDT",
            "direction": "long",
            "decision_ts_ms": 1700000000000,
            "ok": 1,
            "reason": "score_veto",
            "ctx_enabled": True,
            "ctx_mode": "shadow",
            "ctx_key": "symbol=BTCUSDT|session=eu",
            "ctx_bundle_ver": "b1",
            "ctx_p_rule_raw": 0.62,
            "ctx_p_rule_cal": 0.59,
            "ctx_cost_p50_bps": 1.1,
            "ctx_cost_p90_bps": 1.8,
            "ctx_exec_risk_ref_bps": 4.0,
            "ctx_edge_net_p50_bps": 0.4,
            "ctx_edge_net_p90_bps": -0.8,
            "ctx_reason": "shadow_only",
            "ctx_fallback_level": "global",
            "ctx_shadow_disagree": True,
            "ctx_infer_latency_us": 210,
        }
    )
    assert reason == ""
    assert row is not None
    assert row["sid"] == "s1"
    assert row["ctx_enabled"] is True
    assert row[CtxKeys.MODE] == "shadow"
    assert row[CtxKeys.SHADOW_DISAGREE] is True


def test_normalize_row_skips_non_ctx_event():
    row, reason = _normalize_row({"sid": "s1", "symbol": "BTCUSDT", "decision_ts_ms": 1700000000000})
    assert row is None
    assert reason == "skip:no_ctx"
