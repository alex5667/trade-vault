"""Tests for services/gate_value_reporter/normalize.py."""

from __future__ import annotations

import json

from services.gate_value_reporter.normalize import (
    build_ml_confirm_by_sid,
    group_key,
    normalize_gated_out_outcome,
    normalize_passed_label,
    normalize_sid,
)


def _passed_label_payload(
    *,
    sid: str = "crypto-of:BTCUSDT:1700000000000",
    y_edge: int = 1,
    r_mult: float = 1.2,
    primary: int = 1,
    symbol: str = "BTCUSDT",
    direction: str = "LONG",
    h_ms: int = 1_800_000,
    tp_bps: float = 15.0,
    sl_bps: float = 10.0,
    ret_bps: float = 18.0,
    entry_px: float = 50000.0,
) -> dict:
    return {
        "payload": json.dumps(
            {
                "sid": sid,
                "y_edge": y_edge,
                "r_mult": r_mult,
                "primary": primary,
                "symbol": symbol,
                "direction": direction,
                "h_ms": h_ms,
                "tp_bps": tp_bps,
                "sl_bps": sl_bps,
                "ret_bps": ret_bps,
                "entry_px": entry_px,
            }
        )
    }


def _ml_entry(
    *,
    sid: str = "crypto-of:BTCUSDT:1700000000000",
    kind: str = "edge_stack_v1",
    p_edge: float = 0.72,
) -> tuple[str, dict]:
    return ("1700000000000-0", {"sid": sid, "kind": kind, "p_edge_cal": str(p_edge)})


def test_normalize_sid_canonicalises_kind_prefix() -> None:
    assert normalize_sid("of:BTCUSDT:1700000000000") == "crypto-of:BTCUSDT:1700000000000"
    assert (
        normalize_sid("iceberg:ETHUSDT:1700000000000:LONG")
        == "crypto-of:ETHUSDT:1700000000000"
    )


def test_normalize_sid_empty_and_short() -> None:
    assert normalize_sid("") == ""
    assert normalize_sid(None) == ""
    assert normalize_sid("plain_string") == "plain_string"


def test_normalize_passed_primary_label() -> None:
    ml = build_ml_confirm_by_sid([_ml_entry()])
    rec = normalize_passed_label(
        _passed_label_payload(), ml, source_stream="labels:tb"
    )
    assert rec is not None
    assert rec.cohort == "passed"
    assert rec.symbol == "BTCUSDT"
    assert rec.kind == "edge_stack_v1"
    assert rec.side == "LONG"
    assert rec.r_mult == 1.2
    assert rec.y == 1
    assert rec.tp_hit is True
    assert rec.sl_hit is False
    assert rec.outcome_reason == "tp"
    assert rec.p_edge == 0.72
    assert rec.horizon_ms == 1_800_000


def test_normalize_passed_ignores_non_primary() -> None:
    ml = build_ml_confirm_by_sid([_ml_entry()])
    fields = _passed_label_payload(primary=0)
    assert normalize_passed_label(fields, ml, source_stream="labels:tb") is None


def test_normalize_passed_missing_payload() -> None:
    assert normalize_passed_label({}, {}, source_stream="labels:tb") is None
    assert (
        normalize_passed_label({"payload": "not-json"}, {}, source_stream="labels:tb")
        is None
    )


def test_normalize_passed_sl_hit_from_r_mult() -> None:
    ml = build_ml_confirm_by_sid([_ml_entry()])
    fields = _passed_label_payload(y_edge=0, r_mult=-1.0)
    rec = normalize_passed_label(fields, ml, source_stream="labels:tb")
    assert rec is not None
    assert rec.y == 0
    assert rec.tp_hit is False
    assert rec.sl_hit is True
    assert rec.outcome_reason == "sl"


def test_normalize_passed_kind_from_label_when_ml_missing() -> None:
    fields = _passed_label_payload()
    payload = json.loads(fields["payload"])
    payload["kind"] = "delta_spike"
    fields["payload"] = json.dumps(payload)
    rec = normalize_passed_label(fields, {}, source_stream="labels:tb")
    assert rec is not None
    assert rec.kind == "delta_spike"
    assert rec.p_edge is None


def test_normalize_gated_out_outcome_happy_path() -> None:
    fields = {
        "sid": "crypto-of:BTCUSDT:1700000000000",
        "symbol": "BTCUSDT",
        "direction": "SHORT",
        "entry": "50000.0",
        "ts_ms": "1700000000000",
        "horizon_ms": "1800000",
        "tp_bps": "15",
        "sl_bps": "10",
        "ret_bps": "-9.5",
        "r_mult": "-0.95",
        "y": "0",
        "tp_hit": "0",
        "sl_hit": "1",
        "confidence": "0.41",
        "p_edge": "0.33",
    }
    rec = normalize_gated_out_outcome(
        fields, source_stream="stream:signals:gated_out_outcomes"
    )
    assert rec is not None
    assert rec.cohort == "gated_out"
    assert rec.side == "SHORT"
    assert rec.r_mult == -0.95
    assert rec.y == 0
    assert rec.tp_hit is False
    assert rec.sl_hit is True
    assert rec.outcome_reason == "sl"
    assert rec.p_edge == 0.33
    assert rec.confidence == 0.41


def test_normalize_gated_out_outcome_timeout() -> None:
    fields = {
        "sid": "x:y:1",
        "symbol": "ETHUSDT",
        "tp_hit": "0",
        "sl_hit": "0",
        "y": "0",
        "r_mult": "0.1",
        "ts_ms": "1",
        "horizon_ms": "60000",
    }
    rec = normalize_gated_out_outcome(fields, source_stream="s")
    assert rec is not None
    assert rec.outcome_reason == "timeout"
    assert rec.p_edge is None  # absent in fields → None (not 0.0)
    assert rec.confidence is None


def test_normalize_gated_out_outcome_missing_sid() -> None:
    assert normalize_gated_out_outcome({"symbol": "X"}, source_stream="s") is None


def test_group_key_buckets_tp_sl_to_5bps() -> None:
    from services.gate_value_reporter.contracts import GateOutcomeRecord

    rec = GateOutcomeRecord(
        sid="s",
        cohort="passed",
        symbol="BTCUSDT",
        kind="edge_stack_v1",
        side="LONG",
        ts_ms=1,
        horizon_ms=60_000,
        entry_px=0.0,
        tp_bps=13.2,
        sl_bps=9.6,
        ret_bps=0.0,
        r_mult=0.0,
        y=0,
        tp_hit=False,
        sl_hit=False,
        outcome_reason="timeout",
    )
    assert group_key(rec) == ("BTCUSDT", "edge_stack_v1", 60_000, 15, 10)


def test_build_ml_confirm_by_sid_latest_wins() -> None:
    e1 = _ml_entry(sid="of:BTCUSDT:1", kind="k1", p_edge=0.1)
    e2 = _ml_entry(sid="of:BTCUSDT:1", kind="k2", p_edge=0.9)
    out = build_ml_confirm_by_sid([e1, e2])
    assert "crypto-of:BTCUSDT:1" in out
    assert out["crypto-of:BTCUSDT:1"]["kind"] == "k2"
    assert out["crypto-of:BTCUSDT:1"]["p_edge"] == 0.9
