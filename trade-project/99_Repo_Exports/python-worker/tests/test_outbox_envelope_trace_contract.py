from __future__ import annotations

from typing import Any

from services.outbox.envelope_builder import build_outbox_envelope
from tests._helpers.json_contract import assert_json_safe


def _has_key_recursive(obj: Any, needle: str) -> bool:
    if isinstance(obj, dict):
        if needle in obj:
            return True
        return any(_has_key_recursive(v, needle) for v in obj.values())
    if isinstance(obj, list):
        return any(_has_key_recursive(v, needle) for v in obj)
    return False


def test_build_outbox_envelope_trace_is_summary_only():
    """
    Контракт:
      - tradeable envelope НЕ содержит полный trace / events
      - envelope может содержать trace_id + trace_summary + meta.trace_meta_key
    """
    sid = "sid_test_1"

    trace: dict[str, Any] = {
        "trace_id": "trace-123",
        "events": [
            {"type": "gate", "name": "regime_gate", "passed": True, "veto": False, "duration_ms": 0.1},
            {"type": "gate", "name": "conf_min", "passed": True, "veto": False, "duration_ms": 0.2},
        ],
    }

    env = build_outbox_envelope(
        sid=sid,
        ctx=None,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
        meta={},
        trace=trace,
    )

    # Envelope JSON-safe
    assert_json_safe(env)

    # Должны быть только summary/ids (если trace используется)
    assert "trace_id" in env, "trace_id must be present when trace param is provided"
    assert "trace_summary" in env, "trace_summary must be present when trace param is provided"

    # Запрещено: полный trace / events в envelope
    assert "trace" not in env, "full trace must never be in tradeable envelope"
    assert not _has_key_recursive(env, "events"), "events must never appear anywhere inside envelope"

    # meta.trace_meta_key указывает на sidecar ключ (опционально для прямого trace параметра)
    meta = env.get("meta") or {}
    assert isinstance(meta, dict)
    # trace_meta_key is optional when trace is provided directly
