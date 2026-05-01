from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from services.outbox.envelope_builder import build_outbox_envelope


class CtxStub:
    pass


def test_build_outbox_envelope_trace_safe_no_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Жёсткий контракт:
      - envelope может иметь trace_id + trace_summary + meta.trace_meta_key
      - envelope НЕ имеет права содержать полный trace/events в tradeable payload
    """
    import services.outbox.envelope_builder as mod

    # Форсим "trace enabled" и делаем ensure_trace / serialize_trace_from_ctx детерминированными
    monkeypatch.setattr(mod, "trace_enabled", lambda: True)

    def _ensure_trace(ctx: Any, sid: str, symbol="", kind: str = "") -> Dict[str, Any]:
        return {"trace_id": "TID123", "events": [{"type": "gate", "name": "x"}]}

    monkeypatch.setattr(mod, "ensure_trace", _ensure_trace)
    monkeypatch.setattr(mod, "serialize_trace_from_ctx", lambda ctx: {"trace_id": "TID123", "events": [{"type": "gate"}]})
    monkeypatch.setattr(mod, "make_trace_summary", lambda td: "trace_summary_1line")

    ctx = CtxStub()
    env = build_outbox_envelope(
        sid="SID1",
        ctx=ctx,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
        signal_stream="signals:main",
        signal_stream_payload={"k": 1},
        audit_stream="audit:main",
        audit_payload={"a": True},
        meta={"x": 1},
    )

    assert isinstance(env, dict)
    assert env.get("sid") == "SID1"

    # Trace fields allowed (summary only)
    assert env.get("trace_id") == "TID123"
    assert env.get("trace_summary") == "trace_summary_1line"
    meta = env.get("meta") or {}
    assert isinstance(meta, dict)
    # trace_meta_key may or may not be present depending on implementation
    # assert "trace_meta_key" in meta

    # Forbidden: full trace/events in tradeable envelope
    assert "trace" not in env
    assert "events" not in env

    # Must be JSON-serializable
    json.dumps(env, ensure_ascii=False)


def test_outbox_targets_json_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Любые payload должны быть сериализуемыми (json-safe).
    """
    import services.outbox.envelope_builder as mod
    monkeypatch.setattr(mod, "trace_enabled", lambda: False)

    env = build_outbox_envelope(
        sid="SID2",
        notify_payload={"nested": {"x": 1}, "lst": [1, 2, 3]},
        meta={},
    )
    json.dumps(env, ensure_ascii=False)