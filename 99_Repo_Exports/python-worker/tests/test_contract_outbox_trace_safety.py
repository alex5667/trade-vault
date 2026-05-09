from __future__ import annotations

from types import SimpleNamespace

from common.json_contract import assert_json_safe


def test_build_outbox_envelope_trace_is_summary_only(monkeypatch):
    # импорт внутри теста, чтобы monkeypatch работал на модульные символы
    import services.outbox.envelope_builder as eb

    # force trace_enabled=True
    monkeypatch.setattr(eb, "trace_enabled", lambda: True)

    class MockTrace:
        pass

    # stub ensure_trace + set_summary_fields
    def _ensure_trace(ctx, **kwargs):
        return MockTrace()

    def _set_summary_fields(env, tr):
        env["trace_id"] = "T123"
        env["trace_summary"] = "gates:edge=OK"

    monkeypatch.setattr(eb, "ensure_trace", _ensure_trace)
    monkeypatch.setattr(eb, "set_summary_fields", _set_summary_fields)


    ctx = SimpleNamespace()

    env = eb.build_outbox_envelope(
        sid="SID1",
        ctx=ctx,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
        meta={"foo": "bar"},
    )

    # 1) envelope json-safe
    assert_json_safe(env)

    # 2) tradeable envelope НЕ должен содержать полный trace
    assert "trace" not in env
    assert "events" not in env

    # 3) summary ok
    assert env.get("trace_id") == "T123"
    assert env.get("trace_summary") == "gates:edge=OK"

    # 4) meta содержит trace_meta_key (sidecar), и не содержит trace blob
    meta = env.get("meta") or {}
    assert isinstance(meta, dict)
    assert "trace_meta_key" in meta
    assert "trace" not in meta
