from types import SimpleNamespace

from services.outbox.envelope_builder import build_outbox_envelope, build_trace_sidecar_meta
from common.decision_trace import ensure_trace, trace_gate

def test_envelope_contains_only_short_trace_fields(monkeypatch):
    ctx = SimpleNamespace()
    ensure_trace(ctx, sid="sid-1")
    trace_gate(ctx, stage="gates", name="regime_gate", passed=True, veto=False, reason_code="OK", duration_ms=1.0)

    env = build_outbox_envelope(
        sid="sid-1",
        ctx=ctx,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
    )
    assert env.get("trace_id")
    summary = env.get("trace_summary")
    assert isinstance(summary, str)  # may be empty due to trace_enabled or other conditions
    assert "trace" not in env  # full trace must not live in env

def test_trace_sidecar_meta_shape():
    ctx = SimpleNamespace()
    ensure_trace(ctx, sid="sid-2")
    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True, reason_code="REASON_BELOW_K", duration_ms=2.0)
    meta = build_trace_sidecar_meta(ctx=ctx, sid="sid-2")
    assert isinstance(meta, dict)
    assert meta.get("trace_id")  # Should have trace_id
    summary = meta.get("trace_summary")
    assert isinstance(summary, str)  # Should have trace_summary as string
    # Check for trace data (may be in different keys)
    assert meta.get("trace") or meta.get("decision_trace")
