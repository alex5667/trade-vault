from types import SimpleNamespace

from common.decision_trace import ensure_trace, trace_gate
from services.outbox.envelope_builder import build_outbox_envelope, build_trace_sidecar_meta


def test_envelope_contains_only_short_trace_fields():
    ctx = SimpleNamespace()
    ensure_trace(ctx, sid="sid-x", trace_id="tid-x")
    trace_gate(ctx, stage="gates", name="regime", passed=True, veto=False, reason_code="OK", duration_ms=0.3)
    env = build_outbox_envelope(
        sid="sid-x",
        ctx=ctx,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
        signal_stream="stream:s",
        signal_stream_payload={"a": 1},
    )
    assert env.get("trace_id") == "tid-x"
    assert isinstance(env.get("trace_summary"), str)
    # full trace must NOT be in env (goes to sidecar meta-key)
    assert "trace" not in env
    assert "decision_trace" not in env


def test_sidecar_meta_contains_full_trace():
    ctx = SimpleNamespace()
    ensure_trace(ctx, sid="sid-y", trace_id="tid-y")
    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=True, veto=False, reason_code="OK", duration_ms=1.0)
    meta = build_trace_sidecar_meta(ctx=ctx, sid="sid-y")
    assert isinstance(meta, dict)
    assert meta.get("trace_id") == "tid-y"
    dt = meta.get("decision_trace")
    assert isinstance(dt, dict)
    assert isinstance(dt.get("events"), list) and len(dt["events"]) >= 1
