from types import SimpleNamespace

from common.decision_trace import ensure_trace, trace_gate
from services.outbox.envelope_builder import build_outbox_envelope, build_trace_sidecar_meta


def test_build_outbox_envelope_puts_only_trace_id_and_summary(monkeypatch):
    monkeypatch.setenv("DECISION_TRACE_ENABLED", "1")
    ctx = SimpleNamespace()
    ensure_trace(ctx, sid="sid1", trace_id="tid1")
    trace_gate(ctx, stage="gates", name="regime_gate", passed=True, veto=False, reason_code="OK", duration_ms=1.0)

    env = build_outbox_envelope(sid="sid1", ctx=ctx, kind="breakout", symbol="BTCUSDT", notify_payload={"text": "x"})
    assert "trace_id" in env
    assert "trace_summary" in env
    # полный trace в env НЕ кладём
    assert "trace" not in env


def test_build_trace_sidecar_meta_contains_full_trace(monkeypatch):
    monkeypatch.setenv("DECISION_TRACE_ENABLED", "1")
    ctx = SimpleNamespace()
    ensure_trace(ctx, sid="sid2", trace_id="tid2")
    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True, reason_code="REASON_BELOW_K", duration_ms=1.1)
    meta = build_trace_sidecar_meta(ctx=ctx, sid="sid2")
    assert "decision_trace" in meta  # sidecar stores trace under decision_trace key
    assert isinstance(meta["decision_trace"].get("events"), list)
