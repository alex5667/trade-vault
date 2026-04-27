from types import SimpleNamespace

from common.decision_trace import ensure_trace, trace_gate, trace_target, build_trace_summary


def test_trace_gate_and_target_have_duration_ms():
    ctx = SimpleNamespace()
    tr = ensure_trace(ctx, sid="sid-1")
    trace_gate(ctx, stage="gates", name="regime_gate", passed=True, veto=False, reason_code="OK", duration_ms=1.25)
    trace_target(ctx, name="notify", ok=True, reason_code="OK", duration_ms=4.5)
    tr2 = ensure_trace(ctx)
    evs = tr2.get("events")
    assert isinstance(evs, list) and len(evs) >= 2
    g = [e for e in evs if e.get("type") == "gate"][-1]
    t = [e for e in evs if e.get("type") == "target"][-1]
    assert "duration_ms" in g
    assert "duration_ms" in t


def test_trace_summary_is_one_line():
    ctx = SimpleNamespace()
    ensure_trace(ctx, sid="sid-2")
    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True, reason_code="VETO_EDGE_COST", duration_ms=2.0)
    s = build_trace_summary(ensure_trace(ctx))
    assert isinstance(s, str)
    assert "\n" not in s
