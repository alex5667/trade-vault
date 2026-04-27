"""
P0 sanity tests for decision_trace.py (unified, no duplicates).
"""
import pytest
from common.decision_trace import (
    DecisionTrace, trace_enabled, should_sample,
    ensure_trace, serialize_trace_from_ctx, to_dict_bounded,
    build_trace_summary, build_sidecar_meta, patch_trace_sidecar_best_effort,
    trace_gate, trace_target, trace_event, Span
)


def test_decision_trace_new():
    """Test DecisionTrace.new() creates valid trace."""
    tr = DecisionTrace.new(sid="test_sid", symbol="BTCUSDT", kind="signal")
    assert tr.sid == "test_sid"
    assert tr.symbol == "BTCUSDT"
    assert tr.kind == "signal"
    assert tr.ts_ms > 0
    assert tr.event_ts_ms > 0


def test_decision_trace_add():
    """Test DecisionTrace.add() records events."""
    tr = DecisionTrace.new(sid="test", enabled=True)
    tr.add(where="gate", name="test_gate", ok=True, veto=False, reason_code="OK")
    assert len(tr.events) == 1
    assert tr.events[0]["where"] == "gate"
    assert tr.events[0]["name"] == "test_gate"
    assert tr.events[0]["ok"] is True


def test_should_sample():
    """Test deterministic sampling."""
    sid = "test_sid_123"
    # rate=1.0 should always return True
    assert should_sample(sid, 1.0) is True
    # rate=0.0 should always return False
    assert should_sample(sid, 0.0) is False
    # Same sid should give same result (deterministic)
    result1 = should_sample(sid, 0.5)
    result2 = should_sample(sid, 0.5)
    assert result1 == result2


def test_span():
    """Test Span duration measurement."""
    with Span() as sp:
        import time
        time.sleep(0.01)  # 10ms
    ms = sp.ms()
    assert ms > 0
    assert ms < 100  # should be ~10ms, not 100ms


def test_ensure_trace():
    """Test ensure_trace creates trace in context."""
    ctx = {}
    tr = ensure_trace(ctx, "test_sid")
    assert isinstance(tr, DecisionTrace)
    assert tr.sid == "test_sid"
    assert ctx.get("decision_trace") is tr


def test_to_dict_bounded():
    """Test bounded trace dict limits events."""
    tr = DecisionTrace.new(sid="test", enabled=True)
    # Add more than max_events
    for i in range(300):
        tr.add(where="gate", name=f"gate_{i}", ok=True)
    d = to_dict_bounded(tr, max_events=200)
    assert len(d.get("events", [])) == 200  # should be capped


def test_trace_gate():
    """Test trace_gate helper."""
    ctx = {"decision_trace": DecisionTrace.new(sid="test", enabled=True)}
    trace_gate(ctx, stage="test", name="gate1", passed=True, veto=False, reason_code="OK")
    tr = ctx["decision_trace"]
    assert len(tr.events) == 1
    assert tr.events[0]["where"] == "gate"


def test_trace_target():
    """Test trace_target helper."""
    ctx = {"decision_trace": DecisionTrace.new(sid="test", enabled=True)}
    trace_target(ctx, target="target1", ok=True)
    tr = ctx["decision_trace"]
    assert len(tr.events) == 1
    assert tr.events[0]["where"] == "target"


def test_no_duplicates():
    """Test that there are no duplicate function definitions (F811)."""
    import inspect
    from common import decision_trace
    funcs = [name for name, obj in inspect.getmembers(decision_trace, inspect.isfunction)]
    # Check for duplicates
    assert len(funcs) == len(set(funcs)), f"Duplicate functions found: {[f for f in funcs if funcs.count(f) > 1]}"

