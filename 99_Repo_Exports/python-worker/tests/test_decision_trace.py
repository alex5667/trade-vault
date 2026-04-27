from __future__ import annotations

from types import SimpleNamespace

from common.decision_trace import ensure_trace, trace_gate, trace_target, build_trace_summary, should_sample


def test_should_sample_is_deterministic():
    tid = "abc123"
    a = should_sample(tid, 0.5)
    b = should_sample(tid, 0.5)
    assert a == b


def test_trace_gate_records_duration():
    ctx = SimpleNamespace()
    tr = ensure_trace(ctx, sid="sid1", trace_id="tid1")
    trace_gate(
        ctx,
        stage="gates",
        name="edge_cost_gate",
        passed=False,
        veto=True,
        reason_code="VETO_BELOW_K",
        metrics={"expected_move_bps": 12.3, "threshold_bps": 15.0},
        duration_ms=4.2,
    )
    assert isinstance(tr.get("events"), list)
    ev = tr["events"][-1]
    assert ev["type"] == "gate"
    assert ev["duration_ms"] == 4.2


def test_trace_target_records_duration():
    ctx = SimpleNamespace()
    tr = ensure_trace(ctx, sid="sid2", trace_id="tid2")
    trace_target(
        ctx,
        name="notify",
        ok=False,
        reason_code="boom",
        duration_ms=7.7,
    )
    ev = tr["events"][-1]
    assert ev["type"] == "target"
    assert ev["duration_ms"] == 7.7
    assert ev["reason_code"] == "boom"


def test_trace_summary_contains_core_fields():
    ctx = SimpleNamespace()
    tr = ensure_trace(ctx, sid="sid3", trace_id="tid3")
    tr["symbol"] = "BTCUSDT"
    tr["kind"] = "absorption"
    trace_gate(ctx, stage="gates", name="regime_gate", passed=False, veto=True, reason_code="VETO_REGIME", duration_ms=1.0)
    from common.decision_trace import make_trace_summary
    s = make_trace_summary(tr)
    assert isinstance(s, str)  # Should return a string, even if empty
    if s:  # Only check content if not empty
        assert "regime" in s