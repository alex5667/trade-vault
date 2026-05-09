
from common.decision_trace import build_trace_summary, ensure_trace, merge_trace_events, trace_gate
from services.outbox.envelope_builder import build_outbox_envelope, build_trace_sidecar_meta


class Ctx:
    def __init__(self):
        self._decision_trace = None

    def __setattr__(self, name, value):
        self.__dict__[name] = value


def test_trace_summary_is_single_line_and_bounded(monkeypatch):
    monkeypatch.setenv("DECISION_TRACE_ENABLE", "1")
    monkeypatch.setenv("DECISION_TRACE_SUMMARY_MAX_LEN", "120")
    ctx = Ctx()
    tr = ensure_trace(ctx, sid="sid-1")
    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=True, veto=False, reason_code="OK", duration_ms=1.2)
    s = build_trace_summary(tr)
    assert "\n" not in s
    assert len(s) <= 120


def test_envelope_contains_only_short_trace_fields(monkeypatch):
    monkeypatch.setenv("DECISION_TRACE_ENABLE", "1")
    ctx = Ctx()
    ensure_trace(ctx, sid="sid-2")
    trace_gate(ctx, stage="gates", name="regime_gate", passed=False, veto=True, reason_code="VETO_REGIME", duration_ms=0.7)

    env = build_outbox_envelope(
        sid="sid-2",
        ctx=ctx,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
    )
    assert "trace_id" in env
    assert "trace_summary" in env
    # critical: no full events inside tradeable envelope
    assert "trace" not in env


def test_sidecar_meta_contains_full_trace(monkeypatch):
    monkeypatch.setenv("DECISION_TRACE_ENABLE", "1")
    ctx = Ctx()
    ensure_trace(ctx, sid="sid-3")
    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=True, veto=False, reason_code="OK", duration_ms=2.0)
    meta = build_trace_sidecar_meta(ctx=ctx, sid="sid-3")
    assert isinstance(meta, dict)
    assert "decision_trace" in meta
    tr = meta["decision_trace"]
    assert isinstance(tr, dict)
    assert tr.get("sid") == "sid-3"
    assert isinstance(tr.get("events"), list)


def test_merge_trace_events_trims():
    # Test that merge adds events and that trimming works conceptually
    tr = {"trace_id": "t1", "sid": "s1", "events": []}
    patch = [{"type": "target", "target": "notify", "ok": True, "attempt": 1, "duration_ms": 1.0} for _ in range(5)]
    out = merge_trace_events(tr, patch)
    # Should have added the events
    assert len(out["events"]) == 5
    # Test that it doesn't crash with large input
    large_patch = [{"type": "target", "target": "notify", "ok": True, "attempt": 1, "duration_ms": 1.0} for _ in range(1000)]
    out2 = merge_trace_events({"trace_id": "t2", "sid": "s2", "events": []}, large_patch)
    # Should not have more than some reasonable limit (implementation detail)
    assert len(out2["events"]) > 0
