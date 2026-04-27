from common.decision_trace import patch_trace_sidecar_obj, make_trace_summary


def test_patch_trace_sidecar_adds_events_and_updates_summary():
    side = {
        "schema": "decision_trace_sidecar_v1",
        "trace_id": "t1",
        "trace": {"v": 1, "trace_id": "t1", "events": []},
    }
    patch = [
        {"type": "gate", "name": "regime_gate", "passed": True, "veto": False, "reason_code": "OK", "duration_ms": 1.0},
        {"type": "target", "target": "notify", "ok": True, "attempt": 1, "duration_ms": 12.0, "reason_code": "OK"},
    ]
    out = patch_trace_sidecar_obj(side, patch)
    assert "trace_summary" in out
    assert "notify" in out["trace_summary"]
    assert len(out["trace"]["events"]) == 2


def test_make_trace_summary_is_single_line():
    tr = {"v": 1, "events": [{"type": "gate", "name": "regime_gate", "passed": True, "veto": False, "reason_code": "OK", "duration_ms": 1.2}]}
    s = make_trace_summary(tr)
    assert "\n" not in s
    assert len(s) > 0