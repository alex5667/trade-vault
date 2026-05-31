from common.decision_trace import make_trace_summary


def test_make_trace_summary_empty():
    s = make_trace_summary({"trace_id": "t1", "sid": "s1", "events": []})
    assert s == "trace:empty"

def test_make_trace_summary_counts_and_last_veto():
    tr = {
        "trace_id": "t2",
        "sid": "s2",
        "events": [
            {"type": "gate", "name": "regime_gate", "passed": True, "veto": False, "reason_code": "OK", "duration_ms": 1.2},
            {"type": "gate", "name": "edge_cost_gate", "passed": False, "veto": True, "reason_code": "REASON_BELOW_K", "duration_ms": 2.0},
            {"type": "target", "name": "notify", "ok": True, "reason_code": "OK", "duration_ms": 3.0},
        ],
    }
    s = make_trace_summary(tr)
    assert isinstance(s, str)
    assert "regime=OK(1.2ms)" in s
    assert "edge=REASON_BELOW_K(2.0ms)" in s
