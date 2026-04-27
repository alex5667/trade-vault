import json

from hypothesis import given, settings, strategies as st

from common.decision_trace import DecisionTrace, to_dict_bounded


@settings(max_examples=200, deadline=None)
@given(
    n=st.integers(min_value=0, max_value=400),
    big=st.text(min_size=0, max_size=4096),
)
def test_to_dict_bounded_caps_events_and_bytes(n, big):
    tr = DecisionTrace.new(sid="sid_x")
    tr.trace_id = "tid_x"
    for i in range(int(n)):
        tr.events.append(
            {
                "type": "gate",
                "stage": "g",
                "name": f"e{i}",
                "veto": False,
                "passed": True,
                "reason_code": "OK",
                "details": big,  # potentially huge string
            }
        )

    out = to_dict_bounded(tr, max_events=64, max_bytes=16_000)
    assert isinstance(out, dict)
    evs = out.get("events")
    if isinstance(evs, list):
        assert len(evs) <= 64

    raw = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "ignore")
    assert len(raw) <= 16_000 or out.get("trace_too_large") is True