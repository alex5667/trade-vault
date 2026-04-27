import json

from hypothesis import given, settings
from hypothesis import strategies as st

from common.decision_trace import DecisionTrace, to_dict_bounded


@settings(max_examples=60, deadline=None)
@given(n=st.integers(min_value=0, max_value=2000))
def test_to_dict_bounded_caps_size_and_events(n: int):
    tr = DecisionTrace.new(sid="sid_bounded")
    for i in range(n):
        tr.add(where="g", name="gate", ok=True, veto=False, reason_code="OK", metrics={"i": i}, duration_ms=0.01)
    d = to_dict_bounded(tr, max_events=64, max_bytes=16_000)
    s = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
    assert len(s.encode("utf-8", "ignore")) <= 16_000
    evs = d.get("events")
    if isinstance(evs, list):
        assert len(evs) <= 64