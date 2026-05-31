import json

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st

from common.decision_trace import DecisionTrace, to_dict_bounded


@given(k=st.integers(min_value=0, max_value=500))
def test_to_dict_bounded_caps_events_and_bytes(k):
    tr = DecisionTrace.new(sid="sid_x")
    tr.trace_id = "tid_x"
    for i in range(k):
        tr.events.append(
            {
                "type": "gate",
                "stage": "g",
                "name": f"e{i}",
                "veto": False,
                "passed": True,
                "reason_code": "OK",
                "details": "big_string" * 100,  # potentially huge string
            }
        )

    out = to_dict_bounded(tr, max_events=64, max_bytes=16_000)
    assert isinstance(out, dict)
    evs = out.get("events")
    if isinstance(evs, list):
        assert len(evs) <= 64

    raw = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "ignore")
    assert len(raw) <= 16_000 or out.get("trace_too_large") is True
