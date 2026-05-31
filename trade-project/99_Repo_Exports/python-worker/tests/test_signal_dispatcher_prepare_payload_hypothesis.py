from __future__ import annotations

from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from services.dispatch.dispatcher_app import SignalDispatcher



@given(
    payload=st.dictionaries(
        keys=st.text(min_size=1, max_size=10),
        values=st.one_of(st.integers(), st.text(max_size=20), st.none(), st.booleans()),
        max_size=30,
    ),
    sid=st.text(min_size=1, max_size=20),
    tid=st.text(min_size=1, max_size=20),
)
def test_prepare_target_payload_no_inplace_mutation(payload: dict, sid: str, tid: str) -> None:
    sd = SignalDispatcher.__new__(SignalDispatcher)
    original = dict(payload)
    out = sd._prepare_target_payload(payload, sid=sid, trace_id=tid)
    assert payload == original  # input unchanged
    assert isinstance(out, dict)
    # inserted only if missing
    if "sid" in original:
        assert out["sid"] == original["sid"]
    else:
        assert out["sid"] == sid
    if "trace_id" in original:
        assert out["trace_id"] == original["trace_id"]
    else:
        assert out["trace_id"] == tid
