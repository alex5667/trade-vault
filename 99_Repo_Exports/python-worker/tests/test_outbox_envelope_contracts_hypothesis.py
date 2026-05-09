from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from services.outbox.envelope_builder import build_outbox_envelope


def _walk_no_forbidden(obj, forbid):
    if isinstance(obj, dict):
        for k, v in obj.items():
            assert str(k) not in forbid
            _walk_no_forbidden(v, forbid)
    elif isinstance(obj, list):
        for x in obj:
            _walk_no_forbidden(x, forbid)


json_leaf = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10_000, max_value=10_000),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(min_size=0, max_size=64),
)

json_obj = st.recursive(
    json_leaf,
    lambda ch: st.one_of(
        st.lists(ch, min_size=0, max_size=6),
        st.dictionaries(st.text(min_size=0, max_size=16), ch, min_size=0, max_size=6),
    ),
    max_leaves=40,
)


@settings(max_examples=200, deadline=None)
@given(
    sid=st.text(min_size=1, max_size=24),
    meta=json_obj,
    notify=json_obj,
)
def test_envelope_contains_only_trace_summary_not_events(sid, meta, notify):
    # pass a dict-like "trace" that contains events -> envelope MUST NOT embed them
    trace = {"trace_id": "t123", "sid": sid, "events": [{"type": "gate", "name": "x"}]}
    env = build_outbox_envelope(
        sid=sid,
        kind="k",
        symbol="BTCUSDT",
        notify_payload={"data": notify} if isinstance(notify, dict) else {"data": str(notify)},
        meta=meta if isinstance(meta, dict) else {"m": str(meta)},
        trace=trace,  # summary only
    )
    assert "trace_id" in env
    assert "trace_summary" in env
    # hard guarantee: no full trace/events in tradeable envelope
    _walk_no_forbidden(env, {"events", "decision_trace", "trace"})
