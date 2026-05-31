import copy

import pytest

from services.outbox.envelope_builder import build_outbox_envelope

hypothesis = pytest.importorskip("hypothesis")
st = pytest.importorskip("hypothesis.strategies")


def _json_scalars():
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-10**9, max_value=10**9),
        st.floats(allow_nan=False, allow_infinity=False, width=32),
        st.text(max_size=64),
    )


json_value = st.recursive(
    _json_scalars(),
    lambda ch: st.one_of(
        st.lists(ch, max_size=8),
        st.dictionaries(st.text(max_size=32), ch, max_size=8),
    ),
    max_leaves=40,
)


@hypothesis.given(meta=json_value, events=st.lists(st.dictionaries(st.text(max_size=16), json_value, max_size=8), max_size=20))
@hypothesis.settings(max_examples=150, deadline=None)
def test_envelope_never_contains_full_trace(meta, events):
    # inject forbidden keys intentionally
    meta2 = copy.deepcopy(meta)
    if isinstance(meta2, dict):
        meta2["decision_trace"] = {"events": events}
        meta2["events"] = events
        meta2["trace"] = {"events": events}

    tr = {"trace_id": "tid123", "sid": "sid123", "events": events}
    env = build_outbox_envelope(
        sid="sid123",
        kind="k",
        symbol="BTCUSDT",
        notify_payload={"x": 1},
        meta=meta2 if isinstance(meta2, dict) else {"m": "x"},
        trace=tr,
    )

    # tradeable envelope must not contain full diagnostics
    s = str(env)
    assert "decision_trace" not in s
    assert '"events"' not in s  # hard check: no events in envelope
    assert "trace\":" not in s
