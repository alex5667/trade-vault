from __future__ import annotations

import json

from hypothesis import given, settings
from hypothesis import strategies as st

from services.outbox.envelope_builder import build_outbox_envelope

FORBIDDEN_TOP_KEYS = {"trace", "events", "decision_trace", "payload_meta", "parts_full", "raw_trace"}


def _no_trace_leak(env: dict) -> None:
    s = json.dumps(env, ensure_ascii=False)
    assert '"events"' not in s
    assert '"decision_trace"' not in s
    assert '"parts_full"' not in s


json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10_000, max_value=10_000),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(min_size=0, max_size=64),
)

json_values = st.recursive(
    json_scalars,
    lambda children: st.one_of(
        st.lists(children, min_size=0, max_size=16),
        st.dictionaries(st.text(min_size=1, max_size=32), children, max_size=16),
    ),
    max_leaves=64,
)


@settings(max_examples=200)
@given(
    sid=st.text(min_size=1, max_size=32),
    kind=st.text(min_size=0, max_size=16),
    symbol=st.text(min_size=0, max_size=16),
    meta=st.dictionaries(st.text(min_size=1, max_size=16), json_values, max_size=8),
    notify=st.dictionaries(st.text(min_size=1, max_size=16), json_values, max_size=8),
)
def test_envelope_tradeable_contract_no_trace(sid, kind, symbol, meta, notify):
    env = build_outbox_envelope(
        sid=sid,
        kind=kind,
        symbol=symbol,
        meta=meta,
        notify_payload=notify,
    )
    assert isinstance(env, dict)
    assert env.get("sid") == sid
    assert "targets" in env and isinstance(env["targets"], dict)
    assert "meta" in env and isinstance(env["meta"], dict)
    # fingerprint must exist
    assert isinstance(env["meta"].get("payload_sha1"), str) and env["meta"]["payload_sha1"]
    _no_trace_leak(env)
