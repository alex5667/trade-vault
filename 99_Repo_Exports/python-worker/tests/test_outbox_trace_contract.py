import json
from typing import Any, Dict

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from common.decision_trace import DecisionTrace
from services.outbox.envelope_builder import build_outbox_envelope


def _big_trace(sid: str) -> DecisionTrace:
    tr = DecisionTrace.new(sid=sid)
    tr.symbol = "BTCUSDT"
    tr.kind = "ENTRY"
    for i in range(800):
        tr.add(where="gate", name=f"g{i}", ok=(i % 3 != 0), veto=(i % 11 == 0), reason_code="OK", metrics={"i": i}, duration_ms=0.1)
    return tr


def test_build_outbox_envelope_trace_contract_no_full_trace_in_env():
    sid = "sid_contract_1"
    tr = _big_trace(sid)
    env = build_outbox_envelope(
        sid=sid,
        kind="ENTRY",
        symbol="BTCUSDT",
        notify_payload={"sid": sid, "hello": "world"},
        meta={"x": 1},
        trace=tr,
    )
    assert isinstance(env, dict)
    assert env.get("sid") == sid
    # summary fields must exist
    assert env.get("trace_id")
    assert env.get("trace_summary")
    # full trace must NOT be present in env/targets
    assert "trace" not in env
    targets = env.get("targets") or {}
    assert isinstance(targets, dict)
    assert "decision_trace" not in targets
    assert "events" not in targets
    # meta must point to sidecar
    meta = env.get("meta") or {}
    assert isinstance(meta, dict)
    assert meta.get("trace_meta_key", "").endswith(sid)


@settings(max_examples=60, deadline=None)
@given(
    notify=st.dictionaries(
        keys=st.text(min_size=1, max_size=12),
        values=st.one_of(
            st.none(),
            st.booleans(),
            st.integers(min_value=-10_000, max_value=10_000),
            st.floats(allow_nan=False, allow_infinity=False, width=32),
            st.text(max_size=128),
            st.lists(st.integers(min_value=0, max_value=100), max_size=10),
            st.dictionaries(keys=st.text(min_size=1, max_size=8), values=st.text(max_size=64), max_size=10),
        ),
        max_size=30,
    )
)
def test_build_outbox_envelope_targets_are_json_safe(notify: Dict[str, Any]):
    sid = "sid_contract_2"
    env = build_outbox_envelope(sid=sid, notify_payload=notify)
    targets = env.get("targets") or {}
    assert "notify" in targets
    # must be JSON serializable
    json.dumps(targets["notify"], ensure_ascii=False)
