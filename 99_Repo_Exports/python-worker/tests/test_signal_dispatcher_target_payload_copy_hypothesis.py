from __future__ import annotations

import json
import os
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from services.dispatch.dispatcher_app import SignalDispatcher


def json_scalars():
    return st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text()


json_value = st.recursive(
    json_scalars(),
    lambda ch: st.lists(ch, max_size=6) | st.dictionaries(st.text(min_size=1, max_size=16), ch, max_size=6),
    max_leaves=30,
)


def mk_dispatcher(calls: list) -> SignalDispatcher:
    d = SignalDispatcher.__new__(SignalDispatcher)
    # minimal attributes used by _deliver_one_target branches
    d._sha_main = "sha_main"
    d._sha_dual = "sha_dual"
    d.marker_gc_zset = "z"
    d.delivery_marker_ttl_sec = 60
    d.redis = object()

    def _fake_eval(client, sha, op_name, lua_src, numkeys, *argv):
        # argv contains ... sid, payload_json at the end for all xadd/setex paths
        calls.append({"client": client, "sha": sha, "op": op_name, "argv": list(argv)})
        return "OK"

    d._evalsha_or_eval = _fake_eval  # type: ignore[attr-defined]
    return d


def _with_guard_enabled():
    old = os.getenv("TARGETS_MUTATION_GUARD")
    os.environ["TARGETS_MUTATION_GUARD"] = "1"
    return old


def _restore_guard(old: str | None) -> None:
    if old is None:
        os.environ.pop("TARGETS_MUTATION_GUARD", None)
    else:
        os.environ["TARGETS_MUTATION_GUARD"] = old


@given(payload=st.dictionaries(st.text(min_size=1, max_size=16), json_value, max_size=12))
def test_signal_stream_branch_does_not_mutate_original_payload(payload: dict[str, Any]) -> None:
    calls: list = []
    d = mk_dispatcher(calls)
    old = _with_guard_enabled()
    try:

        sid = "SID123"
        trace_id = "TID999"
        meta = {"signal_stream": "stream:signals"}
        targets_obj = {"signal_stream_payload": dict(payload)}  # original stored in env
        env = {"sid": sid, "trace_id": trace_id, "meta": meta, "targets": targets_obj}

        original = dict(targets_obj["signal_stream_payload"])

        # call private delivery method
        d._deliver_one_target(env, sid, "signal_stream", targets_obj, meta, dual_client=None, simple_client=object())  # type: ignore[arg-type]

        # original dict must NOT be mutated
        assert targets_obj["signal_stream_payload"] == original

        # delivered JSON must contain sid/trace_id regardless of whether original had them
        assert calls, "expected lua deliver call"
        payload_json = calls[-1]["argv"][-1]
        obj = json.loads(payload_json)
        assert obj.get("sid") == sid
        assert obj.get("trace_id") == trace_id
    finally:
        _restore_guard(old)


@given(payload=st.dictionaries(st.text(min_size=1, max_size=16), json_value, max_size=12))
def test_audit_branch_does_not_mutate_original_payload(payload: dict[str, Any]) -> None:
    calls: list = []
    d = mk_dispatcher(calls)
    old = _with_guard_enabled()
    try:

        sid = "SID123"
        trace_id = "TID999"
        meta = {"audit_stream": "stream:audit"}
        targets_obj = {"audit_payload": dict(payload)}
        env = {"sid": sid, "trace_id": trace_id, "meta": meta, "targets": targets_obj}

        original = dict(targets_obj["audit_payload"])

        d._deliver_one_target(env, sid, "audit", targets_obj, meta, dual_client=None, simple_client=None)  # type: ignore[arg-type]

        assert targets_obj["audit_payload"] == original
        payload_json = calls[-1]["argv"][-1]
        obj = json.loads(payload_json)
        assert obj.get("sid") == sid
        assert obj.get("trace_id") == trace_id
    finally:
        _restore_guard(old)


@given(payload=st.dictionaries(st.text(min_size=1, max_size=16), json_value, max_size=12))
def test_manual_branch_does_not_mutate_original_payload(payload: dict[str, Any]) -> None:
    calls: list = []
    d = mk_dispatcher(calls)
    old = _with_guard_enabled()
    try:

        sid = "SID123"
        trace_id = "TID999"
        meta = {"manual_stream": "stream:manual"}
        targets_obj = {"manual_payload": dict(payload)}
        env = {"sid": sid, "trace_id": trace_id, "meta": meta, "targets": targets_obj}

        original = dict(targets_obj["manual_payload"])

        d._deliver_one_target(env, sid, "manual", targets_obj, meta, dual_client=object(), simple_client=None)  # type: ignore[arg-type]

        assert targets_obj["manual_payload"] == original
        payload_json = calls[-1]["argv"][-1]
        obj = json.loads(payload_json)
        assert obj.get("sid") == sid
        assert obj.get("trace_id") == trace_id
    finally:
        _restore_guard(old)


@given(payload=st.dictionaries(st.text(min_size=1, max_size=16), json_value, max_size=12))
def test_snapshot_branch_does_not_mutate_original_payload(payload: dict[str, Any]) -> None:
    calls: list = []
    d = mk_dispatcher(calls)
    old = _with_guard_enabled()
    try:

        sid = "SID123"
        trace_id = "TID999"
        meta = {"snap_key": "snap:key:1", "snap_ttl": 21600}
        targets_obj = {"snapshot_payload": dict(payload)}
        env = {"sid": sid, "trace_id": trace_id, "meta": meta, "targets": targets_obj}

        original = dict(targets_obj["snapshot_payload"])

        d._deliver_one_target(env, sid, "snapshot", targets_obj, meta, dual_client=None, simple_client=None)  # type: ignore[arg-type]

        assert targets_obj["snapshot_payload"] == original
        payload_json = calls[-1]["argv"][-1]
        obj = json.loads(payload_json)
        assert obj.get("sid") == sid
        assert obj.get("trace_id") == trace_id
    finally:
        _restore_guard(old)


class EvilDict(dict):
    """
    Mutates itself at the moment somebody starts iterating it.
    This simulates pathological / malicious payload dicts and validates the guard.
    """
    def __iter__(self):
        # mutate BEFORE returning iterator (does not raise, but changes content)
        if "__evil__" not in self:
            self["__evil__"] = "1"
        return super().__iter__()


def test_targets_mutation_guard_trips_on_evil_dict() -> None:
    calls: list = []
    d = mk_dispatcher(calls)
    old = _with_guard_enabled()
    try:
        sid = "SID123"
        trace_id = "TID999"
        meta = {"audit_stream": "stream:audit"}
        payload = EvilDict({"a": 1})
        targets_obj = {"audit_payload": payload}
        env = {"sid": sid, "trace_id": trace_id, "meta": meta, "targets": targets_obj}

        with pytest.raises(RuntimeError):
            d._deliver_one_target(env, sid, "audit", targets_obj, meta, dual_client=None, simple_client=None)  # type: ignore[arg-type]
    finally:
        _restore_guard(old)
