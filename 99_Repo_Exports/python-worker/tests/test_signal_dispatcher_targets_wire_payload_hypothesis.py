import copy
import json

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st


def _json_scalars():
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-10**6, max_value=10**6),
        st.floats(allow_nan=False, allow_infinity=False, width=32),
        st.text(max_size=32),
    )


json_value = st.recursive(
    _json_scalars(),
    lambda ch: st.one_of(
        st.lists(ch, max_size=6),
        st.dictionaries(st.text(max_size=16), ch, max_size=6),
    ),
    max_leaves=30,
)


def _mk_sd():
    from services.dispatch.dispatcher_app import SignalDispatcher

    sd = object.__new__(SignalDispatcher)
    # clients are only checked for non-None, no real redis needed
    sd.redis = object()
    sd.dual_redis = object()
    sd.simple_redis = object()
    sd._sha_main = "sha_main"
    sd._sha_dual = "sha_dual"
    sd.marker_gc_zset = "gc:zset"

    captured = {"calls": []}

    def fake_evalsha_or_eval(client, sha, op_name, lua_src, numkeys, *argv):
        # payload_json is always last argument in your deliver calls
        captured["calls"].append({"op": op_name, "sha": sha, "argv": list(argv)})
        return "OK"

    sd._evalsha_or_eval = fake_evalsha_or_eval
    sd._captured = captured  # for assertions
    return sd



@given(payload=st.dictionaries(st.text(max_size=16), json_value, max_size=12))
def test_targets_do_not_mutate_env_and_inject_ids_into_wire_payload(payload):
    sd = _mk_sd()

    sid = "S1"
    trace_id = "T1"

    env = {
        "sid": sid,
        "trace_id": trace_id,
        "meta": {
            "audit_stream": "stream:a",
            "signal_stream": "stream:s",
            "manual_stream": "stream:m",
            "snap_key": "snap:key",
            "snap_ttl": 120,
        },
        "targets": {
            "audit_payload": dict(payload),
            "signal_stream_payload": dict(payload),
            "manual_payload": dict(payload),
            "snapshot_payload": dict(payload),
        },
    }

    before = copy.deepcopy(env)

    # These are the same signature you already use elsewhere.
    sd._deliver_one_target(target="audit", sid=sid, env=env, attempt=0)          # type: ignore[attr-defined]
    sd._deliver_one_target(target="signal_stream", sid=sid, env=env, attempt=0) # type: ignore[attr-defined]
    sd._deliver_one_target(target="manual", sid=sid, env=env, attempt=0)         # type: ignore[attr-defined]
    sd._deliver_one_target(target="snapshot", sid=sid, env=env, attempt=0)       # type: ignore[attr-defined]

    # 1) env is not mutated at all
    assert env == before

    # 2) each wire payload contains sid + trace_id (even if original payload didn't)
    calls = sd._captured["calls"]
    assert len(calls) >= 4
    for c in calls:
        payload_json = c["argv"][-1]
        obj = json.loads(payload_json)
        assert obj.get("sid") == sid
        assert obj.get("trace_id") == trace_id
