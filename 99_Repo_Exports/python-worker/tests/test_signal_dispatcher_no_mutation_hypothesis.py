import copy
import json
import pytest

from services.signal_dispatcher import SignalDispatcher

hypothesis = pytest.importorskip("hypothesis")
st = pytest.importorskip("hypothesis.strategies")


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


@hypothesis.given(payload=st.dictionaries(st.text(min_size=1, max_size=16), json_value, max_size=12))
def test_deliver_does_not_mutate_env_targets(payload):
    from services.signal_dispatcher import SignalDispatcher

    sd = object.__new__(SignalDispatcher)
    sd.redis = object()
    sd.dual_redis = object()
    sd.simple_redis = object()
    sd._sha_main = "x"
    sd._sha_dual = "y"
    sd.marker_gc_zset = "z"
    sd.delivery_marker_ttl_sec = 60

    # stub: no real lua/redis side-effects
    sd._evalsha_or_eval = lambda *a, **k: "OK"

    env = {
        "sid": "S1",
        "trace_id": "T1",
        "meta": {"audit_stream": "stream:a", "signal_stream": "stream:s", "manual_stream": "stream:m"},
        "targets": {
            "audit_payload": dict(payload),
            "signal_stream_payload": dict(payload),
            "manual_payload": dict(payload),
        },
    }

    before = copy.deepcopy(env)

    # call internal method (must exist in your dispatcher)
    sd._deliver_one_target(target="audit", sid="S1", env=env, attempt=0)  # type: ignore[attr-defined]
    sd._deliver_one_target(target="signal_stream", sid="S1", env=env, attempt=0)  # type: ignore[attr-defined]
    sd._deliver_one_target(target="manual", sid="S1", env=env, attempt=0)  # type: ignore[attr-defined]

    assert env == before
