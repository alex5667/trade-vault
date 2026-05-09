from __future__ import annotations

import copy

from hypothesis import given
from hypothesis import strategies as st

from services.signal_dispatcher import SignalDispatcher


class DummyRedis:
    def set(self, *a, **k):
        return True


@given(st.text(min_size=1, max_size=20))
def test_deliver_one_target_does_not_mutate_original_targets_payload(sid):
    sd = SignalDispatcher.__new__(SignalDispatcher)
    sd.redis = DummyRedis()
    sd.dual_redis = object()
    sd.simple_redis = object()
    sd._sha_main = "sha-main"
    sd._sha_dual = "sha-dual"
    sd.marker_gc_zset = "marker:gc"
    sd.delivery_marker_ttl_sec = 60

    sd._delivery_key = lambda target, sid_: f"mk:{target}:{sid_}"
    sd._evalsha_or_eval = lambda *a, **k: "OK"

    env = {
        "sid": sid,
        "trace_id": "t0",
        "meta": {
            "signal_stream": "stream:sig",
            "audit_stream": "stream:audit",
            "manual_stream": "stream:manual",
            "snap_key": "snap:key",
            "snap_ttl": 10,
        },
        "targets": {
            "signal_stream_payload": {"a": 1},
            "audit_payload": {"b": 2},
            "manual_payload": {"c": 3},
            "snapshot_payload": {"d": 4},
        },
    }

    targets_obj = env["targets"]
    meta = env["meta"]

    baseline = copy.deepcopy(targets_obj)

    # each target branch must not mutate original dict payloads
    sd._deliver_one_target(env, sid, "signal_stream", targets_obj, meta, dual_client=sd.dual_redis, simple_client=sd.simple_redis)
    sd._deliver_one_target(env, sid, "audit", targets_obj, meta, dual_client=sd.dual_redis, simple_client=sd.simple_redis)
    sd._deliver_one_target(env, sid, "manual", targets_obj, meta, dual_client=sd.dual_redis, simple_client=sd.simple_redis)
    sd._deliver_one_target(env, sid, "snapshot", targets_obj, meta, dual_client=sd.dual_redis, simple_client=sd.simple_redis)

    assert targets_obj == baseline
