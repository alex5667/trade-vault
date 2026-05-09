import copy

from services.signal_dispatcher import SignalDispatcher
from utils.time_utils import get_ny_time_millis


def test_deliver_one_target_does_not_mutate_targets_payload(monkeypatch, r):
    d = SignalDispatcher()
    d.redis = r
    d.simple_redis = r
    d.dual_redis = r
    d.delivery_marker_ttl_sec = 60

    calls = {"n": 0}

    def fake_eval(client, sha, tag, script, nkeys, *argv):
        calls["n"] += 1
        marker_key = argv[0]
        ttl = int(argv[3]) if len(argv) > 3 else 60
        client.set(marker_key, str(get_ny_time_millis()), ex=ttl)
        return "OK"

    monkeypatch.setattr(d, "_evalsha_or_eval", fake_eval, raising=True)

    sid = "sid_mut_1"
    env = {
        "sid": sid,
        "trace_id": "t1",
        "targets": {
            "signal_stream_payload": {"x": 1},
            "audit_payload": {"y": 2},
        },
        "meta": {"signal_stream": "s:1", "audit_stream": "s:2"},
    }
    targets_obj = env["targets"]
    meta = env["meta"]

    before = copy.deepcopy(targets_obj)
    d._deliver_one_target(env, sid, "signal_stream", targets_obj, meta, d.dual_redis, d.simple_redis)
    d._deliver_one_target(env, sid, "audit", targets_obj, meta, d.dual_redis, d.simple_redis)
    after = copy.deepcopy(targets_obj)

    assert before == after, "deliver must not mutate tradeable targets payload dict"
