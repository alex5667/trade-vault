import json

from services.dispatch.dispatcher_app import SignalDispatcher


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self._ttl = {}

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v):
        self.kv[k] = v
        return True

    def setex(self, k, ttl, v):
        self.kv[k] = v
        self._ttl[k] = int(ttl)
        return True

    def ttl(self, k):
        return self._ttl.get(k, -1)


def test_write_trace_sidecar_best_effort_preserves_duration_ms():
    d = SignalDispatcher.__new__(SignalDispatcher)
    d.redis = FakeRedis()
    d.outbox_meta_prefix = "signal:meta:"

    sid = "sid:1"
    env = {"sid": sid, "meta": {"trace_meta_key": f"signal:meta:{sid}"}}
    patch = [
        {"type": "target", "target": "notify", "ok": True, "attempt": 1, "duration_ms": 12.3},
        {"type": "target", "target": "audit", "ok": False, "attempt": 2, "duration_ms": 45.6, "err": "boom"},
    ]

    d._write_trace_sidecar_best_effort(sid, env, patch)
    raw = d.redis.get(f"signal:meta:{sid}")
    assert raw
    obj = json.loads(raw)
    tr = obj.get("trace") or {}
    evs = tr.get("events") or []
    assert any(isinstance(e, dict) and e.get("duration_ms") == 12.3 for e in evs)
    assert any(isinstance(e, dict) and e.get("duration_ms") == 45.6 for e in evs)
