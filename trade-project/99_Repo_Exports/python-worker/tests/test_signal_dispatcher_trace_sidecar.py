from __future__ import annotations

import json
from typing import Any

from services.dispatch.dispatcher_app import SignalDispatcher


class FakeRedis:
    def __init__(self):
        self.kv: dict[str, Any] = {}
        self.ttls: dict[str, int] = {}

    def get(self, k: str):
        return self.kv.get(k)

    def set(self, k: str, v: Any, *args, **kwargs):
        self.kv[k] = v
        return True

    def setex(self, k: str, ttl: int, v: Any):
        self.kv[k] = v
        self.ttls[k] = int(ttl)
        return True

    def ttl(self, k: str) -> int:
        return int(self.ttls.get(k, -1))


def test_sidecar_append_events_best_effort():
    sd = SignalDispatcher()
    sd.redis = FakeRedis()
    sd.outbox_meta_prefix = "signal:meta:"

    sid = "S1"
    meta_key = f"signal:meta:{sid}"
    initial = {
        "schema": "decision_trace_v1",
        "trace": {"v": 1, "sid": sid, "events": [{"type": "gate", "name": "g1"}]},
    }
    sd.redis.setex(meta_key, 60, json.dumps(initial, ensure_ascii=False))

    env = {"sid": sid, "meta": {"trace_meta_key": meta_key}}
    patch_events = [{"type": "target", "target": "notify", "ok": True, "duration_ms": 2.0}]

    sd._write_trace_sidecar_best_effort(sid, env, patch_events)

    raw = sd.redis.get(meta_key)
    obj = json.loads(raw)
    assert obj["trace"]["sid"] == sid
    assert len(obj["trace"]["events"]) == 2
    assert obj["trace"]["events"][-1]["type"] == "target"
