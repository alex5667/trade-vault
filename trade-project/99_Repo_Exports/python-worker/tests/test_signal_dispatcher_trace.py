import json

from core.redis_keys import RedisStreams as RS


class FakeRedis:
    def __init__(self):
        self.xadded = []

    def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.xadded.append((stream, dict(fields)))
        return "1-1"

    def set(self, *args, **kwargs):
        return True


def test_dispatcher_emits_diagnostics_on_missing_trace(monkeypatch):
    monkeypatch.setenv("DECISION_TRACE_ENABLE", "1")
    from services.dispatch.dispatcher_app import SignalDispatcher
    d = SignalDispatcher()
    d.redis = FakeRedis()
    d.diag_stream = RS.SIGNAL_DIAGNOSTICS

    env = {"sid": "S1", "targets": {"notify": {"text": "x"}}, "meta": {}}
    # should not throw; should create trace + emit diag
    d._emit_diag_best_effort(env, reason="unit_test")
    assert len(d.redis.xadded) == 1
    stream, fields = d.redis.xadded[0]
    assert stream == RS.SIGNAL_DIAGNOSTICS
    payload = json.loads(fields["data"])
    assert payload["tradeable"] is False
    assert payload["reason"] == "unit_test"
