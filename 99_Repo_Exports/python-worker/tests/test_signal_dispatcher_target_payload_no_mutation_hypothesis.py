import json
import time

import pytest

from services.signal_dispatcher import SignalDispatcher


hypothesis = pytest.importorskip("hypothesis")
st = pytest.importorskip("hypothesis.strategies")


class _FakeRedis:
    def __init__(self):
        self.kv = {}
    def set(self, *a, **k):
        self.kv[a[0]] = a[1]
        return True


@hypothesis.given(payload=st.dictionaries(st.text(min_size=1, max_size=16), st.integers(min_value=0, max_value=10), max_size=16))
@hypothesis.settings(max_examples=120, deadline=None)
def test_deliver_one_target_does_not_mutate_env_targets(payload, monkeypatch):
    d = SignalDispatcher.__new__(SignalDispatcher)
    d.redis = _FakeRedis()
    d.simple_redis = _FakeRedis()
    d._sha_main = "sha"
    d.marker_gc_zset = "gc"
    d.delivery_marker_ttl_sec = 30

    calls = []
    def fake_eval(client, sha, tag, script, nkeys, *argv):
        # argv contains payload_json at the end
        calls.append(argv)
        return "OK"
    d._evalsha_or_eval = fake_eval

    env = {
        "sid": "S1",
        "meta": {"signal_stream": "stream:x", "audit_stream": "stream:y"},
        "targets": {"signal_stream_payload": dict(payload), "audit_payload": dict(payload)},
        "trace_id": "T1",
    }
    orig_stream = dict(env["targets"]["signal_stream_payload"])
    orig_audit = dict(env["targets"]["audit_payload"])

    # signal_stream
    d._deliver_one_target(target="signal_stream", sid="S1", env=env, attempt=0)
    assert env["targets"]["signal_stream_payload"] == orig_stream

    # audit
    d._deliver_one_target(target="audit", sid="S1", env=env, attempt=0)
    assert env["targets"]["audit_payload"] == orig_audit

    # payload_json sent to lua must include sid/trace_id
    assert calls, "expected lua calls"
    sent_json = calls[0][-1]
    obj = json.loads(sent_json)
    assert obj["sid"] == "S1"
    assert obj["trace_id"] == "T1"
