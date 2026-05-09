from __future__ import annotations

from services.signal_dispatcher import SignalDispatcher
from core.redis_keys import RedisStreams as RS


class FakeRedis:
    def __init__(self):
        self.xack_calls = []
        self.xadd_calls = []
        self.setex_calls = []
        self._kv = {}

    def xack(self, *args, **kwargs):
        self.xack_calls.append((args, kwargs))
        return 1

    def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.xadd_calls.append((stream, dict(fields)))
        return "1-0"

    def setex(self, k, ttl, v):
        self._kv[k] = str(v)
        self.setex_calls.append((k, ttl, v))
        return True

    def get(self, k):
        return self._kv.get(k)


def test_handle_one_bad_envelope_dlq_and_returns_ack(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.redis = r
    # Initialize required attributes
    d.logger = None
    d._lease_contention = 0

    dlq = {"n": 0}
    d._parse_envelope = lambda fields: None
    d._send_dlq = lambda msg_id, fields, reason: dlq.__setitem__("n", dlq["n"] + 1)

    ack_now = d._handle_one("1-0", {"data": "x"})
    assert ack_now is True
    assert dlq["n"] == 1
    # CRITICAL: _handle_one must not ACK redis messages
    assert r.xack_calls == []


def test_handle_one_missing_sid_dlq_and_returns_ack(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.redis = r

    dlq = {"n": 0}
    d._parse_envelope = lambda fields: {"foo": "bar"}  # no sid
    d._send_dlq = lambda msg_id, fields, reason: dlq.__setitem__("n", dlq["n"] + 1)

    ack_now = d._handle_one("1-0", {"data": "x"})
    assert ack_now is True
    assert dlq["n"] == 1
    assert r.xack_calls == []


def test_handle_one_lease_contention_reenqueue_and_ack(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.redis = r
    d.outbox_stream = RS.SIGNAL_OUTBOX
    d._parse_envelope = lambda fields: {"sid": "S1", "x": 1}
    d._send_dlq = lambda *a, **k: None
    d._try_acquire_sid_lease = lambda sid: None  # contention

    ack_now = d._handle_one("1-0", {"data": "x"})
    assert ack_now is True
    assert len(r.xadd_calls) == 1
    assert r.xack_calls == []


def test_handle_one_lease_contention_reenqueue_fail_keeps_pending(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.redis = r
    d.outbox_stream = RS.SIGNAL_OUTBOX
    d._parse_envelope = lambda fields: {"sid": "S1", "x": 1}
    d._send_dlq = lambda *a, **k: None
    d._try_acquire_sid_lease = lambda sid: None  # contention

    def _xadd_fail(*a, **k):
        raise RuntimeError("redis down")
    r.xadd = _xadd_fail

    ack_now = d._handle_one("1-0", {"data": "x"})
    assert ack_now is False
    assert r.xack_calls == []


def test_handle_one_deliver_ok_returns_ack_and_no_xack(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.redis = r
    d._parse_envelope = lambda fields: {"sid": "S1", "x": 1}
    d._send_dlq = lambda *a, **k: None
    d._try_acquire_sid_lease = lambda sid: "lease-token"
    released = {"n": 0}
    d._release_sid_lease = lambda sid, lease: released.__setitem__("n", released["n"] + 1)
    d._deliver_targets_with_retry = lambda env, sid: None

    ack_now = d._handle_one("1-0", {"data": "x"})
    assert ack_now is True
    assert released["n"] == 1
    assert r.xack_calls == []
