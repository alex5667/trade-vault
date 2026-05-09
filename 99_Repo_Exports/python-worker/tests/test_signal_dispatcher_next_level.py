from collections import defaultdict

import pytest
from core.redis_keys import RedisStreams as RS


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.setex_calls = []
        self.set_calls = []
        self.zadd_calls = []

    def get(self, key):
        return self.store.get(str(key))

    def set(self, key, value, nx=None, px=None, ex=None):
        self.set_calls.append((str(key), str(value), nx, px, ex))
        if nx and str(key) in self.store:
            return False
        self.store[str(key)] = str(value)
        return True

    def setex(self, key, ttl, value):
        self.setex_calls.append((str(key), int(ttl), str(value)))
        self.store[str(key)] = str(value)
        return True

    def delete(self, key):
        self.store.pop(str(key), None)
        return True

    def zadd(self, key, mapping):
        self.zadd_calls.append((str(key), dict(mapping)))
        return 1

    def xadd(self, *a, **k):
        return "1-0"


class _FakeHelper:
    def __init__(self):
        self.acked = []
        self._claim_seq = []

    def ack(self, stream, msg_id):
        self.acked.append((str(stream), str(msg_id)))
        return 1

    def claim_pending(self, stream, min_idle_ms, start_id, count):
        if not self._claim_seq:
            return ("0-0", [])
        return self._claim_seq.pop(0)


def _mk_sd():
    from services.signal_dispatcher import SignalDispatcher

    # Create instance without calling __init__ to avoid Redis connection
    sd = SignalDispatcher.__new__(SignalDispatcher)
    sd.redis = _FakeRedis()
    sd.simple_redis = sd.redis
    sd.dual_redis = sd.redis

    sd._ctr = defaultdict(int)
    sd.outbox_stream = RS.SIGNAL_OUTBOX
    sd.group = "g"

    # isolate from external Lua usage in unit tests
    sd._evalsha_or_eval = lambda *a, **k: 1
    sd._send_target_dlq = lambda *a, **k: None
    sd._update_env_req = lambda *a, **k: None
    sd._mark_env_done = lambda *a, **k: None

    # leases
    sd._try_acquire_lease = lambda msg_id: True
    sd._release_lease = lambda msg_id: None
    sd._try_acquire_sid_lease = lambda sid: "L"
    sd._release_sid_lease = lambda sid, lease: None

    # retry zset config
    sd.retry_zset = "z:retry"
    sd.retry_base_ms = 100
    sd.retry_max_ms = 1000
    sd.retry_jitter_ms = 0
    sd.max_attempts = 5
    sd.retry_dedup_prefix = "retry:dedup"

    # marker keys
    sd.marker_prefix = "signal:deliver:v2"
    sd.delivery_marker_ttl_sec = 3600
    sd.msg_done_prefix = "signal:outbox:done:v2"
    sd.env_done_prefix = "signal:env:done:v2"
    sd.done_prefix = "legacy:done"
    return sd


def test_parse_envelope_bad_json_is_fail_open():
    sd = _mk_sd()
    assert sd._parse_envelope({"data": "{not json"}) is None


def test_retry_dedup_prevents_multiple_schedules_for_same_target_sid():
    sd = _mk_sd()
    env = {"sid": "S1", "targets": {"audit_payload": {"x": 1}}, "meta": {"audit_stream": "s"}}
    # schedule same retry twice
    sd._schedule_target_retry(target="audit", sid="S1", env=env, attempt=1, last_error="e1")
    sd._schedule_target_retry(target="audit", sid="S1", env=env, attempt=1, last_error="e1")
    assert len(sd.redis.zadd_calls) == 1


def test_env_done_fastpath_skips_delivery_and_acks():
    sd = _mk_sd()
    helper = _FakeHelper()

    # mark env done
    sd.redis.set(sd._env_done_key("S1"), "1")

    # if _handle_one sees env done, it returns True (caller will ack)
    ok = sd._handle_one("10-0", {"data": '{"sid":"S1","targets":{},"meta":{}}'})
    assert ok is True


def test_deliver_one_target_missing_meta_is_permanent():
    from services.signal_dispatcher import PermanentDeliveryError
    sd = _mk_sd()
    with pytest.raises(PermanentDeliveryError):
        sd._deliver_one_target(
            env={"targets": {"signal_stream_payload": {"data": "x"}}, "meta": {}},
            sid="S1",
            target="signal_stream",
            targets_obj={"signal_stream_payload": {"data": "x"}},
            meta={},
            dual_client=sd.redis,
            simple_client=sd.redis,
        )
