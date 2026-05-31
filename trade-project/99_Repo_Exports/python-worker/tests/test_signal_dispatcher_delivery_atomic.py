from collections import defaultdict


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.set_calls = []
        self.setex_calls = []
        self.get_calls = []

    def get(self, key):
        self.get_calls.append(str(key))
        return self.store.get(str(key))

    def set(self, key, value, ex=None, nx=None):
        self.set_calls.append((str(key), str(value), ex, nx))
        # emulate nx
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

    def xadd(self, *a, **k):
        return "1-0"

    def sadd(self, *a, **k):
        return 1

    def expire(self, *a, **k):
        return True


def _mk_sd():
    from services.dispatch.dispatcher_app import SignalDispatcher

    # Create instance without calling __init__ to avoid Redis connection
    sd = SignalDispatcher.__new__(SignalDispatcher)
    sd.redis = _FakeRedis()
    sd.simple_redis = sd.redis
    sd.dual_redis = sd.redis
    sd._ctr = defaultdict(int)

    # minimal config
    sd.done_prefix = "signal:done:v2"
    sd.marker_prefix = "signal:deliver:v2"
    sd.msg_lease_prefix = "signal:lease:v2"
    sd.done_ttl_sec = 3600
    sd.delivery_marker_ttl_sec = 3600
    sd.metrics_prefix = "signal_dispatcher"

    # _r() used by _is_outbox_done
    sd._r = lambda: sd.redis
    return sd


def test_outbox_done_keyspace_is_separate_from_legacy_done_key():
    sd = _mk_sd()

    msg_id = "7-0"
    sd._mark_outbox_done(msg_id)

    # new key must exist
    assert sd.redis.get(sd._outbox_done_key(msg_id)) == "1"

    # legacy key should NOT be written by _mark_outbox_done
    assert sd.redis.get(sd._done_key(msg_id)) is None


def test_is_outbox_done_accepts_legacy_value_1_only():
    sd = _mk_sd()
    msg_id = "9-0"

    # legacy msg-done was stored under _done_key(msg_id) with value "1"
    sd.redis.set(sd._done_key(msg_id), "1")
    assert sd._is_outbox_done(msg_id) is True

    # but if legacy key contains a timestamp (sid-done style), do NOT treat it as msg-done
    msg_id2 = "10-0"
    sd.redis.set(sd._done_key(msg_id2), "1700000000000")
    assert sd._is_outbox_done(msg_id2) is False


def test_delivery_key_is_alias_for_marker_key():
    """Test that _delivery_key is an alias for _marker_key for backward compatibility."""
    sd = _mk_sd()

    target = "notify"
    sid = "sid-1"

    marker_key = sd._marker_key(target, sid)
    delivery_key = sd._delivery_key(target, sid)

    assert marker_key == delivery_key == "signal:deliver:v2:notify:sid-1"


def test_sid_done_key_uses_separate_keyspace():
    """Test that SID done keys are separate from message done keys."""
    sd = _mk_sd()

    sid = "signal:s1"
    msg_id = "7-0"

    sid_done_key = sd._sid_done_key(sid)
    msg_done_key = sd._outbox_done_key(msg_id)

    # Different keyspaces
    assert ":sid:" in sid_done_key
    assert ":msg:" in msg_done_key

    # Different values
    assert sid_done_key != msg_done_key
