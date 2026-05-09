

class FakeRedis:
    def __init__(self):
        self.calls = []
        self.store = {}

    def setex(self, key, ttl, val):
        self.calls.append(("setex", key, ttl, val))
        self.store[key] = str(val)
        return True

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        self.calls.append(("delete", key))
        self.store.pop(key, None)

    def sadd(self, key, *vals):
        if not vals:
            raise Exception("SADD with no members")
        self.calls.append(("sadd", key, vals))

    def expire(self, key, ttl):
        self.calls.append(("expire", key, ttl))
        return True


def test_msg_done_key_is_separate_from_sid_done_key():
    from services.signal_dispatcher import SignalDispatcher

    sd = SignalDispatcher()
    sd.redis = FakeRedis()
    sd.done_prefix = "signal:deliver:done:sid"
    sd.msg_done_prefix = "signal:deliver:done:msg"

    assert sd._done_key("SID123") == "signal:deliver:done:sid:SID123"
    assert sd._msg_done_key("MSG1-0") == "signal:deliver:done:msg:MSG1-0"
    assert sd._done_key("MSG1-0") != sd._msg_done_key("MSG1-0")


def test_mark_outbox_done_writes_msg_done_key():
    from services.signal_dispatcher import SignalDispatcher

    sd = SignalDispatcher()
    sd.redis = FakeRedis()
    sd.done_ttl_sec = 10
    sd.msg_done_prefix = "x:msgdone"

    sd._mark_outbox_done("1-2")
    assert sd.redis.store["x:msgdone:1-2"] == "1"


def test_update_env_req_empty_is_fail_open():
    from services.signal_dispatcher import SignalDispatcher

    sd = SignalDispatcher()
    sd.redis = FakeRedis()
    sd.env_req_prefix = "env:req"
    sd.env_state_ttl_sec = 123

    # should NOT call sadd with empty
    sd._update_env_req("SID1", set())
    assert ("expire", "env:req:SID1", 123) in sd.redis.calls


def test_adapt_notify_payload_from_signal_stream_payload():
    from services.signal_dispatcher import SignalDispatcher

    sd = SignalDispatcher()

    env = {
        "sid": "SID123",
        "targets": {
            "notify": {"text": "legacy text only"},
            "signal_stream_payload": {"symbol": "BTCUSDT", "direction": "LONG", "entry": 100.0},
        },
        "meta": {"signal_stream": "signals:cryptoorderflow:BTCUSDT"},
    }

    out = sd._adapt_notify_payload(env=env, sid="SID123")
    assert isinstance(out, dict)
    assert out.get("type") == "signal"
    assert isinstance(out.get("signal_payload"), dict)
    assert out["signal_payload"]["symbol"] == "BTCUSDT"
    assert out["signal_payload"]["signal_id"] == "SID123"
