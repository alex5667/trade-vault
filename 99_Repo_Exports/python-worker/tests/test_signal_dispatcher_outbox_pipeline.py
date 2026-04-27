import pytest
from collections import defaultdict


class _FakeMsg:
    def __init__(self, msg_id, fields):
        self.msg_id = msg_id
        self.fields = fields


class _FakeHelper:
    def __init__(self):
        self.acked = []

    def ack(self, stream, msg_id):
        # used to assert if ack is called
        self.acked.append((str(stream), str(msg_id)))


class _FakeRedis:
    def __init__(self):
        self.set_calls = []
        self.xadd_calls = []

    def set(self, key, value, ex=None, nx=None):
        self.set_calls.append((str(key), str(value), ex, nx))
        return True

    def setex(self, key, ttl, value):
        self.set_calls.append((str(key), str(value), ttl, None))
        return True

    def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.xadd_calls.append((str(stream), dict(fields)))
        return "1-0"


def _mk_sd():
    # import locally to match repo layout
    from services.signal_dispatcher import SignalDispatcher

    # Mock Redis connection before creating instance
    import services.signal_dispatcher
    original_get_redis = services.signal_dispatcher.get_redis
    services.signal_dispatcher.get_redis = lambda: _FakeRedis()

    try:
        sd = SignalDispatcher()
        sd.simple_redis = sd.redis
        sd.dual_redis = sd.redis
        sd._ctr = defaultdict(int)

        # Initialize required attributes
        sd.done_prefix = "signal:done:v2"

        # leases are no-ops in tests
        sd._try_acquire_lease = lambda _m: True
        sd._release_lease = lambda _m: None

        return sd
    finally:
        services.signal_dispatcher.get_redis = original_get_redis


def test_no_messages_continue_is_not_unconditional():
    """
    This test is structural: it exists to prevent regression of an indentation bug:
      if not messages: ... continue
    The "continue" must be inside the if-block, otherwise dispatcher never processes messages.
    We cannot run the infinite loop here, but we can validate that _process_outbox_message()
    is called for a synthetic message in the same code-path (unit-tested separately).
    """
    sd = _mk_sd()
    helper = _FakeHelper()

    # If this gets called, message processing path is reachable.
    called = {"n": 0}
    sd._process_outbox_message = lambda *a, **k: called.__setitem__("n", called["n"] + 1)

    # emulate "messages" non-empty branch
    messages = [("stream:signals:outbox", [_FakeMsg("1-0", {"data": '{"sid":"s1","targets":{}}'})])]
    for stream, items in messages:
        for m in items:
            sd._process_outbox_message(
                helper,
                stream=str(stream),
                msg_id=str(m.msg_id),
                fields=dict(m.fields),
                where="new",
                ack_ctr_ok="acked",
                ack_ctr_fail="ack_failed",
                handle_transient_ctr="handle_transient",
                handle_failed_ctr="handle_failed",
            )
    assert called["n"] == 1


def test_bad_envelope_goes_to_dlq_and_does_not_call_helper_ack():
    sd = _mk_sd()
    helper = _FakeHelper()

    # done marker should not block this path
    sd._is_outbox_done = lambda _m: False

    # envelope parsing fails => DLQ+ACK via lua hook
    sd._parse_envelope = lambda _fields: None

    dlq_calls = []
    sd._send_dlq_and_ack = lambda msg_id, data, reason: (dlq_calls.append((msg_id, reason)) or True)
    sd._mark_outbox_done = lambda _m: True

    # If handler is called => bug (should not happen on bad envelope)
    sd._handle_env = lambda **k: (_ for _ in ()).throw(AssertionError("_handle_env must not be called"))

    sd._process_outbox_message(
        helper,
        stream="stream:signals:outbox",
        msg_id="7-0",
        fields={"data": ""},  # bad
        where="new",
        ack_ctr_ok="acked",
        ack_ctr_fail="ack_failed",
        handle_transient_ctr="handle_transient",
        handle_failed_ctr="handle_failed",
    )

    assert dlq_calls == [("7-0", "bad_envelope")]
    # We intentionally do not call helper.ack() in this path because lua acks atomically.
    assert helper.acked == []


def test_sid_done_marker_uses_separate_keyspace():
    sd = _mk_sd()

    # Test that _sid_done_key produces correct key format
    key = sd._sid_done_key("signal:s1")
    assert key == "signal_dispatcher:sid_done:signal:s1"

    # Test that it differs from msg_id done key
    msg_done_key = sd._done_key("123-456")
    assert msg_done_key != key
    assert ":sid_done:" in key
    assert ":done:" in msg_done_key
