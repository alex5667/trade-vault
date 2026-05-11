import collections
from types import SimpleNamespace
from typing import Any
from core.redis_keys import RedisStreams as RS


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.setex_calls = []

    def get(self, key):
        return self.store.get(str(key))

    def set(self, key, value, ex=None, nx=None):
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

    def script_load(self, *a, **k):
        return "sha123"


class _FakeHelper:
    def __init__(self):
        self.acked = []
        self._claim_seq = []

    def ack(self, stream, msg_id):
        self.acked.append((stream, msg_id))
        return 1

    def claim_pending(self, stream, min_idle_ms, start_id, count):
        if not self._claim_seq:
            return ("0-0", [])
        return self._claim_seq.pop(0)


def _mk_sd():
    # Mock Redis connection before creating instance
    import services.dispatch.dispatcher_app
    from services.dispatch.dispatcher_app import SignalDispatcher
    original_get_redis = services.dispatch.dispatcher_app.get_redis
    services.dispatch.dispatcher_app.get_redis = lambda *a, **k: _FakeRedis()

    try:
        sd = SignalDispatcher()
        sd.simple_redis = sd.redis
        sd.dual_redis = sd.redis
        sd._r = lambda: sd.redis  # type: ignore[method-assign]

        sd._ctr = collections.defaultdict(int)
        sd._pending_claimed = 0
        sd.dlq_maxlen = 1000
        sd.outbox_stream = RS.SIGNAL_OUTBOX
        sd.env_done_prefix = "done:sid"
        sd.delivery_marker_ttl_sec = 3600

        # make leases always available in unit tests
        sd._try_acquire_lease = lambda msg_id: True
        sd._release_lease = lambda msg_id: None
        sd._try_ack_retry_only = lambda helper, stream, msg_id: False
        sd._remember_ack_retry = lambda stream, msg_id: None

        # _r() used by _is_outbox_done
        sd._r = lambda: sd.redis  # type: ignore[method-assign]
        return sd
    finally:
        services.dispatch.dispatcher_app.get_redis = original_get_redis


def test_handle_read_messages_processes_messages_correctly():
    """Test that _handle_read_messages processes messages and marks done correctly."""
    sd = _mk_sd()
    helper = _FakeHelper()

    marks = []
    sd._mark_outbox_done = lambda msg_id: marks.append(msg_id)
    sd.outbox_stream = RS.SIGNAL_OUTBOX

    # msg A => ack_now True, msg B => ack_now False
    def _handle_env(msg_id, env, sid):
        return msg_id == "A"

    sd._handle_env = _handle_env

    messages = [
        (sd.outbox_stream, [
            SimpleNamespace(msg_id="A", fields={"data": '{"sid": "A", "meta": {}, "targets": {"notify": {}}}'}),
            SimpleNamespace(msg_id="B", fields={"data": '{"sid": "B", "meta": {}, "targets": {"notify": {}}}'})
        ])
    ]

    sd._handle_read_messages(helper, messages)

    assert marks == ["A"]
    assert (sd.outbox_stream, "A") in helper.acked
    assert (sd.outbox_stream, "B") not in helper.acked


def test_maybe_claim_pending_processes_all_batches_not_only_last():
    sd = _mk_sd()
    helper = _FakeHelper()

    # claim two batches; previously only second would be processed
    helper._claim_seq = [
        ("1-0", [SimpleNamespace(msg_id="10-0", fields={"data": '{"sid": "10-0", "meta": {}, "targets": {"notify": {}}}'}), SimpleNamespace(msg_id="11-0", fields={"data": '{"sid": "11-0", "meta": {}, "targets": {"notify": {}}}'})]),
        ("2-0", [SimpleNamespace(msg_id="12-0", fields={"data": '{"sid": "12-0", "meta": {}, "targets": {"notify": {}}}'})]),
        ("0-0", []),
    ]

    called = []
    def mock_process(msg_id, env, sid):
        called.append(msg_id)
        # Simulate successful processing
        return True
    sd._handle_env = mock_process

    # make claim happen now
    sd.claim_every_ms = 0
    sd.claim_budget_per_tick = 10
    sd.claim_count = 10
    sd.claim_min_idle_ms = 0
    sd._pending_start_id = "0-0"
    sd._last_claim_mono = 0.0

    sd._maybe_claim_pending(helper)  # type: ignore[arg-type]

    assert called == ["10-0", "11-0", "12-0"]
    assert (RS.SIGNAL_OUTBOX, "10-0") in helper.acked
    assert (RS.SIGNAL_OUTBOX, "11-0") in helper.acked
    assert (RS.SIGNAL_OUTBOX, "12-0") in helper.acked


def test_handle_read_messages_marks_outbox_done_only_on_ack_now_true():
    sd = _mk_sd()
    helper = _FakeHelper()

    marks = []
    sd._mark_outbox_done = lambda msg_id: marks.append(msg_id)
    sd.outbox_stream = RS.SIGNAL_OUTBOX

    # msg A => ack_now True, msg B => ack_now False
    def _handle_env(msg_id, env, sid):
        return msg_id == "A"

    sd._handle_env = _handle_env

    messages = [
        (sd.outbox_stream, [
            SimpleNamespace(msg_id="A", fields={"data": '{"sid": "A", "meta": {}, "targets": {"notify": {}}}'}),
            SimpleNamespace(msg_id="B", fields={"data": '{"sid": "B", "meta": {}, "targets": {"notify": {}}}'})
        ])
    ]

    sd._handle_read_messages(helper, messages)

    assert marks == ["A"]
    assert (sd.outbox_stream, "A") in helper.acked
    assert (sd.outbox_stream, "B") not in helper.acked
