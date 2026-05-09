from collections import defaultdict
from types import SimpleNamespace
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
    # Mock Redis connection before creating instance
    import services.signal_dispatcher
    from services.signal_dispatcher import SignalDispatcher
    original_get_redis = services.signal_dispatcher.get_redis
    services.signal_dispatcher.get_redis = lambda: _FakeRedis()

    try:
        sd = SignalDispatcher()
        sd.simple_redis = sd.redis
        sd.dual_redis = sd.redis

        sd._ctr = defaultdict(int)
        sd._pending_claimed = 0
        sd.dlq_maxlen = 1000
        sd.outbox_stream = RS.SIGNAL_OUTBOX

        # make leases always available in unit tests
        sd._try_acquire_lease = lambda msg_id: True
        sd._release_lease = lambda msg_id: None
        sd._try_ack_retry_only = lambda helper, stream, msg_id: False
        sd._remember_ack_retry = lambda stream, msg_id: None

        # _r() used by _is_outbox_done
        sd._r = lambda: sd.redis
        return sd
    finally:
        services.signal_dispatcher.get_redis = original_get_redis


def test_handle_read_messages_processes_messages_correctly():
    """Test that _handle_read_messages processes messages and marks done correctly."""
    sd = _mk_sd()
    helper = _FakeHelper()

    marks = []
    sd._mark_outbox_done = lambda msg_id: marks.append(str(msg_id))
    sd.outbox_stream = RS.SIGNAL_OUTBOX

    # msg A => ack_now True, msg B => ack_now False
    def _handle_one(msg_id, fields):
        return msg_id == "A"

    sd._handle_one = _handle_one

    messages = [
        (sd.outbox_stream, [SimpleNamespace(msg_id="A", fields={"data": "{}"}), SimpleNamespace(msg_id="B", fields={"data": "{}"})])
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
        ("1-0", [SimpleNamespace(msg_id="10-0", fields={"data": "{}"}), SimpleNamespace(msg_id="11-0", fields={"data": "{}"})]),
        ("2-0", [SimpleNamespace(msg_id="12-0", fields={"data": "{}"})]),
        ("0-0", []),
    ]

    called = []
    def mock_process(*a, **k):
        called.append(k.get("msg_id"))
        # Simulate successful processing and ACK
        msg_id = k.get("msg_id")
        helper.ack(k.get("stream"), msg_id)
    sd._process_outbox_message = mock_process

    # make claim happen now
    sd.claim_every_ms = 0
    sd.claim_budget_per_tick = 10
    sd.claim_count = 10
    sd.claim_min_idle_ms = 0
    sd._pending_start_id = "0-0"
    sd._last_claim_mono = 0.0

    sd._maybe_claim_pending(helper)

    assert called == ["10-0", "11-0", "12-0"]
    assert (RS.SIGNAL_OUTBOX, "10-0") in helper.acked
    assert (RS.SIGNAL_OUTBOX, "11-0") in helper.acked
    assert (RS.SIGNAL_OUTBOX, "12-0") in helper.acked


def test_handle_read_messages_marks_outbox_done_only_on_ack_now_true():
    sd = _mk_sd()
    helper = _FakeHelper()

    marks = []
    sd._mark_outbox_done = lambda msg_id: marks.append(str(msg_id))
    sd.outbox_stream = RS.SIGNAL_OUTBOX

    # msg A => ack_now True, msg B => ack_now False
    def _handle_one(msg_id, fields):
        return msg_id == "A"

    sd._handle_one = _handle_one

    messages = [
        (sd.outbox_stream, [SimpleNamespace(msg_id="A", fields={"data": "{}"}), SimpleNamespace(msg_id="B", fields={"data": "{}"})])
    ]

    sd._handle_read_messages(helper, messages)

    assert marks == ["A"]
    assert (sd.outbox_stream, "A") in helper.acked
    assert (sd.outbox_stream, "B") not in helper.acked
