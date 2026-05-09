from __future__ import annotations

from services.signal_dispatcher import PendingMsg, SignalDispatcher
from core.redis_keys import RedisStreams as RS


class FakePipe:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def sadd(self, k, *vals):
        self.ops.append(("sadd", k, list(vals)))
        return self

    def expire(self, k, ttl):
        self.ops.append(("expire", k, int(ttl)))
        return self

    def execute(self):
        for op in self.ops:
            if op[0] == "sadd":
                _, k, vals = op
                self.r.sets.setdefault(k, set()).update(vals)
            elif op[0] == "expire":
                _, k, ttl = op
                self.r.ttl[k] = ttl
        return True


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.sets = {}
        self.ttl = {}

    def get(self, k):
        return self.kv.get(k, None)

    def setex(self, k, ttl, v):
        self.kv[k] = str(v)
        self.ttl[k] = int(ttl)
        return True

    def pipeline(self, transaction=False):
        return FakePipe(self)


class FakeHelper:
    def __init__(self):
        self.acked = []

    def ack(self, stream, msg_id):
        self.acked.append((stream, msg_id))
        return 1


def test_update_env_req_pipeline_and_empty_is_safe():
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.env_state_ttl_sec = 10

    # empty must not error and must not create key
    d._update_env_req("sid1", set())
    assert r.sets == {}

    d._update_env_req("sid1", {"a", "b"})
    k = d._env_req_key("sid1")
    assert r.sets[k] == {"a", "b"}
    assert r.ttl[k] == 10


def test_process_pending_batch_acks_each_message_and_marks_done():
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.outbox_stream = RS.SIGNAL_OUTBOX
    d.done_ttl_sec = 60
    d.metrics_prefix = "t"

    helper = FakeHelper()

    # Make lease always available
    d._try_acquire_lease = lambda _msg_id: True
    d._release_lease = lambda _msg_id: None

    # Handle always succeeds
    d._handle_one = lambda msg_id, fields: True

    m1 = PendingMsg(msg_id="1-0", fields={"x": 1})
    m2 = PendingMsg(msg_id="2-0", fields={"x": 2})

    d._process_pending_batch(helper, [m1, m2])

    # ACK must be called for both (bug regression guard: no "ack_now outside loop")
    assert (RS.SIGNAL_OUTBOX, "1-0") in helper.acked
    assert (RS.SIGNAL_OUTBOX, "2-0") in helper.acked

    # Done marker must be set for both
    assert d._is_outbox_done("1-0") is True
    assert d._is_outbox_done("2-0") is True


def test_process_pending_batch_fastpath_done_only_acks_no_handle():
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.outbox_stream = RS.SIGNAL_OUTBOX
    d.done_ttl_sec = 60
    helper = FakeHelper()

    d._try_acquire_lease = lambda _msg_id: True
    d._release_lease = lambda _msg_id: None

    # mark done beforehand
    d._mark_outbox_done("1-0")

    called = {"n": 0}
    def _handle(msg_id, fields):
        called["n"] += 1
        return True
    d._handle_one = _handle

    d._process_pending_batch(helper, [PendingMsg(msg_id="1-0", fields={})])
    assert called["n"] == 0
    assert helper.acked == [(RS.SIGNAL_OUTBOX, "1-0")]
