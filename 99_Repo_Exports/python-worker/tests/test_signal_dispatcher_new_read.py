from __future__ import annotations

import services.signal_dispatcher as sd_mod
from services.signal_dispatcher import PendingMsg, SignalDispatcher
from core.redis_keys import RedisStreams as RS


class FakeHelper:
    def __init__(self):
        self.acked = []
        self.ack_fail_ids = set()

    def ack(self, stream, msg_id):
        if msg_id in self.ack_fail_ids:
            raise RuntimeError("transient ack")
        self.acked.append((stream, msg_id))
        return 1


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.ttl = {}

    def get(self, k):
        return self.kv.get(k)

    def setex(self, k, ttl, v):
        self.kv[k] = str(v)
        self.ttl[k] = int(ttl)
        return True


def test_process_new_batch_acks_each_message_and_marks_done(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.outbox_stream = RS.SIGNAL_OUTBOX
    d.done_ttl_sec = 60
    d._ctr = {"acked": 0, "ack_failed": 0}

    helper = FakeHelper()

    # Lease always acquired / released
    d._try_acquire_lease = lambda _msg_id: True
    d._release_lease = lambda _msg_id: None

    # Handle always returns True => should ACK each message
    d._handle_one = lambda msg_id, fields: True

    messages = [
        (RS.SIGNAL_OUTBOX, [PendingMsg(msg_id="1-0", fields={"a": 1}), PendingMsg(msg_id="2-0", fields={"a": 2})])
    ]

    d._process_new_batch(helper, messages)

    assert (RS.SIGNAL_OUTBOX, "1-0") in helper.acked
    assert (RS.SIGNAL_OUTBOX, "2-0") in helper.acked
    assert d._is_outbox_done("1-0") is True
    assert d._is_outbox_done("2-0") is True
    assert d._ctr["acked"] == 2


def test_process_new_batch_done_fastpath_ack_only(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.outbox_stream = RS.SIGNAL_OUTBOX
    d.done_ttl_sec = 60
    d._ctr = {"acked": 0, "ack_failed": 0}

    helper = FakeHelper()
    d._try_acquire_lease = lambda _msg_id: True
    d._release_lease = lambda _msg_id: None

    # mark done beforehand
    d._mark_outbox_done("1-0")

    called = {"n": 0}
    def _handle_one(msg_id, fields):
        called["n"] += 1
        return True
    d._handle_one = _handle_one

    messages = [(RS.SIGNAL_OUTBOX, [PendingMsg(msg_id="1-0", fields={})])]
    d._process_new_batch(helper, messages)

    assert called["n"] == 0
    assert helper.acked == [(RS.SIGNAL_OUTBOX, "1-0")]
    assert d._ctr["acked"] == 1


def test_process_new_batch_transient_handle_error_does_not_ack(monkeypatch):
    r = FakeRedis()
    d = SignalDispatcher(redis_client=r)
    d.outbox_stream = RS.SIGNAL_OUTBOX
    d._ctr = {"acked": 0, "ack_failed": 0}

    helper = FakeHelper()
    d._try_acquire_lease = lambda _msg_id: True
    d._release_lease = lambda _msg_id: None

    # Make any error transient for the test
    monkeypatch.setattr(sd_mod, "is_transient_error", lambda exc: True)

    def _handle_one(msg_id, fields):
        raise RuntimeError("transient")
    d._handle_one = _handle_one

    messages = [(RS.SIGNAL_OUTBOX, [PendingMsg(msg_id="1-0", fields={})])]
    d._process_new_batch(helper, messages)

    assert helper.acked == []
    assert d._is_outbox_done("1-0") is False
    assert d._ctr["acked"] == 0
