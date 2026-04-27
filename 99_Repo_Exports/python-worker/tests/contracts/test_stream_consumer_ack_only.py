import os
import pytest
import redis

from stream_consumer_impl import StreamConsumer

class DummyStats:
    def update_stats(self, *a, **k): pass
    def increment_errors(self): pass

class DummyHandler:
    def __init__(self):
        self.calls = 0
    def process_stream_message(self, stream_name, message_id, fields):
        self.calls += 1

@pytest.fixture()
def consumer(r, monkeypatch):
    os.environ["SCANNER_MSG_DONE_PREFIX"] = "test:msg_done"
    os.environ["SCANNER_MSG_DONE_TTL_SEC"] = "60"
    os.environ["SCANNER_RECOVER_PENDING"] = "1"
    os.environ["SCANNER_RECOVER_IDLE_MS"] = "0"
    os.environ["SCANNER_RECOVER_INTERVAL_SEC"] = "0"
    os.environ["SCANNER_RECOVER_COUNT"] = "100"

    h = DummyHandler()

    c = StreamConsumer(...)
    c.redis_client = r
    c.handler = h
    c.stats = DummyStats()

    c.streams_to_consume = ["stream:test"]
    c.consumer_group = "g"
    c.consumer_name = "c"
    c.running = True

    # ensure group exists
    try:
        r.xgroup_create("stream:test", "g", id="0-0", mkstream=True)
    except Exception:
        pass

    return c

def test_ack_fail_then_recover_ack_only(consumer, r, monkeypatch):
    stream = "stream:test"
    sid = "s1"

    # produce message
    mid = r.xadd(stream, {"sid": sid, "k": "v"})

    # read it with group so it becomes pending if not acked
    msgs = r.xreadgroup("g", "c", {stream: ">"}, count=1, block=1)
    assert msgs
    message_id, fields = msgs[0][1][0]

    # make xack fail once
    orig_xack = r.xack
    state = {"fail": True}
    def flaky_xack(stream_name, group, message_id):
        if state["fail"]:
            state["fail"] = False
            raise RuntimeError("xack transient fail")
        return orig_xack(stream_name, group, message_id)
    monkeypatch.setattr(r, "xack", flaky_xack, raising=True)

    # first handling: handler called, msg_done set, xack fails
    consumer._handle_single_message(stream, message_id, fields)
    assert consumer.handler.calls == 1
    assert r.exists(consumer._msg_done_key(stream, message_id)) == 1

    # now recover pending: should ACK-only, handler MUST NOT be called again
    consumer._recover_pending_once()
    assert consumer.handler.calls == 1  # ключевое

    # verify no pending for this consumer group (best-effort)
    pending = r.xpending(stream, "g")
    # pending = {'pending':0,...} in redis-py
    assert int(pending.get("pending", 0) or 0) == 0
