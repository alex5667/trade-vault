import os
import pytest

from stream_consumer_impl import StreamConsumer

class DummyStats:
    def update_stats(self, *a, **k): pass
    def increment_errors(self): pass

class DummyHandler:
    def __init__(self):
        self.calls = 0
    def process_stream_message(self, stream_name, message_id, fields):
        self.calls += 1

def _ensure_group(r, stream, group):
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except Exception:
        pass

def test_ack_fail_then_recover_ack_only(r, redis_url, monkeypatch):
    os.environ["TEST_REDIS_URL"] = redis_url

    os.environ["SCANNER_MSG_DONE_PREFIX"] = "test:msg_done"
    os.environ["SCANNER_MSG_DONE_TTL_SEC"] = "60"
    os.environ["SCANNER_RECOVER_PENDING"] = "1"
    os.environ["SCANNER_RECOVER_IDLE_MS"] = "0"
    os.environ["SCANNER_RECOVER_INTERVAL_SEC"] = "0"
    os.environ["SCANNER_RECOVER_COUNT"] = "100"

    h = DummyHandler()

    c = StreamConsumer(consumer_group="g")
    # ВАЖНО: используем fixture r (она flushdb делает)
    c.redis_client = r
    c.handler = h
    c.stats = DummyStats()
    c.consumer_group = "g"
    c.consumer_name = "c"
    c.streams_to_consume = ["stream:test"]

    stream = "stream:test"
    _ensure_group(r, stream, "g")

    mid = r.xadd(stream, {"sid": "s1", "k": "v"})

    msgs = r.xreadgroup("g", "c", {stream: ">"}, count=1, block=1)
    assert msgs
    message_id, fields = msgs[0][1][0]

    # xack fails once
    orig_xack = r.xack
    state = {"fail": True}
    def flaky_xack(stream_name, group, message_id):
        if state["fail"]:
            state["fail"] = False
            raise RuntimeError("xack transient fail")
        return orig_xack(stream_name, group, message_id)
    monkeypatch.setattr(r, "xack", flaky_xack, raising=True)

    # First processing: handler runs, msg_done set, xack fails
    c._handle_single_message(stream, message_id, fields)
    assert c.handler.calls == 1
    assert r.exists(c._msg_done_key(stream, message_id)) == 1

    # Recovery: must ACK-only (no handler re-run)
    c._recover_pending_once()
    assert c.handler.calls == 1

    pending = r.xpending(stream, "g")
    assert int(pending.get("pending", 0) or 0) == 0
