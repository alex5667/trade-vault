import os
import time
import redis

from stream_consumer_impl import StreamConsumer


class DummyStats:
    def update_stats(self, *a, **k): pass
    def increment_errors(self): pass


class CountingHandler:
    def __init__(self):
        self.calls = {}

    def process_stream_message(self, stream, mid, fields):
        self.calls[mid] = self.calls.get(mid, 0) + 1


def _r():
    url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
    r = redis.Redis.from_url(url, decode_responses=True)
    r.ping()
    return r


def _mk(r, handler, group="g", consumer="c1"):
    c = StreamConsumer(consumer_group=group)
    c.redis_client = r
    c.handler = handler
    c.stats = DummyStats()
    c.consumer_group = group
    c.consumer_name = consumer
    c.streams_to_consume = ["stream:test"]
    return c


def _ensure_group(r, stream, group):
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except Exception:
        pass


def test_ack_fail_then_recover_ack_only():
    r = _r()
    r.flushdb()

    os.environ["SCANNER_MSG_DONE_PREFIX"] = "test:msg_done"
    os.environ["SCANNER_MSG_DONE_TTL_SEC"] = "120"
    os.environ["SCANNER_RECOVER_PENDING"] = "1"
    os.environ["SCANNER_RECOVER_REPROCESS_PENDING"] = "1"
    os.environ["SCANNER_RECOVER_IDLE_MS"] = "0"
    os.environ["SCANNER_RECOVER_COUNT"] = "200"

    stream = "stream:test"
    group = "g"
    _ensure_group(r, stream, group)

    handler = CountingHandler()
    c = _mk(r, handler, group=group, consumer="c1")

    mid = r.xadd(stream, {"k": "v"})

    # read as new and process once, but force XACK to fail
    orig_xack = r.xack
    def fail_xack_once(s, g, m):
        r.xack = orig_xack  # next time ok
        raise RuntimeError("xack fail")
    r.xack = fail_xack_once

    msgs = r.xreadgroup(group, c.consumer_name, {stream: ">"}, count=1, block=1)
    got_mid, fields = msgs[0][1][0]
    assert got_mid == mid

    c._handle_single_message(stream, got_mid, fields)

    # handler executed once even though ack failed
    assert handler.calls.get(mid, 0) == 1

    # now pending exists; recovery must ACK-only (no handler re-run)
    c._recover_pending_once()
    assert handler.calls.get(mid, 0) == 1

    # pending should be 0 after recovery
    pend = r.xpending(stream, group)
    assert int(pend.get("pending", 0) or 0) == 0
