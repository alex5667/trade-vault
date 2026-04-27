from utils.time_utils import get_ny_time_millis
import os
import time
import redis

import pytest
from hypothesis import settings, HealthCheck, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, rule, initialize

from stream_consumer_impl import StreamConsumer


def _redis_client():
    url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
    r = redis.Redis.from_url(url, decode_responses=True)
    r.ping()
    return r


class DummyStats:
    def update_stats(self, *a, **k): pass
    def increment_errors(self): pass


class CountingHandler:
    def __init__(self):
        self.calls_by_id = {}

    def process_stream_message(self, stream_name, message_id, fields):
        self.calls_by_id[message_id] = int(self.calls_by_id.get(message_id, 0)) + 1


def _ensure_group(r, stream, group):
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except Exception:
        pass


def _mk_consumer(r, handler, *, group="g", name=None):
    c = StreamConsumer(consumer_group=group)
    c.redis_client = r
    c.handler = handler
    c.stats = DummyStats()
    c.consumer_group = group
    c.consumer_name = name or f"c-{get_ny_time_millis()}"
    c.streams_to_consume = ["stream:test"]
    return c


@settings(
    max_examples=25,
    stateful_step_count=70,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
class StreamExactlyOnceOnSuccessMachine(RuleBasedStateMachine):
    """
    Invariant:
      - handler side-effects happen <= 1 time per msg_id,
        even with ACK flaps, restarts, and pending recovery.
    """

    def __init__(self):
        super().__init__()
        self.r = _redis_client()
        self.handler = CountingHandler()
        self.group = "g"
        self.stream = "stream:test"

        self.ack_fail_budget = 0
        self._orig_xack = self.r.xack

        def flaky_xack(stream_name, group_name, message_id):
            if self.ack_fail_budget > 0:
                self.ack_fail_budget -= 1
                raise RuntimeError("xack transient fail")
            return self._orig_xack(stream_name, group_name, message_id)

        self.r.xack = flaky_xack

        self.consumer = _mk_consumer(self.r, self.handler, group=self.group, name="c0")
        self.all_ids = set()

    @initialize()
    def init(self):
        self.r.flushdb()
        _ensure_group(self.r, self.stream, self.group)

        os.environ["SCANNER_MSG_DONE_PREFIX"] = "test:msg_done"
        os.environ["SCANNER_MSG_DONE_TTL_SEC"] = "120"
        os.environ["SCANNER_RECOVER_PENDING"] = "1"
        os.environ["SCANNER_RECOVER_REPROCESS_PENDING"] = "1"
        os.environ["SCANNER_RECOVER_INTERVAL_SEC"] = "0"
        os.environ["SCANNER_RECOVER_IDLE_MS"] = "0"
        os.environ["SCANNER_RECOVER_COUNT"] = "200"

        self.handler.calls_by_id.clear()
        self.ack_fail_budget = 0
        self.consumer = _mk_consumer(self.r, self.handler, group=self.group, name="c0")
        self.all_ids.clear()

    # ---- actions ----

    @rule()
    def add_message(self):
        mid = self.r.xadd(self.stream, {"k": "v"})
        self.all_ids.add(mid)

    @rule(n=st.integers(min_value=0, max_value=6))
    def set_ack_fail_budget(self, n):
        self.ack_fail_budget = int(n)

    @rule()
    def restart_consumer(self):
        nm = f"c{get_ny_time_millis()}"
        self.consumer = _mk_consumer(self.r, self.handler, group=self.group, name=nm)

    @rule()
    def read_new_and_handle(self):
        msgs = self.r.xreadgroup(self.group, self.consumer.consumer_name, {self.stream: ">"}, count=1, block=1)
        if not msgs:
            return
        mid, fields = msgs[0][1][0]
        self.all_ids.add(mid)
        self.consumer._handle_single_message(self.stream, mid, fields)

    @rule()
    def read_new_but_crash_before_handle(self):
        """
        Simulate crash after XREADGROUP delivery but BEFORE handler/mark/ack:
        message becomes pending for this consumer with msg_done=0.
        """
        msgs = self.r.xreadgroup(self.group, self.consumer.consumer_name, {self.stream: ">"}, count=1, block=1)
        if not msgs:
            return
        mid, _fields = msgs[0][1][0]
        self.all_ids.add(mid)
        # do nothing: no handler call, no mark, no ack

    @rule()
    def recover_pending(self):
        self.consumer._recover_pending_once()

    # ---- invariants ----

    @rule()
    def invariant_at_most_once_side_effects(self):
        for mid, n in self.handler.calls_by_id.items():
            assert int(n) <= 1, f"msg_id {mid} processed {n} times"

    @rule()
    def invariant_no_unbounded_pending_growth(self):
        pend = self.r.xpending(self.stream, self.group)
        pending_n = int(pend.get("pending", 0) or 0)
        # pending cannot exceed total IDs ever seen (sanity)
        assert pending_n <= max(0, len(self.all_ids))


TestCase = StreamExactlyOnceOnSuccessMachine.TestCase
