import os

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from stream_consumer_impl import StreamConsumer
import contextlib


class DummyStats:
    def update_stats(self, *a, **k):
        pass

    def increment_errors(self):
        pass


class DummyHandler:
    def __init__(self):
        self.calls = 0

    def process_stream_message(self, stream_name, message_id, fields):
        self.calls += 1


def _ensure_group(r, stream: str, group: str) -> None:
    with contextlib.suppress(Exception):
        r.xgroup_create(stream, group, id="0-0", mkstream=True)


def _mk_consumer(r):
    c = StreamConsumer(consumer_group="g")
    c.redis_client = r
    c.handler = DummyHandler()
    c.stats = DummyStats()
    c.consumer_group = "g"
    c.consumer_name = "c"
    c.streams_to_consume = ["stream:test"]
    return c


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    fail_first=st.integers(min_value=0, max_value=6),
    recover_calls=st.integers(min_value=0, max_value=12),
)
def test_handler_called_at_most_once_under_ack_flaps(r, monkeypatch, fail_first, recover_calls):
    """
    Контракт:
      - handler/process side-effects вызываются НЕ БОЛЕЕ 1 раза на msg_id,
        даже если XACK падает N раз и recovery гоняется M раз.
      - если XACK в итоге проходит, pending очищается, но handler не повторяется.
    """
    # isolate each hypothesis example
    r.flushdb()

    # harden env config used in StreamConsumer patch
    os.environ["SCANNER_MSG_DONE_PREFIX"] = "test:msg_done"
    os.environ["SCANNER_MSG_DONE_TTL_SEC"] = "60"
    os.environ["SCANNER_RECOVER_PENDING"] = "1"
    os.environ["SCANNER_RECOVER_IDLE_MS"] = "0"
    os.environ["SCANNER_RECOVER_INTERVAL_SEC"] = "0"
    os.environ["SCANNER_RECOVER_COUNT"] = "200"

    c = _mk_consumer(r)

    stream = "stream:test"
    group = "g"
    _ensure_group(r, stream, group)

    # add one message and read it (puts into PEL)
    mid = r.xadd(stream, {"k": "v"})
    msgs = r.xreadgroup(group, "c", {stream: ">"}, count=1, block=1)
    assert msgs
    msg_id, fields = msgs[0][1][0]

    # flaky xack: fails first N times, then succeeds
    orig_xack = r.xack
    state = {"left": int(fail_first)}

    def flaky_xack(stream_name, group_name, message_id):
        if state["left"] > 0:
            state["left"] -= 1
            raise RuntimeError("xack transient fail")
        return orig_xack(stream_name, group_name, message_id)

    monkeypatch.setattr(r, "xack", flaky_xack, raising=True)

    # first processing attempt
    c._handle_single_message(stream, msg_id, fields)

    # invariant: handler called exactly once after first attempt
    assert c.handler.calls == 1

    # repeat recover M times (should be ACK-only due to msg_done marker)
    for _ in range(int(recover_calls)):
        c._recover_pending_once()

    # critical invariant: still only once
    assert c.handler.calls == 1

    # pending cleared iff we had enough attempts for xack to finally succeed
    # total xack attempts = 1 (initial) + recover_calls (each recovery may try ack-only)
    total_attempts = 1 + int(recover_calls)
    pending = r.xpending(stream, group)
    pending_n = int(pending.get("pending", 0) or 0)

    if total_attempts > int(fail_first):
        assert pending_n == 0
    else:
        assert pending_n >= 1


@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    fail_first=st.integers(min_value=0, max_value=4),
)
def test_if_msg_done_exists_then_handler_never_runs(r, monkeypatch, fail_first):
    """
    Контракт:
      - если msg_done marker уже стоит, _handle_single_message должен быть ACK-only,
        handler НЕ вызывается ни при каких условиях.
    """
    r.flushdb()

    os.environ["SCANNER_MSG_DONE_PREFIX"] = "test:msg_done"
    os.environ["SCANNER_MSG_DONE_TTL_SEC"] = "60"
    os.environ["SCANNER_RECOVER_PENDING"] = "1"
    os.environ["SCANNER_RECOVER_IDLE_MS"] = "0"
    os.environ["SCANNER_RECOVER_INTERVAL_SEC"] = "0"

    c = _mk_consumer(r)
    stream = "stream:test"
    group = "g"
    _ensure_group(r, stream, group)

    r.xadd(stream, {"k": "v"})
    msgs = r.xreadgroup(group, "c", {stream: ">"}, count=1, block=1)
    assert msgs
    msg_id, fields = msgs[0][1][0]

    # force msg_done marker BEFORE processing
    c._mark_msg_done(stream, msg_id)

    # flaky xack to ensure even ACK failures don't trigger handler
    orig_xack = r.xack
    state = {"left": int(fail_first)}

    def flaky_xack(stream_name, group_name, message_id):
        if state["left"] > 0:
            state["left"] -= 1
            raise RuntimeError("xack transient fail")
        return orig_xack(stream_name, group_name, message_id)

    monkeypatch.setattr(r, "xack", flaky_xack, raising=True)

    # must NOT call handler
    c.handler.calls = 0
    c._handle_single_message(stream, msg_id, fields)
    assert c.handler.calls == 0
