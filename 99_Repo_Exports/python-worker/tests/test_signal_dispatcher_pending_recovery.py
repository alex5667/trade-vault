import logging
from dataclasses import dataclass
from collections import defaultdict
import types

import pytest


@dataclass
class _Msg:
    msg_id: str
    fields: dict


class _FakeHelper:
    """
    Minimal SyncRedisStreamHelper stub for unit tests.
    We only need claim_pending() + ack() used by _maybe_claim_pending.
    """
    def __init__(self, claim_batches):
        # claim_batches: list of tuples (next_id, [msgs...])
        self._claim_batches = list(claim_batches)
        self._claim_i = 0
        self.acked = []
        self.ack_calls = []  # for ordering assertions

    def claim_pending(self, stream, *, min_idle_ms, start_id, count):
        if self._claim_i >= len(self._claim_batches):
            return "0-0", []
        nxt, msgs = self._claim_batches[self._claim_i]
        self._claim_i += 1
        # respect count cap (simulate helper behavior)
        msgs = list(msgs)[: int(count)]
        return nxt, msgs

    def ack(self, stream, msg_id):
        self.ack_calls.append(("ack", stream, str(msg_id)))
        self.acked.append((stream, str(msg_id)))


class _FakeRedis:
    def __init__(self):
        self.xack_calls = []

    def xack(self, stream, group, msg_id):
        self.xack_calls.append((stream, group, msg_id))


def _mk_dispatcher():
    from services.signal_dispatcher import SignalDispatcher

    # Patch __init__ to avoid Redis connection
    original_init = SignalDispatcher.__init__
    def test_init(self):
        self.simple_redis = _FakeRedis()
        self.redis = self.simple_redis
        # Call the rest of __init__ logic
        self.dual_redis = None
        self.outbox_stream = "stream:signals:outbox"
        self.dlq_stream = "stream:signals:dlq"
        # ... add other required attributes
        self._ctr = defaultdict(int)
        self.claim_every_ms = 0
        self.claim_budget_per_tick = 50
        self.claim_min_idle_ms = 0
        self.claim_count = 50
        self._last_claim_mono = 0.0
        self._pending_start_id = "0-0"
        self._pending_claimed = 0
        self.logger = logging.getLogger("test_signal_dispatcher")

    SignalDispatcher.__init__ = test_init
    try:
        sd = SignalDispatcher()
    finally:
        SignalDispatcher.__init__ = original_init

    return sd


def test_claim_pending_processes_all_batches_and_acks_all():
    """
    Regression test:
      Old code only processed the LAST `msgs` batch returned by claim_pending()
      because it overwrote `msgs` in the while loop and iterated over it after.
    """
    sd = _mk_dispatcher()

    # 2 batches, total 3 messages
    helper = _FakeHelper(
        [
            ("1-0", [_Msg("a", {"data": "1"}), _Msg("b", {"data": "2"})]),
            ("2-0", [_Msg("c", {"data": "3"})]),
        ]
    )

    handled = []
    done_marked = []

    sd._try_ack_retry_only = lambda _h, _s, _m: False
    sd._try_acquire_lease = lambda _m: True
    sd._release_lease = lambda _m: None
    sd._is_outbox_done = lambda _m: False

    sd._mark_outbox_done = lambda mid: done_marked.append(mid)
    sd._handle_one = lambda mid, fields: (handled.append((mid, fields)) or True)
    sd._remember_ack_retry = lambda stream, msg_id: None

    sd._maybe_claim_pending(helper)

    assert [m for m, _ in handled] == ["a", "b", "c"]
    assert done_marked == ["a", "b", "c"]
    assert helper.acked == [(sd.outbox_stream, "a"), (sd.outbox_stream, "b"), (sd.outbox_stream, "c")]


def test_claim_pending_done_fastpath_skips_handle_one():
    """
    If done marker exists (we wrote it earlier but ACK transient-failed),
    recovery should ACK-only and not re-run _handle_one.
    """
    sd = _mk_dispatcher()

    helper = _FakeHelper([("1-0", [_Msg("a", {"data": "1"}), _Msg("b", {"data": "2"})])])

    called = {"handle": 0}
    done_marked = []

    sd._try_ack_retry_only = lambda _h, _s, _m: False
    sd._try_acquire_lease = lambda _m: True
    sd._release_lease = lambda _m: None

    # "a" is already done, "b" is not
    sd._is_outbox_done = lambda mid: True if str(mid) == "a" else False

    sd._mark_outbox_done = lambda mid: done_marked.append(mid)
    sd._handle_one = lambda mid, fields: (called.__setitem__("handle", called["handle"] + 1) or True)
    sd._remember_ack_retry = lambda stream, msg_id: None

    sd._maybe_claim_pending(helper)

    # handle_one called only for "b"
    assert called["handle"] == 1

    # we still ACK both
    assert helper.acked == [(sd.outbox_stream, "a"), (sd.outbox_stream, "b")]

    # done marker is written for both (fastpath re-marks but it's ok)
    assert done_marked == ["a", "b"]


def test_handle_one_must_not_xack_directly_on_bad_envelope():
    """
    Contract test:
      _handle_one should not call redis.xack; it returns True and outer loop ACKs.
    """
    sd = _mk_dispatcher()
    sd.group = "g"
    sd.outbox_stream = "s"
    sd.redis = _FakeRedis()

    dlq = []
    sd._parse_envelope = lambda fields: None
    sd._send_dlq = lambda msg_id, payload, reason="": dlq.append((msg_id, reason))

    ack_now = sd._handle_one("m1", {"data": "x"})
    assert ack_now is True
    assert dlq == [("m1", "bad_envelope")]
    assert sd.redis.xack_calls == []


def test_finalize_ack_marks_done_before_ack():
    """
    Ordering invariant:
      _finalize_ack MUST call _mark_outbox_done() before helper.ack()
    """
    sd = _mk_dispatcher()
    helper = _FakeHelper([])

    calls = []
    sd._mark_outbox_done = lambda mid: calls.append(("done", str(mid)))

    # Use helper.ack to record ordering
    sd._finalize_ack(helper, sd.outbox_stream, "m1", ctr_ok="ok", ctr_fail="fail", where="t")
    calls.extend([c for c in helper.ack_calls])  # ("ack", stream, msg_id)

    assert calls[0] == ("done", "m1")
    assert calls[1][0] == "ack"
    assert calls[1][2] == "m1"
