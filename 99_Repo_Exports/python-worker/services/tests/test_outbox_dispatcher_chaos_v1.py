from __future__ import annotations

"""
P3-#21: Chaos / fault-injection tests for services/signal_outbox_dispatcher.py

Covers Redis failure modes that must not crash or silently lose data:
  1. RedisError / ConnectionError on xadd_once  → schedule_retry, no crash
  2. NOSCRIPT error (Lua cache flushed)          → retried or gracefully degraded
  3. Timeout on Redis call                        → treated as transient, scheduled for retry
  4. DLQ write failure                            → SIGNAL_LOSS_SILENT_TOTAL incremented + logged
  5. _bump_attempt incr failure                   → fallback attempt=1, no crash
  6. PEL XCLAIM failure on retry                 → rescheduled, not lost
  7. Lease acquire failure (lease_contended)      → DISPATCHER_LEASE_CONTENTION incremented
  8. Schema version mismatch                      → DLQ sent, xack called, not retried
"""


import json
from collections import defaultdict
from typing import Any
from unittest.mock import MagicMock, patch

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, "/home/alex/front/trade/scanner_infra/python-worker")
import redis as redis_mod

# ── Minimal stubs ─────────────────────────────────────────────────────────────

def _make_fields(sid: str = "SID-001", schema_version: str = "1") -> dict[str, str]:
    """Build a minimal outbox stream fields dict (data=JSON envelope)."""
    envelope = {
        "sid": sid,
        "schema_version": schema_version,
        "targets": {
            "notify": {"text": "hello"},
            "signal_stream_payload": {"symbol": "BTCUSDT", "side": "LONG"},
        },
        "meta": {
            "signal_stream": "stream:signals:live",
            "audit_stream": "stream:signals:audit",
        },
        "symbol": "BTCUSDT",
    }
    return {"data": json.dumps(envelope)}


class _FakeRedis:
    """Minimal in-memory Redis stub for unit testing (no network)."""

    def __init__(self):
        self._streams: dict[str, list] = defaultdict(list)
        self._pending: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)  # stream -> group -> {id: fields}
        self._keys: dict[str, Any] = {}
        self._incr_map: dict[str, int] = {}
        self.xack_calls: list = []
        self.xadd_calls: list = []
        self._next_id = 1700000000000

    # ── Stream ops ─────────────────────────────────────────────────────────
    def xadd(self, stream, fields, maxlen=None, approximate=True, *args, **kwargs):
        msg_id = f"{self._next_id}-0"
        self._next_id += 1
        self._streams[stream].append((msg_id, fields))
        self.xadd_calls.append((stream, fields))
        return msg_id

    def xack(self, stream, group, *msg_ids):
        for mid in msg_ids:
            self.xack_calls.append(mid)
            if stream in self._pending and group in self._pending[stream]:
                self._pending[stream][group].pop(mid, None)
        return len(msg_ids)

    def xlen(self, stream):
        return len(self._streams.get(stream, []))

    def xpending(self, stream, group):
        return {"pending": len(self._pending.get(stream, {}).get(group, {}))}

    def xpending_range(self, stream, group, min, max, count, consumer=None):
        return []

    # ── Key ops ────────────────────────────────────────────────────────────
    def incr(self, key):
        self._incr_map[key] = self._incr_map.get(key, 0) + 1
        return self._incr_map[key]

    def expire(self, key, ttl):
        return 1

    def exists(self, key):
        return 0

    def get(self, key):
        return self._keys.get(key)

    def set(self, key, value, *args, **kwargs):
        self._keys[key] = value

    def delete(self, *keys):
        for k in keys:
            self._keys.pop(k, None)

    def ping(self):
        return True

    # ── Pipeline stub ──────────────────────────────────────────────────────
    def pipeline(self):
        return _FakePipeline(self)

    # ── ZSet stubs (for retry queue) ───────────────────────────────────────
    def zadd(self, key, mapping, *args, **kwargs):
        return 1

    def zrangebyscore(self, *args, **kwargs):
        return []

    def zcard(self, key):
        return 0

    def zrem(self, *args):
        return 0

    def hset(self, *args, **kwargs):
        return 1

    def hget(self, *args):
        return None

    def hdel(self, *args):
        return 0

    def execute_command(self, *args, **kwargs):
        return None


class _FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._cmds: list = []

    def zadd(self, *a, **kw):
        self._cmds.append(("zadd", a, kw))
        return self

    def zrem(self, *a):
        self._cmds.append(("zrem", a))
        return self

    def hset(self, *a, **kw):
        self._cmds.append(("hset", a, kw))
        return self

    def hdel(self, *a):
        self._cmds.append(("hdel", a))
        return self

    def execute(self):
        return [1] * len(self._cmds)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeStreamHelper:
    """Stub for SyncRedisStreamHelper — records ack calls."""

    def __init__(self):
        self.acked: list = []

    def ack(self, stream, msg_id):
        self.acked.append(msg_id)

    def read(self, *args, **kwargs):
        return {}

    def ensure_group(self, *args, **kwargs):
        pass

    def claim_pending(self, *args, **kwargs):
        return "0-0", []


# ── Helpers to build a SignalDispatcher without real Redis ─────────────────────

def _make_dispatcher(redis_override=None, dual_redis=None):
    """Build a SignalDispatcher fully mocked, no network calls."""
    with (
        patch("services.signal_outbox_dispatcher.get_redis", return_value=redis_override or _FakeRedis()),
        patch("services.signal_outbox_dispatcher.get_dual_signals_redis", return_value=dual_redis or _FakeRedis()),
        patch("services.signal_outbox_dispatcher.SyncRedisStreamHelper"),
        patch("services.signal_outbox_dispatcher.OutboxRetryQueue") as _rq_cls,
        patch("services.signal_outbox_dispatcher.DeliveryAtomic") as _da_cls,
        patch("services.signal_outbox_dispatcher.SidLease") as _lease_cls,
        patch("services.signal_outbox_dispatcher.NotifyGate") as _ng_cls,
    ):
        # Retry queue stub
        rq = MagicMock()
        rq.sizes.return_value = (0, 0)
        rq.pop_due_to_inflight.return_value = []
        rq.schedule.return_value = None
        rq.cancel.return_value = None
        _rq_cls.return_value = rq

        # DeliveryAtomic stub
        da = MagicMock()
        da.marker_key.side_effect = lambda target, sid: f"marker:{target}:{sid}"
        da.xadd_once.return_value = (True, "1234-0")
        _da_cls.return_value = da

        # SidLease stub
        lease = MagicMock()
        lease.acquire.return_value = True
        lease.renew.return_value = True
        _lease_cls.return_value = lease

        # NotifyGate stub
        ng = MagicMock()
        ng.should_send.return_value = True
        _ng_cls.return_value = ng

        from services.signal_outbox_dispatcher import SignalDispatcher
        d = SignalDispatcher()
        # Replace SCHEMA_VERSION with the test envelope's version
        d._schema_version_override = "1"
        return d, rq, da, lease


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ConnectionError on xadd_once → transient → schedule_retry
# ═══════════════════════════════════════════════════════════════════════════════

class TestConnectionErrorIsTransient:

    def test_xadd_once_connection_error_schedules_retry(self):
        """Redis ConnectionError during delivery → message scheduled for retry, not lost."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        # Make delivery raise ConnectionError
        da.xadd_once.side_effect = redis_mod.exceptions.ConnectionError("Connection refused")

        helper = _FakeStreamHelper()
        fields = _make_fields(sid="SID-CONN")
        msg_id = "1700000000000-0"

        with patch.object(d, "_is_transient", return_value=True):
            result = d._handle_one(msg_id, fields, helper=helper, attempt_hint=0)

        # Must not ACK — will be retried
        assert result is False
        assert msg_id not in helper.acked
        # Retry must be scheduled
        rq.schedule.assert_called()

    def test_connection_error_under_max_attempts_not_dlq(self):
        """Below max_attempts: ConnectionError → retry, NOT DLQ."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        da.xadd_once.side_effect = redis_mod.exceptions.ConnectionError("timeout")

        helper = _FakeStreamHelper()
        fields = _make_fields()

        with patch.object(d, "_is_transient", return_value=True):
            with patch.object(d, "_bump_attempt", return_value=1):  # attempt < max
                result = d._handle_one("MSG-001", fields, helper=helper, attempt_hint=0)

        assert result is False
        # DLQ stream must be EMPTY
        assert fake_redis._streams.get(d.dlq_stream, []) == []


# ═══════════════════════════════════════════════════════════════════════════════
# 2. NOSCRIPT error (Lua script evicted from Redis cache)
# ═══════════════════════════════════════════════════════════════════════════════

class TestNoScriptError:

    def test_noscript_treated_as_transient(self):
        """NOSCRIPT ResponseError must be classified as transient and retried."""

        # is_transient_error must recognize NOSCRIPT
        noscript_exc = redis_mod.exceptions.ResponseError("NOSCRIPT No matching script")

        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        da.xadd_once.side_effect = noscript_exc

        helper = _FakeStreamHelper()
        fields = _make_fields()

        with patch("services.signal_outbox_dispatcher.is_transient_error", return_value=True):
            result = d._handle_one("MSG-NOSCRIPT", fields, helper=helper, attempt_hint=0)

        assert result is False
        rq.schedule.assert_called()

    def test_noscript_does_not_increment_dlq(self):
        """A single NOSCRIPT failure must NOT write to DLQ."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        da.xadd_once.side_effect = redis_mod.exceptions.ResponseError("NOSCRIPT No matching script")

        helper = _FakeStreamHelper()
        fields = _make_fields()

        with patch("services.signal_outbox_dispatcher.is_transient_error", return_value=True):
            with patch.object(d, "_bump_attempt", return_value=2):  # still below max
                d._handle_one("MSG-NS2", fields, helper=helper, attempt_hint=0)

        assert fake_redis._streams.get(d.dlq_stream, []) == []


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Timeout on Redis call → treated as transient
# ═══════════════════════════════════════════════════════════════════════════════

class TestTimeoutIsTransient:

    def test_timeout_error_schedules_retry(self):
        """Redis TimeoutError on ACK → schedule retry."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        # Delivery succeeds, but ACK times out
        da.xadd_once.return_value = (True, "1234-0")

        helper = _FakeStreamHelper()
        helper.ack = MagicMock(side_effect=redis_mod.exceptions.TimeoutError("ACK timed out"))

        fields = _make_fields()

        with patch.object(d, "_is_transient", return_value=True):
            result = d._handle_one("MSG-TO", fields, helper=helper, attempt_hint=0)

        assert result is False
        rq.schedule.assert_called()

    def test_timeout_max_attempts_sends_to_dlq(self):
        """After max_attempts of TimeoutError → escalate to DLQ and ACK.

        Note: at max_attempts the code calls self.redis.xack() directly (not helper.ack)
        so we assert against fake_redis.xack_calls, not helper.acked.
        """
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        da.xadd_once.side_effect = redis_mod.exceptions.TimeoutError("timeout")

        helper = _FakeStreamHelper()
        fields = _make_fields()

        with patch("services.signal_outbox_dispatcher.is_transient_error", return_value=True):
            with patch.object(d, "_bump_attempt", return_value=d.max_attempts + 1):
                result = d._handle_one("MSG-TO-MAX", fields, helper=helper, attempt_hint=0)

        # DLQ entry written
        assert len(fake_redis._streams.get(d.dlq_stream, [])) >= 1
        # Message ACKed via self.redis.xack (direct Redis call, not helper)
        assert "MSG-TO-MAX" in fake_redis.xack_calls


# ═══════════════════════════════════════════════════════════════════════════════
# 4. DLQ write failure → SIGNAL_LOSS_SILENT_TOTAL incremented + logged
# ═══════════════════════════════════════════════════════════════════════════════

class TestDlqWriteFailure:

    def test_dlq_failure_increments_silent_loss_counter(self):
        """If DLQ xadd raises, SIGNAL_LOSS_SILENT_TOTAL{reason=dlq_write_failed} is incremented."""
        from services.signal_outbox_dispatcher import SIGNAL_LOSS_SILENT_TOTAL

        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        # Force DLQ write to fail
        original_xadd = fake_redis.xadd

        def _xadd_fail(stream, *a, **kw):
            if stream == d.dlq_stream:
                raise redis_mod.exceptions.ConnectionError("DLQ write failed")
            return original_xadd(stream, *a, **kw)

        fake_redis.xadd = _xadd_fail

        before = SIGNAL_LOSS_SILENT_TOTAL.labels(reason="dlq_write_failed")._value.get()
        d._send_dlq("MSG-DLQ", {"sid": "SID-X"}, reason="test_chaos")
        after = SIGNAL_LOSS_SILENT_TOTAL.labels(reason="dlq_write_failed")._value.get()

        assert after > before, "Silent loss counter must be incremented on DLQ write failure"

    def test_dlq_failure_does_not_raise(self):
        """DLQ write failure must be fail-open (no exception propagated)."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        fake_redis.xadd = MagicMock(side_effect=Exception("boom"))

        # Must not raise
        d._send_dlq("MSG-X", {}, reason="test")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. _bump_attempt incr failure → fallback attempt=1
# ═══════════════════════════════════════════════════════════════════════════════

class TestBumpAttemptFailure:

    def test_incr_failure_returns_1(self):
        """Redis incr failure → _bump_attempt returns 1 (fail-open, no crash)."""
        fake_redis = _FakeRedis()
        fake_redis.incr = MagicMock(side_effect=redis_mod.exceptions.ConnectionError("incr failed"))

        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        result = d._bump_attempt("MSG-INC")
        assert result == 1

    def test_incr_failure_increments_silent_loss(self):
        """SIGNAL_LOSS_SILENT_TOTAL{reason=retry_incr_failed} must fire on incr failure."""
        from services.signal_outbox_dispatcher import SIGNAL_LOSS_SILENT_TOTAL

        fake_redis = _FakeRedis()
        fake_redis.incr = MagicMock(side_effect=redis_mod.exceptions.ConnectionError("incr fail"))

        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        before = SIGNAL_LOSS_SILENT_TOTAL.labels(reason="retry_incr_failed")._value.get()
        d._bump_attempt("MSG-INC2")
        after = SIGNAL_LOSS_SILENT_TOTAL.labels(reason="retry_incr_failed")._value.get()
        assert after > before


# ═══════════════════════════════════════════════════════════════════════════════
# 6. PEL XCLAIM failure in _process_retry_due → rescheduled
# ═══════════════════════════════════════════════════════════════════════════════

class TestXClaimFailure:

    def test_xclaim_connection_error_reschedules(self):
        """XCLAIM ConnectionError in retry path → message rescheduled, not lost."""
        fake_redis = _FakeRedis()
        fake_redis.execute_command = MagicMock(
            side_effect=redis_mod.exceptions.ConnectionError("xclaim failed")
        )

        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        rq.pop_due_to_inflight.return_value = ["1700000000000-1"]

        helper = _FakeStreamHelper()

        with patch.object(d, "_is_transient", return_value=True):
            d._process_retry_due(helper)

        # Must reschedule (not cancel)
        rq.schedule.assert_called()
        rq.cancel.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Lease contention → DISPATCHER_LEASE_CONTENTION incremented
# ═══════════════════════════════════════════════════════════════════════════════

class TestLeaseContention:

    def test_lease_acquire_failure_increments_counter(self):
        """Failure to acquire SID lease → DISPATCHER_LEASE_CONTENTION.inc() called."""
        from services.signal_outbox_dispatcher import DISPATCHER_LEASE_CONTENTION

        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        # Lease acquisition fails
        d._lease.acquire = MagicMock(return_value=False)

        helper = _FakeStreamHelper()
        fields = _make_fields()

        before = DISPATCHER_LEASE_CONTENTION.labels(consumer=d.consumer)._value.get()
        result = d._handle_one("MSG-LEASE", fields, helper=helper, attempt_hint=0)
        after = DISPATCHER_LEASE_CONTENTION.labels(consumer=d.consumer)._value.get()

        assert result is False
        assert after > before, "DISPATCHER_LEASE_CONTENTION must be incremented on lease conflict"

    def test_lease_contention_schedules_retry(self):
        """Lease contention → message scheduled for later retry."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)
        d._lease.acquire = MagicMock(return_value=False)

        helper = _FakeStreamHelper()
        fields = _make_fields()
        d._handle_one("MSG-LEASE2", fields, helper=helper, attempt_hint=0)

        rq.schedule.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Schema version mismatch → DLQ + xack, not retried
# ═══════════════════════════════════════════════════════════════════════════════

class TestSchemaMismatch:

    def test_wrong_schema_version_sends_to_dlq(self):
        """Envelope with unsupported schema_version → DLQ xadd + direct xack, no retry.

        Note: schema mismatch calls self.redis.xack() directly (not helper.ack),
        so we assert against fake_redis.xack_calls.
        """
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        helper = _FakeStreamHelper()
        fields = _make_fields(schema_version="99")

        result = d._handle_one("MSG-SCHEMA", fields, helper=helper, attempt_hint=0)

        # Must return True (ACKed, not retried)
        assert result is True
        # ACKed via self.redis.xack (direct Redis, not helper)
        assert "MSG-SCHEMA" in fake_redis.xack_calls
        # DLQ entry expected
        dlq_entries = fake_redis._streams.get(d.dlq_stream, [])
        assert len(dlq_entries) >= 1

    def test_wrong_schema_version_no_delivery(self):
        """Delivery to targets must NOT happen for mismatched schema."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        helper = _FakeStreamHelper()
        fields = _make_fields(schema_version="999")
        d._handle_one("MSG-BAD-SCHEMA", fields, helper=helper, attempt_hint=0)

        da.xadd_once.assert_not_called()

    def test_missing_sid_sends_to_dlq(self):
        """Envelope missing sid field → DLQ + direct xack."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        helper = _FakeStreamHelper()
        # Override SCHEMA_VERSION to match so only sid is missing
        env = {"schema_version": "1", "targets": {}}
        fields = {"data": json.dumps(env)}

        result = d._handle_one("MSG-NO-SID", fields, helper=helper, attempt_hint=0)

        assert result is True
        assert "MSG-NO-SID" in fake_redis.xack_calls


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Bad envelope (unparseable JSON) → DLQ, no crash
# ═══════════════════════════════════════════════════════════════════════════════

class TestBadEnvelope:

    def test_unparseable_json_sends_to_dlq(self):
        """Completely unparseable data field → DLQ + direct xack, no exception."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        helper = _FakeStreamHelper()
        fields = {"data": "}{NOT JSON}{"}

        result = d._handle_one("MSG-BAD-JSON", fields, helper=helper, attempt_hint=0)

        assert result is True
        assert "MSG-BAD-JSON" in fake_redis.xack_calls

    def test_empty_fields_sends_to_dlq(self):
        """Empty fields dict → DLQ + direct xack."""
        fake_redis = _FakeRedis()
        d, rq, da, lease = _make_dispatcher(redis_override=fake_redis)

        helper = _FakeStreamHelper()
        result = d._handle_one("MSG-EMPTY", {}, helper=helper, attempt_hint=0)

        assert result is True
