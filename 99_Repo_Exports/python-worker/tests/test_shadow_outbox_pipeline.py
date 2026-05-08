from __future__ import annotations
"""
Shadow Outbox Pipeline Tests
============================
Тесты для безопасного включения CRYPTO_SHADOW_OUTBOX=1.

Охватывают:
1. Envelope building: build_outbox_envelope produces correct targets/meta structure
2. atomic_xadd_async shadow write: coroutine called with correct stream key and envelope fields
3. Dispatcher _handle_one: correctly routes envelope to audit stream
4. Dispatcher dedup: same signal_id не дублируется
5. Dispatcher DLQ: envelope без sid попадает в DLQ
"""
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
import uuid
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path setup (run from python-worker root)
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

_PW = Path(__file__).parent.parent
if str(_PW) not in sys.path:
    sys.path.insert(0, str(_PW))


# ---------------------------------------------------------------------------
# Fake Redis helpers
# ---------------------------------------------------------------------------

class FakeSyncRedis:
    """Sync Redis stub для dispatcher tests."""
    def __init__(self) -> None:
        self.kv: Dict[str, str] = {}
        self.streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
        self._seq = 0
        self.xack_calls: List[Tuple[str, str, str]] = []

    def _next_id(self) -> str:
        self._seq += 1
        return f"{get_ny_time_millis()}-{self._seq}"

    def set(self, key: str, value: str, *, nx: bool = False, xx: bool = False,
            ex: Optional[int] = None, px: Optional[int] = None) -> bool:
        exists = key in self.kv
        if nx and exists:
            return False
        if xx and not exists:
            return False
        self.kv[key] = str(value)
        return True

    def get(self, key: str) -> Optional[str]:
        return self.kv.get(key)

    def delete(self, *keys: str) -> int:
        return sum(1 for k in keys if self.kv.pop(k, None) is not None)

    def incr(self, key: str) -> int:
        v = int(self.kv.get(key, "0")) + 1
        self.kv[key] = str(v)
        return v

    def expire(self, key: str, ttl: int) -> bool:
        return key in self.kv

    def xadd(self, stream: str, fields: Dict[str, Any], *args: Any,
              maxlen: Optional[int] = None, approximate: bool = False, **kw: Any) -> str:
        entry_id = self._next_id()
        d = {str(k): str(v) for k, v in (fields or {}).items()}
        self.streams.setdefault(stream, []).append((entry_id, d))
        return entry_id

    def xack(self, stream: str, group: str, *msg_ids: str) -> int:
        self.xack_calls.extend((stream, group, mid) for mid in msg_ids)
        return len(msg_ids)

    def last_stream_entry(self, stream: str) -> Dict[str, str]:
        items = self.streams.get(stream, [])
        assert items, f"Stream '{stream}' is empty"
        return items[-1][1]

    def stream_len(self, stream: str) -> int:
        return len(self.streams.get(stream, []))


class FakeAsyncRedis:
    """Async Redis stub для atomic_xadd_async calls."""
    def __init__(self) -> None:
        self.eval_calls: List[Tuple] = []
        self._return_code = 1      # 1 = success
        self._entry_id = "1700000000001-1"

    async def eval(self, script: str, num_keys: int, *args: Any) -> List[Any]:
        self.eval_calls.append((script, num_keys, args))
        return [self._return_code, self._entry_id]


# ---------------------------------------------------------------------------
# Test 1: build_outbox_envelope produces correct structure
# ---------------------------------------------------------------------------

def test_build_outbox_envelope_structure():
    """build_outbox_envelope создаёт envelope с обязательными targets и meta."""
    from services.outbox.envelope_builder import build_outbox_envelope

    sid = "test-sig-001"
    env = build_outbox_envelope(
        sid=sid,
        symbol="BTCUSDT",
        kind="crypto_orderflow",
        notify_payload={"text": "LONG 95000", "symbol": "BTCUSDT"},
        audit_payload={"payload": json.dumps({"entry": 95000.0, "direction": "LONG"})},
        signal_stream_payload={"data": json.dumps({"entry": 95000.0})},
        audit_stream="signals:crypto:raw",
        signal_stream="signals:cryptoorderflow:BTCUSDT",
    )

    # Обязательная структура
    assert "sid" in env, "env must have 'sid'"
    assert env["sid"] == sid
    assert "targets" in env, "env must have 'targets'"
    assert "meta" in env, "env must have 'meta'"

    targets = env["targets"]
    assert "notify" in targets, "targets must have 'notify'"
    assert "audit_payload" in targets, "targets must have 'audit_payload'"
    assert "signal_stream_payload" in targets, "targets must have 'signal_stream_payload'"

    meta = env["meta"]
    assert meta.get("audit_stream") == "signals:crypto:raw", "meta.audit_stream mismatch"
    assert meta.get("signal_stream") == "signals:cryptoorderflow:BTCUSDT", "meta.signal_stream mismatch"


def test_build_outbox_envelope_no_debug_print(capsys):
    """Горячий путь: build_outbox_envelope НЕ должен ничего печатать в stdout."""
    from services.outbox.envelope_builder import build_outbox_envelope

    build_outbox_envelope(
        sid="no-print-test",
        symbol="ETHUSDT",
        kind="crypto_orderflow",
        notify_payload={"text": "test"},
        audit_payload={"payload": "{}"},
        audit_stream="signals:crypto:raw",
    )
    captured = capsys.readouterr()
    assert "DEBUG_BUILD_OUTBOX_ENV" not in captured.out, (
        "DEBUG print found in envelope_builder hot path! Remove it."
    )


# ---------------------------------------------------------------------------
# Test 2: atomic_xadd_async shadow write path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_atomic_xadd_async_called_with_outbox_stream():
    """Shadow mode: atomic_xadd_async пишет в stream:signals:outbox с корректным envelope."""
    from services.outbox.envelope_builder import build_outbox_envelope
    from services.outbox.atomic_outbox import atomic_xadd_async

    fake_redis = FakeAsyncRedis()
    sid = f"shadow-test-{uuid.uuid4().hex[:8]}"

    env = build_outbox_envelope(
        sid=sid,
        symbol="BTCUSDT",
        kind="crypto_orderflow",
        notify_payload={"text": "LONG"},
        audit_payload={"payload": '{"entry": 95000}'},
        audit_stream="signals:crypto:raw",
    )
    env_json = json.dumps(env)

    outbox_stream = "stream:signals:outbox"
    os.environ["OUTBOX_EVENT_STREAM_ENABLE"] = "0"  # keep it simple for unit test

    entry_id = await atomic_xadd_async(
        fake_redis,
        stream_key=outbox_stream,
        signal_id=sid,
        payload_obj=env,
        kind="crypto_orderflow",
        symbol="BTCUSDT",
        ts=str(env.get("ts_ms", "")),
    )

    assert entry_id is not None, "Must return entry_id on success"
    assert len(fake_redis.eval_calls) == 1, "Must call redis.eval exactly once"

    _, num_keys, args = fake_redis.eval_calls[0]
    assert num_keys == 5, "Must pass 5 KEYS to Lua"

    # KEYS[3] = stream_key (index 2 in args)
    stream_key_in_call = args[2]
    assert stream_key_in_call == outbox_stream, (
        f"stream_key passed to Lua = {stream_key_in_call!r}, expected {outbox_stream!r}"
    )

    # signal_id (ARGV[3] = args[5+2] = args[7])
    signal_id_in_call = args[7]
    assert signal_id_in_call == sid, f"signal_id in Lua ARGV = {signal_id_in_call!r}"

    # payload_json (ARGV[7] = args[5+6] = args[11])
    payload_json = args[11]
    payload = json.loads(payload_json)
    assert payload.get("sid") == sid, f"payload.sid = {payload.get('sid')!r}"
    assert "targets" in payload, "payload must retain 'targets' from envelope"


# ---------------------------------------------------------------------------
# Test 3: Dispatcher _handle_one routes audit payload to stream
# ---------------------------------------------------------------------------

class MinimalSyncHelper:
    """Minimal stub for SyncRedisStreamHelper used by _handle_one."""
    def __init__(self) -> None:
        self.acked: List[str] = []

    def ack(self, stream: str, msg_id: str) -> None:
        self.acked.append(msg_id)


class MinimalDelivery:
    """Minimal stub for DeliveryAtomic."""
    def __init__(self, redis: FakeSyncRedis) -> None:
        self._redis = redis

    def marker_key(self, target: str, sid: str) -> str:
        return f"sig:delivery:{target}:{sid}"

    def xadd_once(self, *, marker_key: str, stream: str, payload: Dict, maxlen: int = 1000
                  ) -> Tuple[bool, str]:
        entry_id = self._redis.xadd(stream, payload)
        return True, entry_id

    def setex_once(self, *, marker_key: str, key: str, ttl_sec: int, payload: Any) -> bool:
        return True


def _make_dispatcher_for_test(redis: FakeSyncRedis):
    """Build SignalDispatcher with all external deps stubbed out."""
    from services.signal_outbox_dispatcher import SignalDispatcher

    d = SignalDispatcher.__new__(SignalDispatcher)
    d.redis = redis
    d.dual = redis
    d.dual_redis = redis
    d.simple_redis = redis

    d.outbox_stream = "stream:signals:outbox"
    d.dlq_stream = "stream:signals:dlq"
    d.group = "test-group"
    d.consumer = "test-consumer"
    d.mt5_plans_stream = "stream:signals:plans"
    d.signal_notify_stream = "notify:telegram"
    d.signal_manual_stream = "stream:signals:manual"
    d.signal_notify_maxlen = 10000
    d.signal_manual_maxlen = 10000
    d.notify_stream = "notify:telegram"
    d.notify_signal_counter_key = "notify:telegram:signal_counter"
    d.max_attempts = 7
    d.retry_base_ms = 250
    d.retry_max_ms = 30000
    d.retry_pop_limit = 200
    d.retry_lease_ms = 60000
    d.retry_requeue_limit = 200
    d.retry_meta_ttl_sec = 86400
    d.retry_ready_zset = "sig:outbox:retry:ready:test-group"
    d.retry_inflight_zset = "sig:outbox:retry:inflight:test-group"
    d.retry_due_hash = "sig:outbox:retry:due:test-group"
    d.retry_owner_hash = "sig:outbox:retry:owner:test-group"
    d.sid_lease_ttl_ms = 15000
    d.delivery_timeout_ms = 30000
    d.claim_min_idle_ms = 65000
    d.claim_count = 200
    d.claim_every_ms = 2000
    d._claim_start_id = "0-0"
    d._last_claim_mono = 0.0
    d._last_metrics_mono = 0.0
    d._metrics_interval_ms = 10000
    d._handle_one_count = 0
    d.sid_lease_renew_every_target = False

    # Stub delivery (write to fake redis)
    d._delivery = MinimalDelivery(redis)

    # Stub retry queue (no-op)
    rq = MagicMock()
    rq.sizes.return_value = (0, 0)
    rq.cancel.return_value = None
    rq.schedule.return_value = None
    d._retryq = rq

    # Stub lease (always acquire)
    lease = MagicMock()
    lease.acquire.return_value = True
    lease.renew.return_value = True
    d._lease = lease

    # Stub notify gate (always send)
    ng = MagicMock()
    ng.should_send.return_value = True
    d._notify_gate = ng

    return d


def test_dispatcher_handle_one_routes_to_audit_stream():
    """_handle_one корректно достаёт audit_payload из envelope и пишет в audit_stream."""
    from services.outbox.envelope_builder import build_outbox_envelope

    redis = FakeSyncRedis()
    dispatcher = _make_dispatcher_for_test(redis)
    helper = MinimalSyncHelper()

    sid = f"dispatch-test-{uuid.uuid4().hex[:8]}"
    audit_stream = "signals:crypto:raw"

    env = build_outbox_envelope(
        sid=sid,
        symbol="BTCUSDT",
        kind="crypto_orderflow",
        notify_payload={"text": "LONG 95000", "symbol": "BTCUSDT"},
        audit_payload={"payload": json.dumps({"entry": 95000.0, "direction": "LONG"})},
        signal_stream_payload={"data": json.dumps({"entry": 95000.0})},
        audit_stream=audit_stream,
        signal_stream="signals:cryptoorderflow:BTCUSDT",
    )

    # Serialize as it arrives from outbox stream
    fields = {"payload": json.dumps(env, ensure_ascii=False)}

    result = dispatcher._handle_one("1700000-1", fields, helper=helper, attempt_hint=0)

    assert result is True, "handle_one must return True on success"
    assert "1700000-1" in helper.acked, "msg_id must be ACKed"

    # audit_stream must have received the payload
    assert redis.stream_len(audit_stream) >= 1, (
        f"audit_stream '{audit_stream}' must receive payload; got {redis.streams}"
    )


# ---------------------------------------------------------------------------
# Test 4: Dispatcher dedup — same signal_id не дублируется
# ---------------------------------------------------------------------------

def test_dispatcher_dedup_same_sid():
    """Два одинаковых signal_id обрабатываются атомарно: второй ACK-ается без записи в стримы."""
    from services.outbox.envelope_builder import build_outbox_envelope

    redis = FakeSyncRedis()
    dispatcher = _make_dispatcher_for_test(redis)
    helper = MinimalSyncHelper()

    sid = f"dedup-test-{uuid.uuid4().hex[:8]}"
    audit_stream = "signals:crypto:raw"

    env = build_outbox_envelope(
        sid=sid,
        symbol="BTCUSDT",
        kind="crypto_orderflow",
        audit_payload={"payload": "{}"},
        audit_stream=audit_stream,
    )
    fields = {"payload": json.dumps(env, ensure_ascii=False)}

    r1 = dispatcher._handle_one("msg-1", fields, helper=helper, attempt_hint=0)
    # Manually set "done" so second call simulates idempotent scenario
    dispatcher._env_done_is_set = lambda sid_: True  # type: ignore

    r2 = dispatcher._handle_one("msg-2", fields, helper=helper, attempt_hint=0)

    assert r1 is True, "first call must succeed"
    assert r2 is True, "second call must also return True (already done)"
    # Only first write reaches audit stream
    assert redis.stream_len(audit_stream) == 1, (
        f"audit_stream should have exactly 1 entry, got {redis.stream_len(audit_stream)}"
    )


# ---------------------------------------------------------------------------
# Test 5: Dispatcher DLQ — envelope без sid → DLQ
# ---------------------------------------------------------------------------

def test_dispatcher_dlq_on_missing_sid():
    """Envelope без 'sid' направляется в DLQ и ACK-ается (не зависает в PEL)."""
    redis = FakeSyncRedis()
    dispatcher = _make_dispatcher_for_test(redis)
    helper = MinimalSyncHelper()

    # Envelope without sid
    bad_env = {"symbol": "BTCUSDT", "targets": {}, "meta": {}}
    fields = {"payload": json.dumps(bad_env)}

    result = dispatcher._handle_one("bad-msg-1", fields, helper=helper, attempt_hint=0)

    assert result is True, "handle_one must ACK (True) even bad envelopes to avoid PEL buildup"
    acked_via_redis = any(mid == "bad-msg-1" for (_, _, mid) in redis.xack_calls)
    acked_via_helper = "bad-msg-1" in helper.acked
    assert acked_via_redis or acked_via_helper, (
        f"bad message must be ACKed either via redis.xack or helper.ack; "
        f"redis.xack_calls={redis.xack_calls}, helper.acked={helper.acked}"
    )
    assert redis.stream_len(dispatcher.dlq_stream) >= 1, (
        f"DLQ stream '{dispatcher.dlq_stream}' must receive the failed message"
    )


# ---------------------------------------------------------------------------
# Test 6: Dispatcher DLQ — bad JSON envelope
# ---------------------------------------------------------------------------

def test_dispatcher_dlq_on_bad_json():
    """Невалидный JSON в envelope идёт в DLQ и ACK-ается."""
    redis = FakeSyncRedis()
    dispatcher = _make_dispatcher_for_test(redis)
    helper = MinimalSyncHelper()

    # corrupt JSON
    fields = {"payload": "{not-valid-json"}

    result = dispatcher._handle_one("corrupt-msg-1", fields, helper=helper, attempt_hint=0)

    assert result is True, "Must ACK corrupt messages"
    acked_via_redis = any(mid == "corrupt-msg-1" for (_, _, mid) in redis.xack_calls)
    acked_via_helper = "corrupt-msg-1" in helper.acked
    assert acked_via_redis or acked_via_helper, (
        f"corrupt message must be ACKed either via redis.xack or helper.ack; "
        f"redis.xack_calls={redis.xack_calls}, helper.acked={helper.acked}"
    )
    assert redis.stream_len(dispatcher.dlq_stream) >= 1, "DLQ must receive corrupt message"


# ---------------------------------------------------------------------------
# Test 7: ENV flag CRYPTO_SHADOW_OUTBOX
# ---------------------------------------------------------------------------

def test_crypto_shadow_outbox_env_parsed():
    """CRYPTO_SHADOW_OUTBOX=1 должен распознаваться корректно."""
    for val in ("1", "true", "yes", "on"):
        os.environ["CRYPTO_SHADOW_OUTBOX"] = val
        result = os.getenv("CRYPTO_SHADOW_OUTBOX", "0").lower() in {"1", "true", "yes", "on"}
        assert result is True, f"CRYPTO_SHADOW_OUTBOX={val!r} should parse as True"

    for val in ("0", "false", "no", "off", ""):
        os.environ["CRYPTO_SHADOW_OUTBOX"] = val
        result = os.getenv("CRYPTO_SHADOW_OUTBOX", "0").lower() in {"1", "true", "yes", "on"}
        assert result is False, f"CRYPTO_SHADOW_OUTBOX={val!r} should parse as False"

    # cleanup
    os.environ.pop("CRYPTO_SHADOW_OUTBOX", None)
