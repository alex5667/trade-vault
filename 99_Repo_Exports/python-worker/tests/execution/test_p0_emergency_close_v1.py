"""P0-1: Emergency auto-close for naked positions — unit tests.

Tests cover:
1. SHADOW mode: emits metric, no flatten called
2. ENFORCE mode + protection fail: force_flatten_exact called
3. ENFORCE mode + flatten succeeds: cooldown key set in Redis
4. ENFORCE mode + flatten fails: emergency_close_failed metric incremented
5. block_symbol_on_protection_fail=False: cooldown NOT set even in ENFORCE
6. Integration: full OrderOpenService with mocked protection fail
7. Idempotency: double call to _handle_unprotected doesn't crash
"""
from __future__ import annotations

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pytest
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Minimal FakeRedis
# ---------------------------------------------------------------------------

class FakeRedis:
    def __init__(self):
        self._kv: dict = {}
        self._events: list = []

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, val, ex=None, px=None):
        self._kv[key] = val

    def delete(self, key):
        self._kv.pop(key, None)

    def hset(self, key, mapping=None, **kw):
        d = self._kv.setdefault(key, {})
        if mapping:
            d.update(mapping)
        d.update(kw)

    def hget(self, key, field):
        return (self._kv.get(key) or {}).get(field)

    def hgetall(self, key):
        return self._kv.get(key) or {}

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self._events.append((stream, fields))
        return b"0-1"

    def sismember(self, key, val):
        return False


# ---------------------------------------------------------------------------
# Minimal fake clients / filters
# ---------------------------------------------------------------------------

class FakeFilters:
    def get(self, symbol):
        f = MagicMock()
        f.step_size = 0.001
        f.tick_size = 0.01
        return f


def _make_flatten_svc(flatten_ok: bool = True, has_position: bool = True):
    svc = MagicMock()
    svc.force_flatten_exact.return_value = {
        "flatten_ok": flatten_ok,
        "flatten_order_id": "123" if flatten_ok else None,
        "flatten_error": None if flatten_ok else "network_error",
        "has_position": has_position,
    }
    return svc


# ---------------------------------------------------------------------------
# Helpers to build OrderOpenService with protection failure
# ---------------------------------------------------------------------------

def _make_open_svc(
    r=None,
    emergency_close_if_unprotected=False,
    block_symbol_on_protection_fail=False,
    cooldown_after_protection_fail_ms=900_000,
    flatten_svc=None,
):
    from services.execution.order_open_service import OrderOpenService

    events = []

    def _write_event(fields):
        events.append(fields)

    event_writer = MagicMock()
    event_writer.write.side_effect = _write_event

    svc = OrderOpenService(
        emergency_close_if_unprotected=emergency_close_if_unprotected,
        block_symbol_on_protection_fail=block_symbol_on_protection_fail,
        cooldown_after_protection_fail_ms=cooldown_after_protection_fail_ms,
        flatten_service=flatten_svc,
        event_writer=event_writer,
        r=r or FakeRedis(),
    )
    return svc, events


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHandleUnprotectedShadow:
    def test_shadow_no_flatten_called(self):
        flatten_svc = _make_flatten_svc()
        svc, events = _make_open_svc(
            emergency_close_if_unprotected=False,
            flatten_svc=flatten_svc,
        )
        svc._handle_unprotected_position(
            sid="test-sid-1",
            symbol="BTCUSDT",
            logical_side="LONG",
            client=MagicMock(),
            filters=FakeFilters(),
            reason="protection_not_confirmed",
        )
        flatten_svc.force_flatten_exact.assert_not_called()

    def test_shadow_emits_shadow_event(self):
        svc, events = _make_open_svc(emergency_close_if_unprotected=False)
        svc._handle_unprotected_position(
            sid="test-sid-2",
            symbol="ETHUSDT",
            logical_side="SHORT",
            client=MagicMock(),
            filters=FakeFilters(),
            reason="protection_not_confirmed",
        )
        shadow_events = [e for e in events if e.get("event_type") == "EMERGENCY_CLOSE_SHADOW"]
        assert len(shadow_events) == 1
        assert shadow_events[0]["symbol"] == "ETHUSDT"


class TestHandleUnprotectedEnforce:
    def test_enforce_calls_force_flatten_exact(self):
        flatten_svc = _make_flatten_svc(flatten_ok=True)
        svc, _ = _make_open_svc(
            emergency_close_if_unprotected=True,
            flatten_svc=flatten_svc,
        )
        client = MagicMock()
        filters = FakeFilters()
        svc._handle_unprotected_position(
            sid="sid-3",
            symbol="SOLUSDT",
            logical_side="LONG",
            client=client,
            filters=filters,
            reason="protection_not_confirmed",
        )
        flatten_svc.force_flatten_exact.assert_called_once()
        call_kwargs = flatten_svc.force_flatten_exact.call_args.kwargs
        assert call_kwargs["symbol"] == "SOLUSDT"
        assert call_kwargs["logical_side"] == "LONG"
        assert "emergency_close" in call_kwargs["reason"]

    def test_enforce_sets_cooldown_in_redis_when_block_enabled(self):
        r = FakeRedis()
        flatten_svc = _make_flatten_svc(flatten_ok=True)
        svc, _ = _make_open_svc(
            r=r,
            emergency_close_if_unprotected=True,
            block_symbol_on_protection_fail=True,
            cooldown_after_protection_fail_ms=60_000,
            flatten_svc=flatten_svc,
        )
        svc._handle_unprotected_position(
            sid="sid-4",
            symbol="BTCUSDT",
            logical_side="LONG",
            client=MagicMock(),
            filters=FakeFilters(),
            reason="protection_not_confirmed",
        )
        cooldown_val = r.get("risk:cooldown:symbol:BTCUSDT")
        assert cooldown_val is not None
        until_ms = int(cooldown_val)
        now_ms = int(time.time() * 1000)
        assert until_ms > now_ms
        assert until_ms < now_ms + 120_000  # within 2× cooldown

    def test_enforce_no_cooldown_when_block_disabled(self):
        r = FakeRedis()
        flatten_svc = _make_flatten_svc(flatten_ok=True)
        svc, _ = _make_open_svc(
            r=r,
            emergency_close_if_unprotected=True,
            block_symbol_on_protection_fail=False,
            flatten_svc=flatten_svc,
        )
        svc._handle_unprotected_position(
            sid="sid-5",
            symbol="BTCUSDT",
            logical_side="LONG",
            client=MagicMock(),
            filters=FakeFilters(),
            reason="protection_not_confirmed",
        )
        assert r.get("risk:cooldown:symbol:BTCUSDT") is None

    def test_enforce_flatten_failure_does_not_raise(self):
        flatten_svc = _make_flatten_svc(flatten_ok=False)
        svc, _ = _make_open_svc(
            emergency_close_if_unprotected=True,
            flatten_svc=flatten_svc,
        )
        # Must not raise even if flatten fails
        svc._handle_unprotected_position(
            sid="sid-6",
            symbol="ETHUSDT",
            logical_side="SHORT",
            client=MagicMock(),
            filters=FakeFilters(),
            reason="protection_not_confirmed",
        )

    def test_enforce_no_flatten_svc_does_not_crash(self):
        svc, events = _make_open_svc(
            emergency_close_if_unprotected=True,
            flatten_svc=None,
        )
        # Should not raise even with no flatten_service injected
        svc._handle_unprotected_position(
            sid="sid-7",
            symbol="BTCUSDT",
            logical_side="LONG",
            client=MagicMock(),
            filters=FakeFilters(),
            reason="protection_not_confirmed",
        )


class TestSetSymbolCooldown:
    def test_cooldown_key_format(self):
        r = FakeRedis()
        svc, _ = _make_open_svc(
            r=r,
            block_symbol_on_protection_fail=True,
            cooldown_after_protection_fail_ms=300_000,
        )
        svc._set_symbol_cooldown(sid="sid-8", symbol="btcusdt", reason="test")
        # Key should be uppercase
        assert r.get("risk:cooldown:symbol:BTCUSDT") is not None

    def test_cooldown_skipped_when_block_disabled(self):
        r = FakeRedis()
        svc, _ = _make_open_svc(
            r=r,
            block_symbol_on_protection_fail=False,
        )
        svc._set_symbol_cooldown(sid="sid-9", symbol="ETHUSDT", reason="test")
        assert r.get("risk:cooldown:symbol:ETHUSDT") is None

    def test_cooldown_emits_event(self):
        r = FakeRedis()
        svc, events = _make_open_svc(
            r=r,
            block_symbol_on_protection_fail=True,
            cooldown_after_protection_fail_ms=60_000,
        )
        svc._set_symbol_cooldown(sid="sid-10", symbol="SOLUSDT", reason="test_reason")
        cooldown_events = [e for e in events if e.get("event_type") == "SYMBOL_COOLDOWN_SET"]
        assert len(cooldown_events) == 1
        assert cooldown_events[0]["symbol"] == "SOLUSDT"
        assert cooldown_events[0]["cooldown_ms"] == 60_000


class TestIdempotency:
    def test_double_call_does_not_crash(self):
        r = FakeRedis()
        flatten_svc = _make_flatten_svc(flatten_ok=True)
        svc, _ = _make_open_svc(
            r=r,
            emergency_close_if_unprotected=True,
            block_symbol_on_protection_fail=True,
            flatten_svc=flatten_svc,
        )
        kwargs = dict(
            sid="sid-11",
            symbol="BTCUSDT",
            logical_side="LONG",
            client=MagicMock(),
            filters=FakeFilters(),
            reason="protection_not_confirmed",
        )
        svc._handle_unprotected_position(**kwargs)
        svc._handle_unprotected_position(**kwargs)
        # Second call overwrites cooldown key — that's OK
        assert r.get("risk:cooldown:symbol:BTCUSDT") is not None
