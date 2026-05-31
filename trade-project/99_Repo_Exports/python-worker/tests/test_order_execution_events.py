"""Plan 3 / Step 2 — TCA lifecycle emitter + writer tests."""
from __future__ import annotations

import json

import pytest

import asyncio

from core.order_execution_events import (
    ALLOWED_STAGES,
    Stage,
    _reset_enabled_cache,
    async_emit,
    build_event,
    emit,
)
from services.order_execution_events_writer import event_to_row, parse_event


# ─── build_event contract ────────────────────────────────────────────────────


def test_allowed_stages_match_enum():
    assert {s.value for s in Stage} == ALLOWED_STAGES


def test_build_event_minimal_fields():
    ev = build_event(
        sid="sig-1", stage="DECISION", symbol="btcusdt", side=1,
        status="ok", ts_ms=1_700_000_000_000,
    )
    assert ev["sid"] == "sig-1"
    assert ev["stage"] == "DECISION"
    assert ev["symbol"] == "BTCUSDT"  # uppercased
    assert ev["side"] == 1
    assert ev["status"] == "ok"
    assert ev["ts_ms"] == 1_700_000_000_000
    assert ev["payload"] == "{}"


def test_build_event_full_fields():
    ev = build_event(
        sid="sig-1", stage="FILL", symbol="ETHUSDT", side=-1,
        status="ok", ts_ms=1_700_000_000_500, seq=2,
        venue="binance",
        client_order_id="coid-1", exchange_order_id="eoid-1",
        px=3000.5, qty=0.1, notional_usd=300.05,
        reason_code="ok",
        latency_ms=42.5,
        payload={"slippage_bps": 1.2, "fee_bps": 0.4},
    )
    assert ev["venue"] == "binance"
    assert ev["client_order_id"] == "coid-1"
    assert ev["px"] == 3000.5
    payload = json.loads(ev["payload"])
    assert payload["slippage_bps"] == 1.2


def test_build_event_rejects_unknown_stage():
    with pytest.raises(ValueError, match="invalid stage"):
        build_event(sid="x", stage="UNKNOWN_STAGE", symbol="X", side=1, status="ok")


def test_build_event_rejects_missing_sid():
    with pytest.raises(ValueError, match="sid required"):
        build_event(sid="", stage="DECISION", symbol="X", side=1, status="ok")


def test_build_event_rejects_invalid_side():
    with pytest.raises(ValueError, match="side must be"):
        build_event(sid="x", stage="DECISION", symbol="X", side=0, status="ok")


def test_build_event_rejects_empty_status():
    with pytest.raises(ValueError, match="status required"):
        build_event(sid="x", stage="DECISION", symbol="X", side=1, status="")


def test_build_event_default_ts_ms_when_none(monkeypatch):
    """ts_ms=None → time.time()*1000 (>0)."""
    ev = build_event(sid="x", stage="DECISION", symbol="X", side=1, status="ok")
    assert ev["ts_ms"] > 0


# ─── emit() fail-open behavior ───────────────────────────────────────────────


class _FakeRedis:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.calls: list[tuple] = []

    def xadd(self, key, fields, maxlen=None, approximate=False):  # noqa: ARG002
        if self.fail:
            raise RuntimeError("redis down")
        self.calls.append((key, dict(fields)))
        return "1-0"


def test_emit_shadow_returns_true_no_redis_call(monkeypatch):
    """When ORDER_EXEC_EVENTS_ENABLED is unset/0 emit is a successful no-op."""
    monkeypatch.delenv("ORDER_EXEC_EVENTS_ENABLED", raising=False)
    _reset_enabled_cache()
    rc = _FakeRedis()
    assert emit(rc, sid="x", stage="DECISION", symbol="X", side=1, status="ok") is True
    assert rc.calls == []


def test_emit_enabled_writes_to_stream(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    rc = _FakeRedis()
    ok = emit(rc, sid="x", stage="DECISION", symbol="X", side=1, status="ok", ts_ms=1)
    assert ok is True
    assert len(rc.calls) == 1
    key, fields = rc.calls[0]
    assert key == "stream:order_exec_events"
    assert fields["sid"] == "x"
    assert fields["stage"] == "DECISION"


def test_emit_fail_open_on_invalid_input(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    rc = _FakeRedis()
    # bad stage → build_event raises → emit returns False, never crashes
    assert emit(rc, sid="x", stage="BOGUS", symbol="X", side=1, status="ok") is False
    assert rc.calls == []


def test_emit_fail_open_on_redis_error(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    rc = _FakeRedis(fail=True)
    assert emit(rc, sid="x", stage="DECISION", symbol="X", side=1, status="ok") is False


def test_emit_fail_open_with_none_redis(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    assert emit(None, sid="x", stage="DECISION", symbol="X", side=1, status="ok") is False


# ─── writer.parse_event ──────────────────────────────────────────────────────


def test_parse_event_full_row():
    fields = {
        "ts_ms": "1700000000000",
        "sid": "sig-1",
        "stage": "FILL",
        "seq": "0",
        "symbol": "btcusdt",
        "side": "1",
        "status": "ok",
        "venue": "binance",
        "client_order_id": "coid-1",
        "px": "50000.5",
        "qty": "0.001",
        "notional_usd": "50.0",
        "reason_code": "ok",
        "latency_ms": "42.0",
        "payload": json.dumps({"slip_bps": 1.2}),
    }
    ev = parse_event(fields)
    assert ev is not None
    assert ev["ts_ms"] == 1_700_000_000_000
    assert ev["symbol"] == "BTCUSDT"
    assert ev["px"] == 50000.5
    assert json.loads(ev["payload_json"])["slip_bps"] == 1.2


def test_parse_event_rejects_missing_required():
    assert parse_event({}) is None
    assert parse_event({"sid": "x"}) is None
    assert parse_event({"ts_ms": "1", "sid": "x", "stage": "DECISION", "symbol": "X"}) is None


def test_parse_event_invalid_side():
    fields = {"ts_ms": "1", "sid": "x", "stage": "DECISION", "symbol": "X", "side": "0", "status": "ok"}
    assert parse_event(fields) is None


def test_parse_event_safe_floats_handle_garbage():
    fields = {
        "ts_ms": "1", "sid": "x", "stage": "DECISION", "symbol": "X",
        "side": "1", "status": "ok", "px": "not_a_number", "qty": "",
    }
    ev = parse_event(fields)
    assert ev is not None
    assert ev["px"] is None
    assert ev["qty"] is None


def test_parse_event_payload_garbage_kept_as_raw():
    fields = {
        "ts_ms": "1", "sid": "x", "stage": "DECISION", "symbol": "X",
        "side": "1", "status": "ok", "payload": "{not valid json",
    }
    ev = parse_event(fields)
    assert ev is not None
    payload = json.loads(ev["payload_json"])
    assert payload["_raw"] == "{not valid json"


# ─── async_emit (mirrors emit but on aioredis-style client) ──────────────────


class _FakeAsyncRedis:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.calls: list[tuple] = []

    async def xadd(self, key, fields, maxlen=None, approximate=False):  # noqa: ARG002
        if self.fail:
            raise RuntimeError("async redis down")
        self.calls.append((key, dict(fields)))
        return "1-0"


def test_async_emit_shadow_returns_true_no_call(monkeypatch):
    monkeypatch.delenv("ORDER_EXEC_EVENTS_ENABLED", raising=False)
    _reset_enabled_cache()
    rc = _FakeAsyncRedis()
    ok = asyncio.run(async_emit(rc, sid="x", stage="DECISION", symbol="X", side=1, status="ok"))
    assert ok is True
    assert rc.calls == []


def test_async_emit_enabled_writes(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    rc = _FakeAsyncRedis()
    ok = asyncio.run(async_emit(
        rc, sid="s", stage="SIGNAL_PUBLISHED", symbol="BTC", side=-1, status="ok", ts_ms=42,
    ))
    assert ok is True
    assert len(rc.calls) == 1
    key, fields = rc.calls[0]
    assert key == "stream:order_exec_events"
    assert fields["stage"] == "SIGNAL_PUBLISHED"
    assert fields["side"] == -1


def test_async_emit_fail_open_on_redis_error(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    rc = _FakeAsyncRedis(fail=True)
    ok = asyncio.run(async_emit(rc, sid="s", stage="DECISION", symbol="X", side=1, status="ok"))
    assert ok is False


def test_async_emit_fail_open_on_invalid(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    rc = _FakeAsyncRedis()
    ok = asyncio.run(async_emit(rc, sid="x", stage="BOGUS", symbol="X", side=1, status="ok"))
    assert ok is False
    assert rc.calls == []


def test_async_emit_with_none_client(monkeypatch):
    monkeypatch.setenv("ORDER_EXEC_EVENTS_ENABLED", "1")
    _reset_enabled_cache()
    assert asyncio.run(async_emit(None, sid="x", stage="DECISION", symbol="X", side=1, status="ok")) is False


def test_event_to_row_tuple_shape():
    ev = parse_event({
        "ts_ms": "100", "sid": "s", "stage": "FILL", "seq": "1",
        "symbol": "X", "side": "1", "status": "ok",
    })
    row = event_to_row(ev)
    # 16 columns per INSERT_SQL
    assert len(row) == 16
    assert row[0] == 100
    assert row[1] == "s"
    assert row[2] == "FILL"
    assert row[3] == 1
