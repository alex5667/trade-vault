"""MT5 kill switch + default-venue regression.

MT5 is intentionally disabled in production (2026-05-19). These tests pin
two invariants:

1. core.mt5_kill_switch.mt5_enabled() defaults to False.
2. OrderPayloadBuilder defaults venue to "binance" (was "mt5") and
   suppresses publish to orders:queue:mt5 when MT5_ENABLED is off.
"""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ── mt5_enabled() ────────────────────────────────────────────────────────────

def test_mt5_enabled_default_false(monkeypatch):
    monkeypatch.delenv("MT5_ENABLED", raising=False)
    from core.mt5_kill_switch import mt5_enabled
    assert mt5_enabled() is False


def test_mt5_enabled_truthy_variants(monkeypatch):
    from core.mt5_kill_switch import mt5_enabled
    for v in ("1", "true", "TRUE", "yes", "on", "True"):
        monkeypatch.setenv("MT5_ENABLED", v)
        assert mt5_enabled() is True, v


def test_mt5_enabled_falsy_variants(monkeypatch):
    from core.mt5_kill_switch import mt5_enabled
    for v in ("0", "false", "no", "off", "", "  ", "random_other"):
        monkeypatch.setenv("MT5_ENABLED", v)
        assert mt5_enabled() is False, v


# ── OrderPayloadBuilder defaults ─────────────────────────────────────────────

def _make_builder(*, queue_mt5="orders:queue:mt5", queue_binance="orders:queue:binance"):
    from services.orderflow.order_payload_builder import OrderPayloadBuilder

    xadded: list[tuple] = []
    lpushed: list[tuple] = []

    redis_mock = MagicMock()
    redis_mock.xadd = AsyncMock(
        side_effect=lambda stream, fields, **kw: xadded.append((stream, dict(fields)))
    )
    redis_mock.lpush = AsyncMock(
        side_effect=lambda queue, data: lpushed.append((queue, json.loads(data)))
    )

    facade = SimpleNamespace(
        redis=redis_mock,
        orders_queue_mt5=queue_mt5,
        orders_queue_binance=queue_binance,
    )
    return OrderPayloadBuilder(facade), xadded, lpushed


def _signal(**overrides):
    base = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "tick_ts": 1779131220929,
        "reason": "test",
        "sid": "of:BTCUSDT:1779131220929:LONG",
    }
    base.update(overrides)
    return base


def test_default_venue_is_binance_not_mt5(monkeypatch):
    """Signals without explicit venue must NOT land in MT5 stream."""
    monkeypatch.delenv("MT5_ENABLED", raising=False)
    builder, xadded, lpushed = _make_builder()
    runtime = SimpleNamespace(symbol="BTCUSDT")

    _run(builder.publish_orders_queue(runtime, _signal()))

    # Nothing must be XADDed to MT5 stream.
    assert all(stream != "orders:queue:mt5" for stream, _ in xadded), xadded
    # Binance list must receive the order.
    assert lpushed, "expected at least one lpush to binance queue"
    assert lpushed[0][0] == "orders:queue:binance"
    assert lpushed[0][1]["venue"] == "binance"


def test_explicit_mt5_venue_suppressed_when_disabled(monkeypatch):
    """Even explicit venue='mt5' must no-op when MT5_ENABLED is off."""
    monkeypatch.delenv("MT5_ENABLED", raising=False)
    builder, xadded, lpushed = _make_builder()
    runtime = SimpleNamespace(symbol="BTCUSDT")

    _run(builder.publish_orders_queue(runtime, _signal(venue="mt5")))

    assert not xadded, "MT5 publish must be suppressed by kill switch"
    assert not lpushed, "Binance fallback must NOT happen for explicit mt5 venue"


def test_explicit_mt5_venue_works_when_enabled(monkeypatch):
    """Re-enable path: MT5_ENABLED=1 restores XADD to orders:queue:mt5."""
    monkeypatch.setenv("MT5_ENABLED", "1")
    builder, xadded, lpushed = _make_builder()
    runtime = SimpleNamespace(symbol="BTCUSDT")

    _run(builder.publish_orders_queue(runtime, _signal(venue="mt5")))

    assert xadded, "MT5 stream must receive the order when MT5_ENABLED=1"
    assert xadded[0][0] == "orders:queue:mt5"
    assert xadded[0][1]["venue"] == "mt5"
    assert not lpushed


def test_explicit_binance_venue_unaffected(monkeypatch):
    """Binance path must work identically regardless of MT5_ENABLED."""
    for env_val in ("0", "1"):
        monkeypatch.setenv("MT5_ENABLED", env_val)
        builder, xadded, lpushed = _make_builder()
        runtime = SimpleNamespace(symbol="BTCUSDT")

        _run(builder.publish_orders_queue(runtime, _signal(venue="binance")))

        assert not xadded, f"unexpected mt5 XADD with MT5_ENABLED={env_val}"
        assert lpushed and lpushed[0][0] == "orders:queue:binance"
