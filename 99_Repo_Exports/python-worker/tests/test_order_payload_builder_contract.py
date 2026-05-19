"""Contract tests for OrderPayloadBuilder.publish_orders_queue.

Verifies field encoding for side/direction in the orders queue payload:
  - direction : "buy" | "sell"   (Side.value.lower())
  - side      : "BUY" | "SELL"   (Side.value)
  - side_int  : 1 | -1           (NormalizedSide.side_int)

Also verifies routing: MT5 → xadd, Binance → lpush.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, call


def _run(coro):
    return asyncio.run(coro)


def _make_builder(*, orders_queue_mt5="orders:queue:mt5", orders_queue_binance="orders:queue:binance"):
    from services.orderflow.order_payload_builder import OrderPayloadBuilder

    facade = MagicMock()
    facade.orders_queue_mt5 = orders_queue_mt5
    facade.orders_queue_binance = orders_queue_binance

    xadded: list[tuple] = []
    lpushed: list[tuple] = []

    redis_mock = MagicMock()
    redis_mock.xadd = AsyncMock(
        side_effect=lambda stream, fields, **kw: xadded.append((stream, dict(fields)))
    )
    redis_mock.lpush = AsyncMock(
        side_effect=lambda queue, data: lpushed.append((queue, json.loads(data)))
    )
    facade.redis = redis_mock

    builder = OrderPayloadBuilder(facade)
    builder._xadded = xadded
    builder._lpushed = lpushed
    return builder


class _Runtime:
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol


# ---------------------------------------------------------------------------
# LONG signal → MT5 venue
# ---------------------------------------------------------------------------

def test_long_mt5_side_fields():
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "venue": "mt5",
        "tick_ts": 1_716_000_000_000,
        "reason": "sweep",
    }
    _run(b.publish_orders_queue(_Runtime(), signal))

    assert len(b._xadded) == 1
    stream, cmd = b._xadded[0]
    assert stream == "orders:queue:mt5"

    # All values are stringified for Redis Stream (xadd)
    assert cmd["direction"] == "buy",    f"direction={cmd['direction']!r}"
    assert cmd["side"] == "BUY",         f"side={cmd['side']!r}"
    assert cmd["side_int"] == "1",       f"side_int={cmd['side_int']!r}"


def test_short_mt5_side_fields():
    b = _make_builder()
    signal = {
        "symbol": "ETHUSDT",
        "direction": "SHORT",
        "venue": "mt5",
        "tick_ts": 1_716_000_001_000,
    }
    _run(b.publish_orders_queue(_Runtime("ETHUSDT"), signal))

    assert len(b._xadded) == 1
    _, cmd = b._xadded[0]
    assert cmd["direction"] == "sell"
    assert cmd["side"] == "SELL"
    assert cmd["side_int"] == "-1"


# ---------------------------------------------------------------------------
# LONG signal → Binance venue (list, JSON-encoded)
# ---------------------------------------------------------------------------

def test_long_binance_side_fields():
    b = _make_builder()
    signal = {
        "symbol": "SOLUSDT",
        "side": "BUY",     # accepts Side alias too
        "venue": "binance",
        "tick_ts": 1_716_000_002_000,
    }
    _run(b.publish_orders_queue(_Runtime("SOLUSDT"), signal))

    assert len(b._lpushed) == 1
    queue, cmd = b._lpushed[0]
    assert queue == "orders:queue:binance"
    assert cmd["direction"] == "buy"
    assert cmd["side"] == "BUY"
    assert cmd["side_int"] == 1         # LONG → side_int=1, JSON-encoded integer preserved


def test_short_binance_side_fields():
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "direction": "sell",    # lowercase alias
        "venue": "binance",
        "tick_ts": 1_716_000_003_000,
    }
    _run(b.publish_orders_queue(_Runtime(), signal))

    _, cmd = b._lpushed[0]
    assert cmd["direction"] == "sell"
    assert cmd["side"] == "SELL"
    assert cmd["side_int"] == -1


# ---------------------------------------------------------------------------
# Required fields always present
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {"id", "sid", "signal_id", "symbol", "type", "direction", "side", "side_int", "source", "venue"}


def test_mt5_required_fields():
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "venue": "mt5",
        "tick_ts": 1_716_000_004_000,
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    _, cmd = b._xadded[0]
    # xadd stringifies all values; check keys
    missing = REQUIRED_FIELDS - set(cmd.keys())
    assert not missing, f"Missing required fields: {missing}"


def test_binance_required_fields():
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "direction": "SHORT",
        "venue": "binance",
        "tick_ts": 1_716_000_005_000,
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    _, cmd = b._lpushed[0]
    missing = REQUIRED_FIELDS - set(cmd.keys())
    assert not missing, f"Missing required fields: {missing}"


# ---------------------------------------------------------------------------
# No timestamp → skip (fail-safe)
# ---------------------------------------------------------------------------

def test_no_timestamp_skips_publish():
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "venue": "mt5",
        # no tick_ts / ts_event_ms / generated_at
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    assert b._xadded == []
    assert b._lpushed == []


# ---------------------------------------------------------------------------
# Upstream sid passthrough — preserves of:SYM:TS:LONG format
# ---------------------------------------------------------------------------

def test_upstream_sid_passthrough_mt5():
    """If signal already carries sid, it must be preserved verbatim (not regenerated)."""
    b = _make_builder()
    upstream_sid = "of:BTCUSDT:1716000007000:LONG"
    signal = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "venue": "mt5",
        "tick_ts": 1_716_000_007_000,
        "sid": upstream_sid,
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    _, cmd = b._xadded[0]
    assert cmd["sid"] == upstream_sid,        f"sid not preserved: {cmd['sid']!r}"
    assert cmd["signal_id"] == upstream_sid,  f"signal_id not preserved: {cmd['signal_id']!r}"


# ---------------------------------------------------------------------------
# MT5 queue not configured → skip (fail-safe)
# ---------------------------------------------------------------------------

def test_mt5_queue_not_configured_skips():
    b = _make_builder(orders_queue_mt5="")
    signal = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "venue": "mt5",
        "tick_ts": 1_716_000_006_000,
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    assert b._xadded == []

# ---------------------------------------------------------------------------
# Unknown / missing direction → skip (fail-safe, F1 regression test)
# Previously raised ValueError; now guarded and silently skipped.
# ---------------------------------------------------------------------------

def test_unknown_direction_skips_publish():
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "venue": "mt5",
        "tick_ts": 1_716_000_007_000,
        # no direction / side
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    assert b._xadded == [], "No MT5 xadd on unknown direction"
    assert b._lpushed == [], "No Binance lpush on unknown direction"


def test_garbage_direction_skips_publish():
    b = _make_builder()
    signal = {
        "symbol": "SOLUSDT",
        "direction": "SIDEWAYS",  # invalid
        "venue": "binance",
        "tick_ts": 1_716_000_008_000,
    }
    _run(b.publish_orders_queue(_Runtime("SOLUSDT"), signal))
    assert b._lpushed == [], "No Binance lpush on garbage direction"


# ---------------------------------------------------------------------------
# MT5 side_int type contract (F6): xadd stringifies; bridge normalizes to int
# ---------------------------------------------------------------------------

def test_mt5_side_int_is_string_in_stream():
    """MT5 xadd payload has side_int as string (Redis Stream stringification).
    The orders_http_bridge /orders/poll endpoint is responsible for casting to int.
    """
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "venue": "mt5",
        "tick_ts": 1_716_000_009_000,
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    _, cmd = b._xadded[0]
    assert cmd["side_int"] == "1", (
        "MT5 xadd must stringify side_int; bridge casts to int before MT5 EA sees it"
    )


def test_binance_side_int_is_int_in_json():
    """Binance lpush payload preserves side_int as native int in JSON."""
    b = _make_builder()
    signal = {
        "symbol": "BTCUSDT",
        "direction": "SHORT",
        "venue": "binance",
        "tick_ts": 1_716_000_010_000,
    }
    _run(b.publish_orders_queue(_Runtime(), signal))
    _, cmd = b._lpushed[0]
    assert cmd["side_int"] == -1 and isinstance(cmd["side_int"], int), (
        "Binance JSON must preserve side_int as integer -1"
    )
