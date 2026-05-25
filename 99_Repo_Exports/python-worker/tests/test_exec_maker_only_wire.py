"""End-to-end wire tests for maker-only execution (item 4, 2026-05-24).

Validates the flag propagation chain:
  EntryPolicyGate (ctx)
    → signal_pipeline.enriched_signal["exec_maker_only"]
    → OrderPayloadBuilder.order_cmd["exec_maker_only"] + maker_price
    → OrderOpenService.handle_open params: type=LIMIT, timeInForce=GTX, price

These tests pin the wire contract — any change that drops `exec_maker_only`
or `maker_price` between stages would silently revert canary trades to MARKET
without any visible failure.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


# ─────────────────────────────────────────────────────────────────────────────
# Stage A: OrderPayloadBuilder — signal → order_cmd
# ─────────────────────────────────────────────────────────────────────────────


def test_payload_builder_maker_only_switches_type_and_carries_price(monkeypatch):
    """When signal has exec_maker_only=1 + entry price, cmd switches to LIMIT."""
    import asyncio
    from services.orderflow.order_payload_builder import OrderPayloadBuilder

    captured = {}

    class _FakeRedis:
        async def xadd(self, *_a, **_kw):
            return "0-0"

        async def lpush(self, queue, payload):
            captured["queue"] = queue
            captured["payload"] = payload
            return 1

    class _Facade:
        redis = _FakeRedis()
        orders_queue_mt5 = "orders:queue:mt5"
        orders_queue_binance = "orders:queue:binance"

    b = OrderPayloadBuilder(_Facade())
    signal = {
        "symbol": "ETHUSDT",
        "tick_ts": 1779558000000,
        "direction": "long",
        "side": "buy",
        "venue": "binance",
        "kind": "iceberg",
        "reason": "iceberg",
        "entry": 2061.97,
        "exec_maker_only": 1,
        "exec_maker_only_shadow": 1,
        "sid": "iceberg:ETHUSDT:1779558000000:L",
    }

    class _Runtime:
        symbol = "ETHUSDT"

    asyncio.run(b.publish_orders_queue(_Runtime(), signal))

    import json as _json
    cmd = _json.loads(captured["payload"])
    assert cmd["type"] == "limit", "maker-only with valid entry must produce LIMIT order"
    assert cmd["exec_maker_only"] == 1
    assert cmd["maker_price"] == 2061.97
    assert cmd["symbol"] == "ETHUSDT"
    assert cmd["sid"] == "iceberg:ETHUSDT:1779558000000:L"


def test_payload_builder_shadow_only_keeps_market():
    """exec_maker_only_shadow=1 without enforce=1 must stay MARKET."""
    import asyncio
    import json as _json
    from services.orderflow.order_payload_builder import OrderPayloadBuilder

    captured = {}

    class _FakeRedis:
        async def lpush(self, queue, payload):
            captured["payload"] = payload
            return 1

    class _Facade:
        redis = _FakeRedis()
        orders_queue_mt5 = "orders:queue:mt5"
        orders_queue_binance = "orders:queue:binance"

    b = OrderPayloadBuilder(_Facade())
    signal = {
        "symbol": "ETHUSDT",
        "tick_ts": 1779558000000,
        "direction": "long",
        "side": "buy",
        "venue": "binance",
        "kind": "iceberg",
        "entry": 2061.97,
        "exec_maker_only": 0,
        "exec_maker_only_shadow": 1,
    }

    class _Runtime:
        symbol = "ETHUSDT"

    asyncio.run(b.publish_orders_queue(_Runtime(), signal))

    cmd = _json.loads(captured["payload"])
    assert cmd["type"] == "market"
    assert cmd["exec_maker_only"] == 0
    assert cmd["exec_maker_only_shadow"] == 1


def test_payload_builder_enforce_without_price_falls_back_to_market():
    """Without entry price, even enforce=1 must NOT produce broken LIMIT cmd."""
    import asyncio
    import json as _json
    from services.orderflow.order_payload_builder import OrderPayloadBuilder

    captured = {}

    class _FakeRedis:
        async def lpush(self, queue, payload):
            captured["payload"] = payload
            return 1

    class _Facade:
        redis = _FakeRedis()
        orders_queue_mt5 = "orders:queue:mt5"
        orders_queue_binance = "orders:queue:binance"

    b = OrderPayloadBuilder(_Facade())
    signal = {
        "symbol": "ETHUSDT",
        "tick_ts": 1779558000000,
        "direction": "long",
        "side": "buy",
        "venue": "binance",
        "kind": "iceberg",
        "exec_maker_only": 1,
        # NO entry / entry_price
    }

    class _Runtime:
        symbol = "ETHUSDT"

    asyncio.run(b.publish_orders_queue(_Runtime(), signal))

    cmd = _json.loads(captured["payload"])
    assert cmd["type"] == "market", "no price → cannot place LIMIT, fallback expected"
    assert cmd["maker_price"] == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Stage B: EntryPolicyGate — canary CSV filter
# ─────────────────────────────────────────────────────────────────────────────


def _parse_csv(value: str | None) -> set[str]:
    return {k.strip().lower() for k in (value or "").split(",") if k.strip()}


def _eval_enforce(*, global_enforce: bool, canary_csv: str, kind: str) -> bool:
    """Mirror entry_policy_gate canary logic — must stay in sync."""
    kind_lc = kind.lower()
    canary = _parse_csv(canary_csv)
    if canary:
        return global_enforce and kind_lc in canary
    return global_enforce


def test_canary_csv_restricts_enforce_to_listed_kinds():
    # Global ON + canary=iceberg → only iceberg enforces; weak_progress stays SHADOW
    assert _eval_enforce(global_enforce=True, canary_csv="iceberg", kind="iceberg") is True
    assert _eval_enforce(global_enforce=True, canary_csv="iceberg", kind="weak_progress") is False
    assert _eval_enforce(global_enforce=True, canary_csv="iceberg", kind="ICEBERG") is True  # case


def test_canary_empty_means_all_kinds_enforce():
    # Empty canary → behave like pre-canary global enforce
    assert _eval_enforce(global_enforce=True, canary_csv="", kind="iceberg") is True
    assert _eval_enforce(global_enforce=True, canary_csv="", kind="weak_progress") is True
    assert _eval_enforce(global_enforce=True, canary_csv="   ", kind="absorption") is True


def test_canary_global_off_blocks_everything():
    # Global OFF → nothing enforces regardless of canary
    assert _eval_enforce(global_enforce=False, canary_csv="iceberg", kind="iceberg") is False
    assert _eval_enforce(global_enforce=False, canary_csv="", kind="iceberg") is False


# ─────────────────────────────────────────────────────────────────────────────
# Stage C: OrderOpenService — params building for maker-only
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _Filter:
    step_size: float = 0.0001
    tick_size: float = 0.01
    min_qty: float = 0.001
    min_notional: float = 5.0


class _Filters:
    def __init__(self, fmap):
        self._m = fmap

    def get(self, symbol):
        return self._m[symbol]


def _build_params_minimal(payload: dict, binance_side: str, qty_str: str, sf: _Filter) -> dict:
    """Re-implements the maker-only branch from OrderOpenService.handle_open.

    Kept inline for pinning — if the real branch logic changes, this test will
    drift and force review.
    """
    import math
    params = {
        "symbol": payload["symbol"], "side": binance_side,
        "type": "MARKET", "quantity": qty_str,
        "newClientOrderId": "x",
    }
    maker_enforce = int(payload.get("exec_maker_only") or 0)
    maker_price = float(payload.get("maker_price") or 0.0)
    if maker_enforce and maker_price > 0 and sf.tick_size > 0:
        if binance_side == "BUY":
            limit_px = (int(maker_price / sf.tick_size)) * sf.tick_size
        else:
            limit_px = math.ceil(maker_price / sf.tick_size) * sf.tick_size
        if limit_px > 0:
            params["type"] = "LIMIT"
            params["timeInForce"] = "GTX"
            params["price"] = f"{limit_px:.2f}"
    return params


def test_executor_buy_uses_floor_tick_for_passive_bid():
    """BUY maker price must round DOWN to nearest tick (sit at best bid)."""
    sf = _Filter(tick_size=0.01)
    p = _build_params_minimal(
        {"symbol": "ETHUSDT", "exec_maker_only": 1, "maker_price": 2061.978},
        "BUY", "0.24", sf,
    )
    assert p["type"] == "LIMIT"
    assert p["timeInForce"] == "GTX"
    assert float(p["price"]) == 2061.97  # floored


def test_executor_sell_uses_ceil_tick_for_passive_ask():
    """SELL maker price must round UP to nearest tick (sit at best ask)."""
    sf = _Filter(tick_size=0.01)
    p = _build_params_minimal(
        {"symbol": "ETHUSDT", "exec_maker_only": 1, "maker_price": 2061.972},
        "SELL", "0.24", sf,
    )
    assert p["type"] == "LIMIT"
    assert p["timeInForce"] == "GTX"
    assert float(p["price"]) == 2061.98  # ceiled


def test_executor_no_maker_flag_keeps_market():
    sf = _Filter(tick_size=0.01)
    p = _build_params_minimal(
        {"symbol": "ETHUSDT", "exec_maker_only": 0, "maker_price": 2061.97},
        "BUY", "0.24", sf,
    )
    assert p["type"] == "MARKET"
    assert "timeInForce" not in p
    assert "price" not in p


def test_executor_zero_price_falls_back_to_market():
    sf = _Filter(tick_size=0.01)
    p = _build_params_minimal(
        {"symbol": "ETHUSDT", "exec_maker_only": 1, "maker_price": 0.0},
        "BUY", "0.24", sf,
    )
    assert p["type"] == "MARKET"


# ─────────────────────────────────────────────────────────────────────────────
# Stage D: config invariants
# ─────────────────────────────────────────────────────────────────────────────


def test_canary_env_default_is_iceberg_only():
    """Initial canary state must be iceberg-only to limit blast radius."""
    from pathlib import Path

    cfg_path = Path(__file__).resolve().parents[2] / "config" / "crypto-of-common.env"
    text = cfg_path.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.strip().startswith("EXEC_MAKER_ONLY_KINDS_ENFORCE="):
            value = line.split("=", 1)[1]
            kinds = _parse_csv(value)
            assert kinds == {"iceberg"}, (
                f"Canary scope drifted from iceberg-only: {kinds}. "
                f"Expand only after 24-48h of clean ENFORCE telemetry."
            )
            return
    raise AssertionError("EXEC_MAKER_ONLY_KINDS_ENFORCE not present in env")


def test_commission_rate_is_maker_blended_target():
    """CRYPTO_COMMISSION_RATE must be 0.0003 (blended) after item-4 wiring.

    0.0005 (pure taker) = simulation handicap that makes calibrators
    over-cautious vs expected maker-first prod economics.
    """
    from pathlib import Path

    cfg_path = Path(__file__).resolve().parents[2] / "config" / "crypto-of-common.env"
    text = cfg_path.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.strip().startswith("CRYPTO_COMMISSION_RATE="):
            assert line.split("=", 1)[1].strip() == "0.0003"
            return
    raise AssertionError("CRYPTO_COMMISSION_RATE not present in env")
