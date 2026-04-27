# -*- coding: utf-8 -*-
"""Unit-тесты контракта fill_event (A3).

Покрывают:
  - normalize_fill_event
  - validate_fill_event

Запуск:
  pytest python-worker/tests/test_fill_event_contract.py -v
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
from typing import Any, Dict

import pytest

from services.posttrade.fill_event_contract import (
    normalize_fill_event,
    validate_fill_event,
)

NOW_MS = get_ny_time_millis()


def _base_fill(**overrides: Any) -> Dict[str, Any]:
    """Минимально полный fill event."""
    base: Dict[str, Any] = {
        "sid": "sid-abc-001",
        "order_id": "ord-123",
        "ts_fill_ms": NOW_MS,
        "px": 1850.5,
        "qty": 0.10,
        "fee_bps": 2.5,
        "venue": "binance",
        "symbol": "XAUUSD",
        "side": "LONG",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------------------- #
# normalize_fill_event
# --------------------------------------------------------------------------- #

class TestNormalizeFillEvent:
    def test_basic_passthrough(self):
        out = normalize_fill_event(_base_fill())
        assert out["sid"] == "sid-abc-001"
        assert out["order_id"] == "ord-123"
        assert out["px"] == 1850.5
        assert out["qty"] == 0.10
        assert out["fee_bps"] == 2.5

    def test_side_buy_to_long(self):
        out = normalize_fill_event({**_base_fill(), "side": "BUY"})
        assert out["side"] == "LONG"

    def test_side_sell_to_short(self):
        out = normalize_fill_event({**_base_fill(), "side": "SELL"})
        assert out["side"] == "SHORT"

    def test_alias_price_to_px(self):
        raw = _base_fill()
        raw.pop("px", None)
        raw["price"] = 2000.0
        out = normalize_fill_event(raw)
        assert out["px"] == 2000.0

    def test_alias_qty_from_q(self):
        raw = _base_fill()
        raw.pop("qty", None)
        raw["q"] = 0.05
        out = normalize_fill_event(raw)
        assert out["qty"] == 0.05

    def test_alias_venue_from_exchange(self):
        raw = _base_fill()
        raw.pop("venue", None)
        raw["exchange"] = "bybit"
        out = normalize_fill_event(raw)
        assert out["venue"] == "bybit"

    def test_missing_optional_bbo(self):
        out = normalize_fill_event(_base_fill())
        assert out.get("bid_at_fill") is None
        assert out.get("ask_at_fill") is None

    def test_bbo_passthrough(self):
        raw = _base_fill()
        raw["bid_at_fill"] = 1849.9
        raw["ask_at_fill"] = 1851.1
        out = normalize_fill_event(raw)
        assert out["bid_at_fill"] == 1849.9
        assert out["ask_at_fill"] == 1851.1

    def test_signal_id_as_sid_alias(self):
        raw = _base_fill()
        raw.pop("sid")
        raw["signal_id"] = "alias-sid-x"
        out = normalize_fill_event(raw)
        assert out["sid"] == "alias-sid-x"

    def test_missing_all_returns_nones(self):
        out = normalize_fill_event({})
        assert out["sid"] is None
        assert out["order_id"] is None
        assert out["px"] is None

    def test_no_mutation_of_input(self):
        raw = _base_fill()
        original = dict(raw)
        normalize_fill_event(raw)
        assert raw == original


# --------------------------------------------------------------------------- #
# validate_fill_event
# --------------------------------------------------------------------------- #

class TestValidateFillEvent:
    def test_valid_normalized_event(self):
        out = normalize_fill_event(_base_fill())
        ok, missing = validate_fill_event(out)
        assert ok is True
        assert missing == []

    def test_missing_sid(self):
        raw = _base_fill()
        raw.pop("sid")
        out = normalize_fill_event(raw)
        ok, missing = validate_fill_event(out)
        assert not ok
        assert "sid" in missing

    def test_missing_venue(self):
        raw = _base_fill()
        raw.pop("venue")
        out = normalize_fill_event(raw)
        ok, missing = validate_fill_event(out)
        assert not ok
        assert "venue" in missing

    def test_missing_px(self):
        raw = _base_fill()
        raw.pop("px", None)
        raw.pop("price", None)
        out = normalize_fill_event(raw)
        ok, missing = validate_fill_event(out)
        assert not ok
        assert "px" in missing

    def test_missing_qty(self):
        raw = _base_fill()
        raw.pop("qty", None)
        raw.pop("q", None)
        out = normalize_fill_event(raw)
        ok, missing = validate_fill_event(out)
        assert not ok
        assert "qty" in missing

    def test_missing_ts_fill_ms(self):
        raw = _base_fill()
        raw.pop("ts_fill_ms", None)
        out = normalize_fill_event(raw)
        ok, missing = validate_fill_event(out)
        assert not ok
        assert "ts_fill_ms" in missing


# --------------------------------------------------------------------------- #
# Round-trip
# --------------------------------------------------------------------------- #

class TestRoundTrip:
    def test_good_fill_passes(self):
        raw = {
            "sid": "sid-rt-01",
            "order_id": "oid-99",
            "ts_fill_ms": NOW_MS,
            "px": 50000.0,
            "qty": 0.001,
            "fee_bps": 3.5,
            "venue": "binance",
            "symbol": "BTCUSDT",
            "side": "SHORT",
        }
        out = normalize_fill_event(raw)
        ok, missing = validate_fill_event(out)
        assert ok, f"Expected ok, got missing: {missing}"

    def test_buy_side_normalized(self):
        raw = dict(_base_fill(), side="BUY")
        out = normalize_fill_event(raw)
        ok, _ = validate_fill_event(out)
        assert ok
        assert out["side"] == "LONG"
