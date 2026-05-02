"""P0.2 — signal side field must be BUY/SELL (execution), not LONG/SHORT."""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from services.signal_preprocess import preprocess_signal_for_publish


def _run(direction: str) -> dict:
    signal = {"symbol": "BTCUSDT", "direction": direction, "entry": 100.0}
    return preprocess_signal_for_publish(signal, "BTCUSDT", "CryptoOrderFlow", logger=None)


def test_long_produces_buy():
    out = _run("LONG")
    assert out["direction"] == "LONG"
    assert out["side"] == "BUY"
    assert out["side_lc"] == "buy"
    assert out["side_uc"] == "BUY"
    assert out["side_int"] == 1


def test_short_produces_sell():
    out = _run("SHORT")
    assert out["direction"] == "SHORT"
    assert out["side"] == "SELL"
    assert out["side_lc"] == "sell"
    assert out["side_uc"] == "SELL"
    assert out["side_int"] == -1


def test_buy_input_normalised_to_long_direction_buy_side():
    out = _run("BUY")
    assert out["direction"] == "LONG"
    assert out["side"] == "BUY"


def test_sell_input_normalised_to_short_direction_sell_side():
    out = _run("SELL")
    assert out["direction"] == "SHORT"
    assert out["side"] == "SELL"


def test_side_is_never_long_or_short():
    for d in ("LONG", "SHORT", "BUY", "SELL"):
        out = _run(d)
        assert out["side"] not in ("LONG", "SHORT"), f"side must not be {out['side']} for direction={d}"
