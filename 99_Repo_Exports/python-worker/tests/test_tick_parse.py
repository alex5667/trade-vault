import json
import os
import sys

# Ensure python-worker/ is on sys.path so `services.*` imports work in CI/pytest
HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from services.orderflow.utils import _parse_tick_payload


def test_parse_tick_payload_extracts_trade_id_and_tick_uid():
    """Test that trade_id is extracted and tick_uid is generated correctly with trade_id."""
    payload = {
        "data": json.dumps({
            "symbol": "BTCUSDT",
            "ts_ms": 1700000000123,
            "price": "50000.0",
            "qty": "0.10",
            "side": "BUY",
            "trade_id": 123,
        })
    }
    tick = _parse_tick_payload(payload)
    assert tick["trade_id"] == 123
    assert tick["tick_uid"] == "BTCUSDT:123"
    assert tick["event_ts_ms"] == 1700000000123


def test_parse_tick_payload_tick_uid_fallback_is_deterministic():
    """Test that tick_uid fallback (when no trade_id) is deterministic."""
    payload = {
        "data": json.dumps({
            "symbol": "ETHUSDT",
            "ts_ms": 1700000000456,
            "price": "3000.0",
            "qty": "1.0",
            "side": "SELL",
        })
    }
    t1 = _parse_tick_payload(payload)
    t2 = _parse_tick_payload(payload)
    assert isinstance(t1.get("tick_uid"), str)
    assert len(t1["tick_uid"]) == 16
    assert t1["tick_uid"] == t2["tick_uid"]


def test_parse_tick_payload_trade_id_zero_becomes_none():
    """Test that trade_id=0 becomes None and triggers hash fallback."""
    payload = {
        "data": json.dumps({
            "symbol": "SOLUSDT",
            "ts_ms": 1700000000789,
            "price": "100.0",
            "qty": "5.0",
            "side": "BUY",
            "trade_id": 0,
        })
    }
    tick = _parse_tick_payload(payload)
    assert tick["trade_id"] is None
    assert isinstance(tick["tick_uid"], str)
    assert len(tick["tick_uid"]) == 16
    assert not tick["tick_uid"].startswith("SOLUSDT:")


def test_parse_tick_payload_binance_agg_trade_format():
    """Test Binance aggTrade format (t/a fields)."""
    payload = {
        "data": json.dumps({
            "symbol": "BTCUSDT",
            "E": 1700000000123,  # event time
            "p": "50000.0",  # price
            "q": "0.10",  # quantity
            "m": False,  # isBuyerMaker
            "a": 456,  # aggregated trade id
        })
    }
    tick = _parse_tick_payload(payload)
    assert tick["trade_id"] == 456
    assert tick["tick_uid"] == "BTCUSDT:456"
    assert tick["ts_ms"] == 1700000000123
    assert tick["event_ts_ms"] == 1700000000123
    assert tick["price"] == 50000.0
    assert tick["qty"] == 0.10
    assert tick["is_buyer_maker"] is False
    assert tick["side"] == "BUY"  # m=False => taker BUY
