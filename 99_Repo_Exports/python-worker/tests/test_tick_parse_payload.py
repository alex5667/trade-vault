import time

from services.orderflow.utils import _parse_tick_payload


def test_side_is_not_defaulted_to_buy_when_missing():
    tick = _parse_tick_payload({
        "symbol": "BTCUSDT",
        "ts_ms": 1700000000000,
        "price": "100.0",
        "qty": "1.0",
    })
    assert tick["side"] == "UNKNOWN"


def test_side_inferred_from_maker_flag_binance_m_true_means_sell_taker():
    tick = _parse_tick_payload({
        "symbol": "BTCUSDT",
        "ts_ms": 1700000000000,
        "price": "100.0",
        "qty": "1.0",
        "m": True,
    })
    assert tick["side"] == "SELL"
    assert tick["side_inferred"] == 1
    assert tick["side_inferred_from"] == "maker_flag"


def test_side_inferred_from_bbo_when_unknown():
    # price closer to bid => SELL (aggressive sell)
    tick = _parse_tick_payload({
        "symbol": "BTCUSDT",
        "ts_ms": 1700000000000,
        "price": "99.9",
        "bid": "99.9",
        "ask": "100.1",
        "qty": "1.0",
    })
    assert tick["side"] in ("SELL", "BUY")
    assert tick["side_inferred"] == 1


def test_trade_id_extracted_when_present():
    tick = _parse_tick_payload({
        "symbol": "BTCUSDT",
        "ts_ms": 1700000000000,
        "price": "100.0",
        "qty": "1.0",
        "t": 123456,
    })
    assert tick["trade_id"] == "123456"
    assert isinstance(tick["tick_uid"], str) and len(tick["tick_uid"]) > 0

