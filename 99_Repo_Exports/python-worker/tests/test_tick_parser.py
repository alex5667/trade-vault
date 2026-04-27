from __future__ import annotations

import json
import pytest

from handlers.tick_parser import Tick, parse_tick, classify_delta, FLAG_TRADE, FLAG_BUY, FLAG_SELL


def test_parse_tick_flat_dict_ms() -> None:
    raw = {"ts": 1700000000000, "bid": 100.0, "ask": 101.0, "last": 100.5, "volume": 2.0, "flags": FLAG_TRADE | FLAG_BUY}
    t = parse_tick(raw, now_ms=1)
    assert t is not None
    assert t.ts == 1700000000000
    assert t.bid == pytest.approx(100.0)
    assert t.ask == pytest.approx(101.0)
    assert t.flags & FLAG_TRADE


def test_parse_tick_seconds_to_ms() -> None:
    raw = {"ts": 1700000000, "bid": 100.0, "ask": 101.0, "last": 100.5, "volume": 2.0, "flags": 0}
    t = parse_tick(raw, now_ms=1)
    assert t is not None
    assert t.ts == 1700000000 * 1000


def test_parse_tick_json_in_data_string() -> None:
    inner = {"ts": 1700000000000, "bid": "100", "ask": "101", "last": "100.5", "volume": "3", "flags": FLAG_TRADE | FLAG_SELL}
    raw = {"data": json.dumps(inner)}
    t = parse_tick(raw, now_ms=1)
    assert t is not None
    assert t.bid == pytest.approx(100.0)
    assert t.volume == pytest.approx(3.0)
    assert (t.flags & FLAG_SELL) != 0


def test_parse_tick_nested_tick() -> None:
    raw = {"data": {"tick": {"ts": 1700000000000, "bid": 100.0, "ask": 101.0, "last": 100.5, "volume": 1.0, "flags": FLAG_TRADE | FLAG_BUY}}}
    t = parse_tick(raw, now_ms=1)
    assert t is not None
    assert t.ts == 1700000000000
    assert (t.flags & FLAG_BUY) != 0


def test_parse_tick_bad_types_drop() -> None:
    # ts не парсится -> None (детерминизм: now_ms не используем как "магический" fallback тут)
    raw = {"ts": "abc", "bid": 100.0, "ask": 101.0, "last": 100.5, "volume": 1.0}
    t = parse_tick(raw, now_ms=None)
    assert t is None


def test_classify_delta_bookticker_no_trade() -> None:
    t = Tick(ts=1, bid=100.0, ask=101.0, last=100.5, volume=999.0, flags=0, is_buyer_maker=None)
    assert classify_delta(t) == 0.0


def test_classify_delta_trade_buy_sell_flags() -> None:
    t_buy = Tick(ts=1, bid=100.0, ask=101.0, last=100.5, volume=5.0, flags=FLAG_TRADE | FLAG_BUY)
    t_sell = Tick(ts=1, bid=100.0, ask=101.0, last=100.5, volume=5.0, flags=FLAG_TRADE | FLAG_SELL)
    assert classify_delta(t_buy) == pytest.approx(+5.0)
    assert classify_delta(t_sell) == pytest.approx(-5.0)


def test_classify_delta_trade_from_is_buyer_maker() -> None:
    # Binance: isBuyerMaker=True => taker sell => negative
    t1 = Tick(ts=1, bid=100.0, ask=101.0, last=100.5, volume=2.0, flags=FLAG_TRADE, is_buyer_maker=True)
    t2 = Tick(ts=1, bid=100.0, ask=101.0, last=100.5, volume=2.0, flags=FLAG_TRADE, is_buyer_maker=False)
    assert classify_delta(t1) == pytest.approx(-2.0)
    assert classify_delta(t2) == pytest.approx(+2.0)
