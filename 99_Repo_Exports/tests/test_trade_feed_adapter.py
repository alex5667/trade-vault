# -*- coding: utf-8 -*-
"""
Unit tests for adapters/trade_feed_adapter.py

Coverage:
  - Trade dataclass
  - TradeFeedAdapter: publish_trade, publish_from_dict, get_stats, reset_stats
  - TradeStreamReader: read_trades (bytes keys, str keys, empty stream, parse errors)
  - _decode_field helper (indirect via TradeStreamReader)
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from adapters.trade_feed_adapter import (
    StatsDict,
    Trade,
    TradeFeedAdapter,
    TradeStreamReader,
    _decode_field,
)


# ---------------------------------------------------------------------------
# Trade dataclass
# ---------------------------------------------------------------------------


class TestTrade:
    def test_fields(self) -> None:
        t = Trade(price=2650.5, qty=1.0, side="buy", ts=1_700_000_000_000, symbol="XAUUSD")
        assert t.price == 2650.5
        assert t.qty == 1.0
        assert t.side == "buy"
        assert t.ts == 1_700_000_000_000
        assert t.symbol == "XAUUSD"

    def test_slots(self) -> None:
        """Trade must use __slots__ for memory efficiency."""
        t = Trade(price=1.0, qty=1.0, side="sell", ts=0, symbol="BTC")
        assert hasattr(t, "__slots__")
        with pytest.raises(AttributeError):
            t.nonexistent = "x"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# _decode_field helper
# ---------------------------------------------------------------------------


class TestDecodeField:
    def test_str_key(self) -> None:
        assert _decode_field({"price": "1.5"}, "price") == "1.5"

    def test_bytes_key(self) -> None:
        assert _decode_field({b"price": b"2.5"}, "price") == "2.5"

    def test_bytes_value_str_key(self) -> None:
        assert _decode_field({"side": b"buy"}, "side") == "buy"

    def test_missing_key_returns_default(self) -> None:
        assert _decode_field({}, "missing", default="X") == "X"

    def test_missing_key_returns_none(self) -> None:
        assert _decode_field({}, "missing") is None

    def test_str_key_takes_priority_over_bytes_key(self) -> None:
        d = {"symbol": "BTC", b"symbol": b"ETH"}
        assert _decode_field(d, "symbol") == "BTC"


# ---------------------------------------------------------------------------
# TradeFeedAdapter
# ---------------------------------------------------------------------------


def _make_adapter() -> tuple[MagicMock, TradeFeedAdapter]:
    mock_r = MagicMock()
    adapter = TradeFeedAdapter(mock_r, "XAUUSD")
    return mock_r, adapter


class TestTradeFeedAdapter:
    def test_publish_trade_success(self) -> None:
        mock_r, adapter = _make_adapter()
        trade = Trade(price=2651.0, qty=2.0, side="buy", ts=1_000_000, symbol="XAUUSD")
        result = adapter.publish_trade(trade)

        assert result is True
        assert adapter.stats["trades_published"] == 1
        assert adapter.stats["errors"] == 0
        assert adapter.stats["last_trade_ts"] == 1_000_000

        mock_r.xadd.assert_called_once_with(
            "trades:XAUUSD",
            {"price": "2651.0", "qty": "2.0", "side": "buy", "ts": "1000000", "symbol": "XAUUSD"},
            maxlen=10_000,
        )

    def test_publish_trade_side_lowercased(self) -> None:
        mock_r, adapter = _make_adapter()
        trade = Trade(price=1.0, qty=1.0, side="SELL", ts=0, symbol="XAUUSD")
        adapter.publish_trade(trade)
        call_args = mock_r.xadd.call_args[0][1]
        assert call_args["side"] == "sell"

    def test_publish_trade_redis_error_returns_false(self) -> None:
        mock_r, adapter = _make_adapter()
        mock_r.xadd.side_effect = ConnectionError("Redis down")
        trade = Trade(price=1.0, qty=1.0, side="buy", ts=0, symbol="XAUUSD")
        result = adapter.publish_trade(trade)

        assert result is False
        assert adapter.stats["errors"] == 1
        assert adapter.stats["trades_published"] == 0

    def test_publish_from_dict_all_fields(self) -> None:
        mock_r, adapter = _make_adapter()
        data = {"price": "2700.5", "qty": "3.0", "side": "SELL", "ts": "999", "symbol": "BTCUSDT"}
        result = adapter.publish_from_dict(data)

        assert result is True
        call_args = mock_r.xadd.call_args[0][1]
        assert call_args["price"] == "2700.5"
        assert call_args["qty"] == "3.0"
        assert call_args["side"] == "sell"
        assert call_args["ts"] == "999"
        assert call_args["symbol"] == "BTCUSDT"

    def test_publish_from_dict_qty_fallback_to_volume(self) -> None:
        mock_r, adapter = _make_adapter()
        result = adapter.publish_from_dict({"price": "100.0", "volume": "5.0"})
        assert result is True
        call_args = mock_r.xadd.call_args[0][1]
        assert call_args["qty"] == "5.0"

    def test_publish_from_dict_side_default_buy(self) -> None:
        mock_r, adapter = _make_adapter()
        adapter.publish_from_dict({"price": "100.0"})
        assert mock_r.xadd.call_args[0][1]["side"] == "buy"

    def test_publish_from_dict_ts_fallback(self) -> None:
        mock_r, adapter = _make_adapter()
        before = int(time.time() * 1000)
        adapter.publish_from_dict({"price": "100.0"})
        after = int(time.time() * 1000)
        ts = int(mock_r.xadd.call_args[0][1]["ts"])
        assert before <= ts <= after

    def test_publish_from_dict_symbol_fallback_to_adapter_symbol(self) -> None:
        mock_r, adapter = _make_adapter()
        adapter.publish_from_dict({"price": "100.0"})
        assert mock_r.xadd.call_args[0][1]["symbol"] == "XAUUSD"

    def test_publish_from_dict_bad_price_returns_false(self) -> None:
        mock_r, adapter = _make_adapter()
        result = adapter.publish_from_dict({"price": "not_a_number"})
        assert result is False
        assert adapter.stats["errors"] == 1

    def test_get_stats_returns_copy(self) -> None:
        mock_r, adapter = _make_adapter()
        stats = adapter.get_stats()
        stats["trades_published"] = 999  # mutate returned copy
        assert adapter.stats["trades_published"] == 0  # original unchanged

    def test_reset_stats(self) -> None:
        mock_r, adapter = _make_adapter()
        trade = Trade(price=1.0, qty=1.0, side="buy", ts=1, symbol="XAUUSD")
        adapter.publish_trade(trade)
        assert adapter.stats["trades_published"] == 1
        adapter.reset_stats()
        assert adapter.stats["trades_published"] == 0
        assert adapter.stats["errors"] == 0
        assert adapter.stats["last_trade_ts"] == 0

    def test_stats_type(self) -> None:
        """get_stats must return a StatsDict."""
        _, adapter = _make_adapter()
        stats: StatsDict = adapter.get_stats()
        assert set(stats.keys()) == {"trades_published", "errors", "last_trade_ts"}


# ---------------------------------------------------------------------------
# TradeStreamReader
# ---------------------------------------------------------------------------


def _make_reader() -> tuple[MagicMock, TradeStreamReader]:
    mock_r = MagicMock()
    reader = TradeStreamReader(mock_r, "XAUUSD")
    return mock_r, reader


class TestTradeStreamReader:
    def _stream_response(self, fields: dict) -> list:
        """Build a fake xread response list."""
        return [(b"trades:XAUUSD", [(b"1700000000000-0", fields)])]

    def test_read_empty_stream(self) -> None:
        mock_r, reader = _make_reader()
        mock_r.xread.return_value = []
        result = reader.read_trades()
        assert result == []

    def test_read_none_stream(self) -> None:
        mock_r, reader = _make_reader()
        mock_r.xread.return_value = None
        result = reader.read_trades()
        assert result == []

    def test_read_str_keys(self) -> None:
        """Stream data with str keys (decode_responses=True)."""
        mock_r, reader = _make_reader()
        mock_r.xread.return_value = self._stream_response(
            {"price": "2650.0", "qty": "1.5", "side": "buy", "ts": "1700000000000", "symbol": "XAUUSD"}
        )
        trades = reader.read_trades()
        assert len(trades) == 1
        assert trades[0].price == 2650.0
        assert trades[0].qty == 1.5
        assert trades[0].side == "buy"
        assert trades[0].ts == 1_700_000_000_000
        assert trades[0].symbol == "XAUUSD"

    def test_read_bytes_keys(self) -> None:
        """Stream data with bytes keys (decode_responses=False)."""
        mock_r, reader = _make_reader()
        mock_r.xread.return_value = self._stream_response(
            {b"price": b"2651.5", b"qty": b"2.0", b"side": b"sell", b"ts": b"1700000001000", b"symbol": b"XAUUSD"}
        )
        trades = reader.read_trades()
        assert len(trades) == 1
        assert trades[0].price == 2651.5
        assert trades[0].side == "sell"

    def test_read_side_lowercased(self) -> None:
        mock_r, reader = _make_reader()
        mock_r.xread.return_value = self._stream_response(
            {"price": "1.0", "qty": "1.0", "side": "BUY", "ts": "1", "symbol": "X"}
        )
        trades = reader.read_trades()
        assert trades[0].side == "buy"

    def test_read_advances_last_id(self) -> None:
        mock_r, reader = _make_reader()
        assert reader.last_id == "0-0"
        mock_r.xread.return_value = self._stream_response(
            {"price": "1.0", "qty": "1.0", "side": "buy", "ts": "1", "symbol": "X"}
        )
        reader.read_trades()
        assert reader.last_id == "1700000000000-0"

    def test_bad_message_skipped_good_message_included(self) -> None:
        """Malformed entry must be skipped without aborting the batch."""
        mock_r, reader = _make_reader()
        bad_msg = (b"9999-0", {"price": "NOT_A_FLOAT"})
        good_msg = (b"10000-0", {"price": "100.0", "qty": "1.0", "side": "buy", "ts": "1", "symbol": "X"})
        mock_r.xread.return_value = [(b"trades:XAUUSD", [bad_msg, good_msg])]
        trades = reader.read_trades()
        assert len(trades) == 1
        assert trades[0].price == 100.0

    def test_redis_error_returns_empty_list(self) -> None:
        mock_r, reader = _make_reader()
        mock_r.xread.side_effect = ConnectionError("Redis down")
        result = reader.read_trades()
        assert result == []

    def test_count_and_block_passed_to_xread(self) -> None:
        mock_r, reader = _make_reader()
        mock_r.xread.return_value = []
        reader.read_trades(count=50, block_ms=500)
        mock_r.xread.assert_called_once_with(
            {"trades:XAUUSD": "0-0"}, count=50, block=500
        )
