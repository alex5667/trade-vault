"""
Unit tests for _parse_tick_payload focusing on side/is_buyer_maker/trade_id/tick_uid.

Tests cover:
- Side inference from is_buyer_maker (Binance semantics)
- UNKNOWN side when neither side nor is_buyer_maker present
- Trade ID extraction (including Binance 'a' for aggTrade)
- Deterministic tick_uid generation
- Edge cases and robustness

Note: tick_uid format (current implementation):
  - trade_id present:  "{SYMBOL}:{trade_id}"    e.g. "BTCUSDT:12345"
  - stream_id present: "{SYMBOL}:mid{stream_id}" e.g. "BTCUSDT:mid1700000000000-0"
  - fallback hash:     "{SYMBOL}:h{hex8}"        e.g. "BTCUSDT:h1a2b3c4d"
"""

import unittest
import sys
import os

# Add parent directory to path to import from services.orderflow
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from services.orderflow.utils import _parse_tick_payload, _compute_tick_uid


class TestTickParse(unittest.TestCase):
    """Test cases for _parse_tick_payload."""

    def test_side_from_is_buyer_maker_true(self):
        """is_buyer_maker=True => side=SELL (Binance: buyer is maker => taker SELL)."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1, "is_buyer_maker": True,
        })
        self.assertEqual(tick["side"], "SELL")
        self.assertEqual(tick["is_buyer_maker"], True)

    def test_side_from_is_buyer_maker_false(self):
        """is_buyer_maker=False => side=BUY."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1, "is_buyer_maker": False,
        })
        self.assertEqual(tick["side"], "BUY")
        self.assertEqual(tick["is_buyer_maker"], False)

    def test_side_unknown_when_no_hints(self):
        """side=UNKNOWN when neither side nor is_buyer_maker present."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1,
        })
        self.assertEqual(tick["side"], "UNKNOWN")
        self.assertIsNone(tick.get("is_buyer_maker"))

    def test_side_explicit_buy(self):
        """Explicit side=BUY is preserved; is_buyer_maker is NOT inferred from side."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1, "side": "BUY",
        })
        self.assertEqual(tick["side"], "BUY")
        # is_buyer_maker is NOT inferred from explicit side — stays None
        self.assertIsNone(tick["is_buyer_maker"])

    def test_side_explicit_sell(self):
        """Explicit side=SELL is preserved; is_buyer_maker is NOT inferred from side."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1, "side": "SELL",
        })
        self.assertEqual(tick["side"], "SELL")
        # is_buyer_maker is NOT inferred from explicit side — stays None
        self.assertIsNone(tick["is_buyer_maker"])

    def test_binance_m_flag(self):
        """Binance 'm' field is recognized as is_buyer_maker."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1,
            "m": True,  # Binance trade stream isBuyerMaker
        })
        self.assertEqual(tick["side"], "SELL")   # m=True => taker SELL
        self.assertEqual(tick["is_buyer_maker"], True)

    def test_trade_id_extraction(self):
        """trade_id is extracted from various field names; tick_uid uses {SYMBOL}:{trade_id} format."""
        cases = [
            ("trade_id", 12345),
            ("tradeId", 67890),
            ("t", 11111),
            ("a", 22222),
        ]
        for field, tid in cases:
            with self.subTest(field=field, tid=tid):
                tick = _parse_tick_payload({
                    "symbol": "BTCUSDT", "ts_ms": 1700000000000,
                    "qty": 0.1, field: tid,
                })
                self.assertIsNotNone(tick, f"None returned for field={field}")
                self.assertEqual(tick["trade_id"], tid)
                self.assertEqual(tick["tick_uid"], f"BTCUSDT:{tid}")

    def test_tick_uid_deterministic(self):
        """tick_uid is deterministic; trade_id format is {SYMBOL}:{trade_id}."""
        payload = {
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1, "side": "BUY", "trade_id": 12345,
        }
        tick1 = _parse_tick_payload(payload)
        tick2 = _parse_tick_payload(payload)
        self.assertEqual(tick1["tick_uid"], tick2["tick_uid"])
        self.assertEqual(tick1["tick_uid"], "BTCUSDT:12345")

    def test_tick_uid_without_trade_id(self):
        """tick_uid falls back to {SYMBOL}:h{hex8} content hash."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "price": 50000.0, "qty": 0.1, "side": "BUY",
        })
        self.assertIsNotNone(tick)
        self.assertTrue(tick["tick_uid"].startswith("BTCUSDT:h"), f"Got: {tick['tick_uid']!r}")

    def test_tick_uid_different_for_different_ticks(self):
        """Different ticks produce different tick_uid."""
        base = {"symbol": "BTCUSDT", "price": 50000.0, "qty": 0.1, "side": "BUY"}
        tick1 = _parse_tick_payload({**base, "ts_ms": 1700000000000})
        tick2 = _parse_tick_payload({**base, "ts_ms": 1700000000001})
        self.assertNotEqual(tick1["tick_uid"], tick2["tick_uid"])

    def test_is_buyer_maker_bool_normalization(self):
        """is_buyer_maker int 1/0 mapped correctly."""
        tick_t = _parse_tick_payload(
            {"symbol": "BTCUSDT", "ts_ms": 1700000000000, "qty": 0.1, "is_buyer_maker": 1}
        )
        self.assertEqual(tick_t["is_buyer_maker"], True)

        tick_f = _parse_tick_payload(
            {"symbol": "BTCUSDT", "ts_ms": 1700000000000, "qty": 0.1, "is_buyer_maker": 0}
        )
        self.assertEqual(tick_f["is_buyer_maker"], False)

    def test_is_buyer_maker_string_normalization(self):
        """is_buyer_maker strings '1'/'true'/'yes'/'on' and '0'/'false'/'no'/'off' work."""
        for val in ["1", "true", "yes", "on"]:
            tick = _parse_tick_payload(
                {"symbol": "BTCUSDT", "ts_ms": 1700000000000, "qty": 0.1, "is_buyer_maker": val}
            )
            self.assertEqual(tick["is_buyer_maker"], True, f"Expected True for {val!r}")

        for val in ["0", "false", "no", "off"]:
            tick = _parse_tick_payload(
                {"symbol": "BTCUSDT", "ts_ms": 1700000000000, "qty": 0.1, "is_buyer_maker": val}
            )
            self.assertEqual(tick["is_buyer_maker"], False, f"Expected False for {val!r}")

    def test_event_ts_ms_priority(self):
        """ts_ms field is used for timestamp."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000001000, "qty": 0.1, "price": 50000.0,
        })
        self.assertEqual(tick["ts_ms"], 1700000001000)

    def test_empty_payload(self):
        """Payload without qty returns None."""
        self.assertIsNone(_parse_tick_payload({}))

    def test_invalid_payload_type(self):
        """Non-dict payload types return None."""
        self.assertIsNone(_parse_tick_payload(None))
        self.assertIsNone(_parse_tick_payload([]))

    def test_side_not_coerced_to_buy_or_sell(self):
        """Unknown side values become UNKNOWN (not coerced to BUY/SELL)."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000,
            "qty": 0.1, "side": "UNKNOWN_VALUE",
        })
        self.assertIsNotNone(tick)
        self.assertEqual(tick["side"], "UNKNOWN")

    def test_nested_data_field(self):
        """Nested JSON in 'data' field is merged correctly."""
        import json
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT",
            "data": json.dumps({"ts_ms": 1700000000000, "price": 50000.0, "side": "BUY", "qty": 0.1}),
        })
        self.assertIsNotNone(tick)
        self.assertEqual(tick["ts_ms"], 1700000000000)
        self.assertEqual(tick["price"], 50000.0)
        self.assertEqual(tick["side"], "BUY")

    def test_parse_tick_payload_binance_aggtrade_like(self):
        """Binance aggTrade stream format (s/E/p/q/m/t)."""
        import json
        payload = {
            "data": json.dumps({
                "s": "BTCUSDT",
                "E": 1700000000123,
                "p": "100.0",
                "q": "0.1",
                "m": True,   # isBuyerMaker
                "t": 12345,  # trade id
            })
        }
        tick = _parse_tick_payload(payload)
        self.assertIsNotNone(tick)
        self.assertEqual(tick["symbol"], "BTCUSDT")
        self.assertEqual(tick["ts_ms"], 1700000000123)
        self.assertEqual(tick["trade_id"], 12345)
        self.assertEqual(tick["is_buyer_maker"], True)
        self.assertEqual(tick["side"], "SELL")   # isBuyerMaker=True => taker SELL
        self.assertEqual(tick["tick_uid"], "BTCUSDT:12345")

    def test_parse_tick_payload_fallback_hash_uid_is_stable(self):
        """Fallback hash-based tick_uid is stable for identical ticks."""
        payload = {
            "symbol": "ETHUSDT", "ts_ms": 1700000000999,
            "price": "2500.5", "qty": "0.02", "side": "BUY",
        }
        a = _parse_tick_payload(payload)
        b = _parse_tick_payload(payload)
        self.assertIsNone(a["trade_id"])
        # Fallback format: ETHUSDT:hXXXXXXXX
        self.assertTrue(a["tick_uid"].startswith("ETHUSDT:h"), f"Got: {a['tick_uid']!r}")
        self.assertEqual(a["tick_uid"], b["tick_uid"])

    def test_qty_zero_returns_none(self):
        """Payload with qty=0 returns None (invalid trade volume)."""
        tick = _parse_tick_payload({
            "symbol": "BTCUSDT", "ts_ms": 1700000000000, "price": 50000.0, "qty": 0.0,
        })
        self.assertIsNone(tick)

    def test_stream_id_uid_format(self):
        """When no trade_id but stream_id provided, uid uses {SYMBOL}:mid{stream_id}."""
        uid = _compute_tick_uid(
            symbol="BTCUSDT",
            trade_id=None,
            ts_ms=1700000000000,
            price_src="50000",
            qty_src="0.1",
            side="BUY",
            is_buyer_maker=False,
            stream_id="1700000000000-0",
        )
        self.assertEqual(uid, "BTCUSDT:mid1700000000000-0")


if __name__ == "__main__":
    unittest.main()
