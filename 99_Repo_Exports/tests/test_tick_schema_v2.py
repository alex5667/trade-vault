import os
import sys
import types
import unittest

# Ensure project root is importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# --- Optional stubs for running tests in a partial checkout ---
# In the full repo these modules should exist; stubs activate only if imports fail.

# redis.exceptions
try:
    import redis.exceptions  # type: ignore
except Exception:
    redis_mod = types.ModuleType("redis")
    exc_mod = types.ModuleType("redis.exceptions")

    class RedisError(Exception):
        pass

    exc_mod.RedisError = RedisError
    redis_mod.exceptions = exc_mod
    sys.modules.setdefault("redis", redis_mod)
    sys.modules.setdefault("redis.exceptions", exc_mod)

# services.pnl_math (referenced by services/orderflow/configuration.py)
try:
    import services.pnl_math  # type: ignore
except Exception:
    pnl_mod = types.ModuleType("services.pnl_math")

    def get_symbol_info(*_a, **_k):
        return {}

    def calculate_position_size(*_a, **_k):
        return 0.0

    pnl_mod.get_symbol_info = get_symbol_info
    pnl_mod.calculate_position_size = calculate_position_size
    sys.modules.setdefault("services.pnl_math", pnl_mod)

from services.orderflow.utils import _parse_tick_payload, _compute_tick_uid


class TestTickSchemaV2(unittest.TestCase):
    def test_side_maker_inference(self):
        tick = _parse_tick_payload(
            {"symbol": "BTCUSDT", "E": 1700000000000, "price": "50000", "qty": "0.01", "m": True}
        )
        self.assertIsNotNone(tick)
        assert tick is not None
        self.assertEqual(tick["side"], "SELL")
        self.assertEqual(tick["side_conf"], "maker")
        self.assertIsNone(tick["side_raw"])
        self.assertTrue(tick["is_buyer_maker"])

    def test_side_unknown_when_missing(self):
        tick = _parse_tick_payload(
            {"symbol": "BTCUSDT", "E": 1700000000000, "price": "50000", "qty": "0.01"}
        )
        self.assertIsNotNone(tick)
        assert tick is not None
        self.assertEqual(tick["side"], "UNKNOWN")
        self.assertEqual(tick["side_conf"], "unknown")
        self.assertIsNone(tick["is_buyer_maker"])

    def test_explicit_wins_over_maker(self):
        tick = _parse_tick_payload(
            {"symbol": "BTCUSDT", "E": 1700000000000, "price": "50000", "qty": "0.01", "side": "BUY", "m": True}
        )
        self.assertIsNotNone(tick)
        assert tick is not None
        self.assertEqual(tick["side"], "BUY")
        self.assertEqual(tick["side_conf"], "explicit")
        self.assertEqual(tick["side_raw"], "BUY")
        self.assertTrue(tick["is_buyer_maker"])  # still preserved

    def test_tick_uid_preference(self):
        uid1 = _compute_tick_uid(
            symbol="BTCUSDT",
            trade_id=123,
            ts_ms=0,
            price_src=None,
            qty_src=None,
            side="BUY",
            is_buyer_maker=None,
        )
        self.assertEqual(uid1, "BTCUSDT:123")

        uid2 = _compute_tick_uid(
            symbol="BTCUSDT",
            trade_id=None,
            ts_ms=1700000000000,
            price_src="1",
            qty_src="2",
            side="SELL",
            is_buyer_maker=True,
            stream_id="1700000000123-7",
        )
        self.assertEqual(uid2, "BTCUSDT:mid1700000000123-7")

        uid3 = _compute_tick_uid(
            symbol="BTCUSDT",
            trade_id=None,
            ts_ms=1700000000000,
            price_src="1",
            qty_src="2",
            side="SELL",
            is_buyer_maker=True,
        )
        self.assertTrue(uid3.startswith("BTCUSDT:h"))


if __name__ == "__main__":
    unittest.main()

