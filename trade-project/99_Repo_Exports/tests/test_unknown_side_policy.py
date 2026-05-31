import os
import sys
import types
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    import redis.exceptions  # type: ignore
except Exception:
    redis_mod = types.ModuleType("redis")
    exc_mod = types.ModuleType("redis.exceptions")
    class RedisError(Exception): pass
    exc_mod.RedisError = RedisError
    redis_mod.exceptions = exc_mod
    sys.modules.setdefault("redis", redis_mod)
    sys.modules.setdefault("redis.exceptions", exc_mod)

try:
    import services.pnl_math  # type: ignore
except Exception:
    pnl_mod = types.ModuleType("services.pnl_math")
    def get_symbol_info(*_a, **_k): return {}
    def calculate_position_size(*_a, **_k): return 0.0
    pnl_mod.get_symbol_info = get_symbol_info
    pnl_mod.calculate_position_size = calculate_position_size
    sys.modules.setdefault("services.pnl_math", pnl_mod)

from services.orderflow.utils import _parse_tick_payload
from services.orderflow.side_policy import (
    normalize_unknown_side_policy, is_unknown_side_tick, deterministic_sample
)


class TestUnknownSidePolicy(unittest.TestCase):
    def test_normalize_policy(self):
        self.assertEqual(normalize_unknown_side_policy(None), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("keep"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("drop"), "drop")
        self.assertEqual(normalize_unknown_side_policy("quarantine"), "quarantine")
        self.assertEqual(normalize_unknown_side_policy("lol"), "ignore_delta")

    def test_is_unknown_side_tick_true(self):
        tick = _parse_tick_payload({"symbol":"BTCUSDT","E":1700000000000,"price":"1","qty":"2"})
        assert tick is not None
        self.assertTrue(is_unknown_side_tick(tick))

    def test_is_unknown_side_tick_false_explicit(self):
        tick = _parse_tick_payload({"symbol":"BTCUSDT","E":1700000000000,"price":"1","qty":"2","side":"SELL"})
        assert tick is not None
        self.assertFalse(is_unknown_side_tick(tick))

    def test_is_unknown_side_tick_false_maker(self):
        tick = _parse_tick_payload({"symbol":"BTCUSDT","E":1700000000000,"price":"1","qty":"2","m":True})
        assert tick is not None
        self.assertFalse(is_unknown_side_tick(tick))

    def test_deterministic_sample(self):
        self.assertFalse(deterministic_sample(123, 0.0))
        self.assertTrue(deterministic_sample(123, 1.0))
        a = deterministic_sample(1234567890, 0.05)
        b = deterministic_sample(1234567890, 0.05)
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()

