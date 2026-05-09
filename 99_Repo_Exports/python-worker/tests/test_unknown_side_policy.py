import os
import sys
import unittest

# Ensure project root is importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from services.orderflow.side_policy import (
    deterministic_sample,
    is_unknown_side_tick,
    normalize_unknown_side_policy,
)
from services.orderflow.utils import _parse_tick_payload


class TestUnknownSidePolicy(unittest.TestCase):
    def test_normalize_policy(self):
        self.assertEqual(normalize_unknown_side_policy(None), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy(""), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("ignore"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("DROP"), "drop")
        self.assertEqual(normalize_unknown_side_policy("Quarantine"), "quarantine")
        self.assertEqual(normalize_unknown_side_policy("invalid"), "ignore_delta")

    def test_is_unknown_side_tick_explicit(self):
        t = _parse_tick_payload({"symbol": "BTCUSDT", "E": 1700000000000, "price": "50000", "qty": "0.01", "side": "BUY"})
        self.assertIsNotNone(t)
        assert t is not None
        self.assertFalse(is_unknown_side_tick(t))

    def test_is_unknown_side_tick_maker_inferred(self):
        t = _parse_tick_payload({"symbol": "BTCUSDT", "E": 1700000000000, "price": "50000", "qty": "0.01", "m": True})
        self.assertIsNotNone(t)
        assert t is not None
        self.assertEqual(t["side"], "SELL")
        self.assertEqual(t["side_conf"], "maker")
        self.assertFalse(is_unknown_side_tick(t))

    def test_is_unknown_side_tick_missing(self):
        t = _parse_tick_payload({"symbol": "BTCUSDT", "E": 1700000000000, "price": "50000", "qty": "0.01"})
        self.assertIsNotNone(t)
        assert t is not None
        self.assertEqual(t["side"], "UNKNOWN")
        self.assertEqual(t["side_conf"], "unknown")
        self.assertTrue(is_unknown_side_tick(t))

    def test_deterministic_sample(self):
        # k = abs(key_ms) % 10000
        # sample if k < rate*10000
        key_ms = 123456
        k = abs(key_ms) % 10000
        self.assertEqual(deterministic_sample(key_ms, 0.01), k < 100)
        self.assertTrue(deterministic_sample(key_ms, 1.0))
        self.assertFalse(deterministic_sample(key_ms, 0.0))


if __name__ == "__main__":
    unittest.main()

