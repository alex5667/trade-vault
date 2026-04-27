import os
import sys
import unittest

# Ensure /app (or repo root) is on sys.path; tests live under python-worker/tests
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from services.orderflow.side_sign import side_sign_from_tick, signed_qty


class TestSideSign(unittest.TestCase):
    def test_maker_preferred(self):
        # buyer is maker => SELL aggressor
        sign, reason = side_sign_from_tick({"is_buyer_maker": True, "side": "BUY"})
        self.assertEqual(sign, -1)
        self.assertEqual(reason, "maker_sell")

        sign, reason = side_sign_from_tick({"is_buyer_maker": False, "side": "SELL"})
        self.assertEqual(sign, 1)
        self.assertEqual(reason, "maker_buy")

    def test_explicit_side(self):
        sign, reason = side_sign_from_tick({"side": "BUY"})
        self.assertEqual(sign, 1)
        self.assertEqual(reason, "side_buy")

        sign, reason = side_sign_from_tick({"side": "SELL"})
        self.assertEqual(sign, -1)
        self.assertEqual(reason, "side_sell")

    def test_unknown(self):
        sign, reason = side_sign_from_tick({"side": "UNKNOWN"})
        self.assertEqual(sign, 0)
        self.assertEqual(reason, "unknown")

        sign, reason = side_sign_from_tick({})
        self.assertEqual(sign, 0)
        self.assertEqual(reason, "unknown")

        sign, _ = side_sign_from_tick({"side": ""})
        self.assertEqual(sign, 0)

        sign, _ = side_sign_from_tick({"side": None})
        self.assertEqual(sign, 0)

    def test_signed_qty(self):
        self.assertEqual(signed_qty(2.5, 1), 2.5)
        self.assertEqual(signed_qty(2.5, -1), -2.5)
        self.assertEqual(signed_qty(2.5, 0), 0.0)
        self.assertEqual(signed_qty(2.5, 7), 0.0)


if __name__ == "__main__":
    unittest.main()

