import unittest

from services.signal_preprocess import preprocess_signal_for_publish
from utils.time_utils import get_ny_time_millis


class TestSignalPreprocess(unittest.TestCase):
    def test_adds_required_fields_and_flags(self):
        sig = {
            "symbol": "btcusdt",
            "confidence": 87.0,  # percent-like
            "ts_ms": get_ny_time_millis(),  # Valid timestamp to avoid bad_ts flag
            "micro": {
                "spread_bps": 15.0,  # > 12.0 new default threshold
                "book_stale_ms": 5000,  # > 1500 new default threshold
            },
            "indicators": {
                "touch_is_stale": True,
                "tick_oood": True,
            },
        }
        preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

        self.assertEqual(sig["symbol"], "BTCUSDT")
        self.assertTrue(int(sig["ts_ms"]) > 0)
        self.assertIn("data_quality_flags", sig)
        flags = set(sig["data_quality_flags"])
        # wide_spread: spread_bps=15.0 >= _DQ_SPREAD_WIDE_FLAG_BPS (12.0)
        self.assertIn("wide_spread", flags, f"Expected 'wide_spread' in flags: {sorted(flags)}")
        # stale_l2: book_stale_ms=5000 >= _DQ_BOOK_STALE_FLAG_MS (1500)
        self.assertIn("stale_l2", flags, f"Expected 'stale_l2' in flags: {sorted(flags)}")
        # tick_oood: tick_oood=True
        self.assertIn("tick_oood", flags, f"Expected 'tick_oood' in flags: {sorted(flags)}")
        self.assertAlmostEqual(sig["confidence01"], 0.87, places=6)

    def test_fail_open(self):
        sig = {"symbol": "ETHUSDT", "confidence": "nan", "indicators": "not-a-dict"}
        preprocess_signal_for_publish(sig, symbol="ETHUSDT", source="CryptoOrderFlow", logger=None)
        self.assertEqual(sig["symbol"], "ETHUSDT")
        self.assertIn("ts_ms", sig)


if __name__ == "__main__":
    unittest.main()

