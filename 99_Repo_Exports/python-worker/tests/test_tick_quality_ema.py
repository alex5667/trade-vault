import unittest

from services.orderflow.tick_quality_ema import TickQualityEMA


class TestTickQualityEMA(unittest.TestCase):
    def test_ema_updates(self):
        q = TickQualityEMA(tau_ms=1000)
        out1 = q.update(
            symbol="BTCUSDT",
            ts_ms=0,
            unknown_side=1.0,
            ts_source="now",
            abs_skew_ms=100.0,
            abs_age_ms=50.0,
        )
        self.assertAlmostEqual(out1["unknown"], 1.0, places=6)
        out2 = q.update(
            symbol="BTCUSDT",
            ts_ms=1000,
            unknown_side=0.0,
            ts_source="payload",
            abs_skew_ms=0.0,
            abs_age_ms=0.0,
        )
        # After one tau, value should decay significantly (~e^-1)
        self.assertTrue(0.20 < out2["unknown"] < 0.60)
        self.assertTrue(0.0 <= out2["ts_now"] <= 1.0)


if __name__ == "__main__":
    unittest.main()
