import unittest

# Add the project root to sys.path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.trade_metrics_service import TradeMetricsService


class TestMetricAccumulation(unittest.TestCase):
    def setUp(self):
        self.tm = TradeMetricsService()
        self.m = self.tm.new_metrics()

    def test_atr_accumulation_with_explicit_fields(self):
        # Mock trade with explicit sl_atr and tp_atr
        t = {
            "atr": 10.0,
            "entry_price": 100.0,
            "sl_price": 90.0,
            "tp1_price": 120.0,
            "sl_atr": 1.0,
            "tp_atr": 2.0,
            "pnl_net": 200.0,
            "one_r_money": 100.0,
            "r_multiple": 2.0
        }

        self.tm.accumulate_trade(self.m, t)
        self.tm.finalize(self.m)

        self.assertEqual(self.m["cnt_sl_atr"], 1)
        self.assertEqual(self.m["cnt_tp_atr"], 1)
        self.assertAlmostEqual(self.m["avg_sl_atr"], 1.0)
        self.assertAlmostEqual(self.m["avg_tp_atr"], 2.0)

    def test_atr_accumulation_with_prices(self):
        # Mock trade without explicit sl_atr/tp_atr but with prices and ATR.
        # Must use the labeled key — generic `atr` is rejected to avoid TF mismatch
        # (e.g. 1m feature-time ATR vs 15m level-time ATR → 20-40 ATR readings).
        t = {
            "atr_used_for_levels": 10.0,
            "entry_price": 100.0,
            "sl_price": 90.0,
            "tp1_price": 120.0,
            "pnl_net": 200.0,
            "one_r_money": 100.0,
            "r_multiple": 2.0
        }

        self.tm.accumulate_trade(self.m, t)
        self.tm.finalize(self.m)

        # SL_ATR = |100 - 90| / 10 = 1.0
        # TP_ATR = |120 - 100| / 10 = 2.0
        self.assertEqual(self.m["cnt_sl_atr"], 1)
        self.assertEqual(self.m["cnt_tp_atr"], 1)
        self.assertAlmostEqual(self.m["avg_sl_atr"], 1.0)
        self.assertAlmostEqual(self.m["avg_tp_atr"], 2.0)

    def test_r_multiple_accumulation(self):
        t1 = {"pnl_net": 100.0, "one_r_money": 50.0, "r_multiple": 2.0, "notional_usd": 1000.0}
        t2 = {"pnl_net": -50.0, "one_r_money": 50.0, "r_multiple": -1.0, "notional_usd": 1000.0}

        self.tm.accumulate_trade(self.m, t1)
        self.tm.accumulate_trade(self.m, t2)
        self.tm.finalize(self.m)

        self.assertEqual(self.m["cnt_r"], 2)
        self.assertAlmostEqual(self.m["expectancy_r"], 0.5) # (2 - 1) / 2
        self.assertAlmostEqual(self.m["median_r"], 0.5)

if __name__ == "__main__":
    unittest.main()
