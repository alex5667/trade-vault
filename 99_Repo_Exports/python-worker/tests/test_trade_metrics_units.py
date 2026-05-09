
import unittest

# Add parent directory to path to import services
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from services.trade_metrics_service import TradeMetricsService


class TestTradeMetricsUnits(unittest.TestCase):
    def setUp(self):
        self.tm = TradeMetricsService()

    def test_mfe_unit_conversion(self):
        """Test that MFE (Price) is converted to USD using Lot."""
        m = self.tm.new_metrics()
        t = {
            "pnl_gross": "10.0",
            "pnl_net": "9.0",
            "fees": "-1.0",
            "mfe": "1000.0",  # Price
            "lot": "0.01",
            "close_reason": "TP"
        }
        self.tm.accumulate_trade(m, t)

        # MFE USD = 1000 * 0.01 = 10.0
        # Exit Eff = 10.0 / 10.0 = 1.0
        self.assertAlmostEqual(m["sum_exit_eff_win"], 1.0)
        self.assertEqual(m["cnt_exit_eff_win"], 1)

    def test_giveback_unit_conversion(self):
        """Test that Giveback (Price) is converted to USD using Lot."""
        m = self.tm.new_metrics()
        # Case: price went up 2000 (MFE=20), closed at 1000 (PnL=10), gave back 1000 (10 USD)
        t = {
            "pnl_gross": "10.0",
            "pnl_net": "9.0",
            "fees": "-1.0",
            "mfe": "2000.0",   # Price MFE -> 20 USD
            "giveback": "1000.0", # Price Giveback -> 10 USD
            "lot": "0.01",
            "close_reason": "TP"
        }
        self.tm.accumulate_trade(m, t)

        # MFE USD = 20.0
        # Giveback USD = 10.0
        # Ratio = 10/20 = 0.5
        self.assertAlmostEqual(m["sum_giveback_ratio_win"], 0.5)

    def test_missed_profit_unit_conversion(self):
        """Test that Missed Profit (Price) is converted to USD using Lot."""
        m = self.tm.new_metrics()
        # Case: SL after TP. MFE=2000 (20 USD). PnL=0. Missed=2000 (20 USD).
        t = {
            "pnl_gross": "0.0",
            "pnl_net": "-1.0",
            "fees": "-1.0",
            "mfe": "2000.0",       # Price MFE -> 20 USD
            "missed_profit": "2000.0", # Price Missed -> 20 USD
            "lot": "0.01",
            "close_reason": "SL_AFTER_TP" # Trigger specific path
        }
        self.tm.accumulate_trade(m, t)

        # MFE USD = 20.0
        # Missed USD = 20.0
        # Ratio = 1.0
        self.assertAlmostEqual(m["sum_missed_profit_ratio"], 1.0)

    def test_explicit_usd_priority(self):
        """Test that explicit USD fields take precedence over raw fields."""
        m = self.tm.new_metrics()
        t = {
            "pnl_gross": "10.0",
            "mfe": "5000.0",       # Garbage Price
            "mfe_usd": "20.0",     # Correct USD
            "lot": "0.01",
            "close_reason": "TP"
        }
        self.tm.accumulate_trade(m, t)

        # Eff = 10 / 20 = 0.5
        self.assertAlmostEqual(m["sum_exit_eff_win"], 0.5)

if __name__ == "__main__":
    unittest.main()
