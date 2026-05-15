
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

    def test_giveback_usd_semantics(self):
        """Giveback field is stored as USD in Redis (closed.giveback = pos.mfe_pnl - pnl_gross).
        Reader must NOT multiply by lot again — that was a double-multiplication bug
        that clamped reported giveback_ratio at 1.5 for any lot != 1.
        """
        m = self.tm.new_metrics()
        # MFE USD = 20, PnL gross = 10, gave back 10 USD. Lot = 0.01 (irrelevant for USD fields).
        t = {
            "pnl_gross": "10.0",
            "pnl_net": "9.0",
            "fees": "-1.0",
            "mfe_pnl": "20.0",   # USD
            "giveback": "10.0",  # USD (NOT price-delta)
            "lot": "0.01",
            "close_reason": "TP"
        }
        self.tm.accumulate_trade(m, t)
        # Ratio = 10/20 = 0.5 (NOT 0.005 from over-multiplication, NOT 1.5 clamp)
        self.assertAlmostEqual(m["sum_giveback_ratio_win"], 0.5)

    def test_missed_profit_usd_semantics(self):
        """Missed_profit is stored as USD; reader must not multiply by lot."""
        m = self.tm.new_metrics()
        t = {
            "pnl_gross": "0.0",
            "pnl_net": "-1.0",
            "fees": "-1.0",
            "mfe_pnl": "20.0",        # USD
            "missed_profit": "20.0",  # USD (NOT price-delta)
            "lot": "0.01",
            "close_reason": "SL_AFTER_TP"
        }
        self.tm.accumulate_trade(m, t)
        # Ratio = 20/20 = 1.0
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
