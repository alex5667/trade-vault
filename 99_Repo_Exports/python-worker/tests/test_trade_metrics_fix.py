
import unittest

from services.trade_metrics_service import TradeMetricsService


class TestTradeMetricsFix(unittest.TestCase):
    def test_accumulate_with_missing_pnl_gross(self):
        tm = TradeMetricsService()
        m = tm.new_metrics()

        # Scenario:
        # PnL Net = 0.01 (small profit)
        # Fees = 0.20 (huge relative to net)
        # PnL Gross should be 0.21
        # MFE = 0.40
        # Expected ExitEff = 0.21 / 0.40 = 0.525
        # Expected Giveback = 0.40 - 0.21 = 0.19
        # Expected Giveback Ratio = 0.19 / 0.40 = 0.475

        t = {
            "pnl_net": "0.01",
            "fees": "0.20",
            # pnl_gross missing
            "mfe_pnl": "0.40",
            # giveback missing
            "close_reason": "TP1",
            "entry_ts_ms": "1600000000000",
            "exit_ts_ms": "1600000001000",
        }

        tm.accumulate_trade(m, t)
        tm.finalize(m)

        print(f"Stats: ExitEff={m['exit_eff_avg_win']}, GivebackRatio={m['giveback_ratio_avg_win']}")

        # Verify pnl_gross was calculated correctly
        self.assertAlmostEqual(m['total_pnl_gross'], 0.21)

        # Verify ExitEff
        # exit_eff = 0.21 / 0.40 = 0.525
        self.assertAlmostEqual(m['sum_exit_eff_win'], 0.525)

        # Verify Giveback (recalculated)
        # giveback = 0.40 - 0.21 = 0.19
        # ratio = 0.19 / 0.40 = 0.475
        self.assertAlmostEqual(m['sum_giveback_ratio_win'], 0.475)
    def test_trail_sl_missed_profit(self):
        tm = TradeMetricsService()
        m = tm.new_metrics()

        # Scenario: TRAIL_SL trade
        # Entry=100, Peak=120 (MFE=20), Exit=115 (Net=15)
        # Missed = 5
        # Missed Ratio = 5/20 = 0.25
        t = {
            "pnl_net": "15.0",
            "pnl_gross": "15.1", # fees 0.1
            "fees": "0.1",
            "mfe_pnl": "20.0",
            "missed_profit": "5.0",
            "bucket_close_reason": "TRAIL_SL",
            "close_reason_raw": "TRAILING_STOP",
            "entry_ts_ms": "1600000000000",
            "exit_ts_ms": "1600000001000",
        }

        tm.accumulate_trade(m, t)
        tm.finalize(m)

        # Verify Missed Profit Ratio
        # Should be accumulated because bucket is TRAIL_SL
        self.assertAlmostEqual(m['missed_profit_ratio_avg'], 0.25)

        # Verify ExitEff
        # ExitEff = 15.1 / 20.0 = 0.755
        self.assertAlmostEqual(m['exit_eff_avg_win'], 0.755)

if __name__ == '__main__':
    unittest.main()
