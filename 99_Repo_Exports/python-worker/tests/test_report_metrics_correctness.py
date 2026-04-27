
import unittest
from services.trade_metrics_service import TradeMetricsService

class TestReportMetricsCorrectness(unittest.TestCase):
    def setUp(self):
        self.tm = TradeMetricsService()

    def test_metrics_calculation_simple_scenario(self):
        """
        Scenario:
        Trade 1: Win, +$20, Risk $10 (2R)
        Trade 2: Win, +$10, Risk $10 (1R)
        Trade 3: Loss, -$10, Risk $10 (-1R)
        
        Expected:
        - Total Trades: 3
        - Wins: 2
        - Losses: 1
        - Win Rate: 2/3 (~0.666)
        - Total PnL: $20
        - Gross Profit: $30
        - Gross Loss: $10
        - Profit Factor (Net): 30 / 10 = 3.0
        
        - R Stats:
            - Avg Win R: (2.0 + 1.0) / 2 = 1.5
            - Avg Loss R: -1.0
            - Payoff R: 1.5 / 1.0 = 1.5
        
        - Kelly (Standard formula: K = W - (1-W)/B):
            - W = 2/3
            - B = 1.5
            - K = (2/3) - (1/3)/1.5 = 2/3 - 1/4.5 = 0.666 - 0.222 = 0.444
        """
        
        # Mocking minimal trade dicts needed for accumulators
        trades = [
            {
                "pnl_net": 20.0, "pnl_gross": 21.0, "fees": 1.0, 
                "risk_amount": 10.0, "entry_ts_ms": 1000, "exit_ts_ms": 2000
            },
            {
                "pnl_net": 10.0, "pnl_gross": 11.0, "fees": 1.0, 
                "risk_amount": 10.0, "entry_ts_ms": 3000, "exit_ts_ms": 4000
            },
            {
                "pnl_net": -10.0, "pnl_gross": -9.0, "fees": 1.0, 
                "risk_amount": 10.0, "entry_ts_ms": 5000, "exit_ts_ms": 6000
            }
        ]
        
        m = self.tm.new_metrics()
        for t in trades:
            self.tm.accumulate_trade(m, t)
        
        self.tm.finalize(m)
        
        # 1. Basic Counts
        self.assertEqual(m["total_trades"], 3)
        self.assertEqual(m["wins"], 2)
        self.assertEqual(m["losses"], 1)
        
        # 2. PnL Sums
        self.assertAlmostEqual(m["total_pnl"], 20.0)
        self.assertAlmostEqual(m["gross_profit"], 21.0 + 11.0) # 32.0 (Gross is pnl_gross sums?)
        # Let's check logic: if pnl_gross > eps: gross_profit += pnl_gross.
        # Trade 1: 21, Trade 2: 11. Sum = 32.
        self.assertAlmostEqual(m["gross_profit"], 32.0)
        
        # Loss: -9.0. gross_loss += abs(-9.0) = 9.0
        self.assertAlmostEqual(m["gross_loss"], 9.0)
        
        # 3. Profit Factor Net
        # Sum Win Net: 20 + 10 = 30
        # Sum Loss Net: -10
        # PF = 30 / 10 = 3.0
        self.assertAlmostEqual(m["profit_factor_net"], 3.0)
        
        # 4. Payoff Net
        # Avg Win Net: 30 / 2 = 15
        # Avg Loss Net: -10 / 1 = -10
        # Payoff: 15 / 10 = 1.5
        self.assertAlmostEqual(m["payoff_net"], 1.5)
        
        # 5. R Stats
        self.assertAlmostEqual(m["sum_r"], 2.0 + 1.0 - 1.0) # 2.0
        self.assertAlmostEqual(m["expectancy_r"], 2.0 / 3) # 0.666
        
        # Payoff R
        # Avg Win R: 1.5
        # Avg Loss R: -1.0
        # Payoff R: 1.5
        self.assertAlmostEqual(m["payoff_r"], 1.5)
        
        # 6. Kelly
        # W = 2/3
        # B = 1.5
        # K = 2/3 - (1/3)/1.5 = 2/3 - 2/9 = 6/9 - 2/9 = 4/9 ~= 0.4444...
        self.assertAlmostEqual(m["kelly_f_r"], 4/9, places=4)

    def test_zero_risk_trades_excluded_from_r_metrics(self):
        """
        BUG FIX verification: when trades have no risk data (one_r_money=0),
        they must be excluded from R-based statistics.
        
        Previously, a $1.0 floor was applied, making R = PnL/1.0 = PnL,
        which produced absurd Median R values (e.g. -271R for a $271 loss).
        
        After fix:
        - count_missing_risk should equal trade count
        - cnt_r should be 0
        - R-based metrics (expectancy_r, median_r, etc.) should be 0
        """
        # All trades have 0 risk (typical for virtual trades)
        trades = [
            {"pnl_net": 20.0, "one_r_money": 0.0},
            {"pnl_net": -15.0, "one_r_money": 0.0},
            {"pnl_net": -271.0, "risk_amount": 0.0},
        ]
        
        m = self.tm.new_metrics()
        for t in trades:
            self.tm.accumulate_trade(m, t)
        self.tm.finalize(m)
        
        # PnL-based metrics still work
        self.assertEqual(m["total_trades"], 3)
        self.assertAlmostEqual(m["total_pnl"], 20.0 - 15.0 - 271.0)
        
        # R metrics are zeroed out (no risk data)
        self.assertEqual(m["cnt_r"], 0)
        self.assertEqual(m["count_missing_risk"], 3)
        self.assertAlmostEqual(m["expectancy_r"], 0.0)
        self.assertAlmostEqual(m.get("median_r", 0.0), 0.0)
        self.assertAlmostEqual(m.get("std_r", 0.0), 0.0)
        self.assertAlmostEqual(m.get("payoff_r", 0.0), 0.0)
        self.assertAlmostEqual(m.get("kelly_f_r", 0.0), 0.0)

    def test_mixed_risk_and_missing_risk_trades(self):
        """
        Mixed scenario: some trades have risk data, others don't.
        Only trades WITH risk data should contribute to R-metrics.
        """
        trades = [
            # Trade with real risk: +$20 on $10 risk = +2.0R
            {"pnl_net": 20.0, "one_r_money": 10.0},
            # Trade with real risk: -$10 on $10 risk = -1.0R
            {"pnl_net": -10.0, "one_r_money": 10.0},
            # Virtual trade (no risk): -$271, should be EXCLUDED from R
            {"pnl_net": -271.0, "one_r_money": 0.0},
            # Another virtual trade: +$50, should be EXCLUDED from R
            {"pnl_net": 50.0, "risk_amount": 0.0},
        ]
        
        m = self.tm.new_metrics()
        for t in trades:
            self.tm.accumulate_trade(m, t)
        self.tm.finalize(m)
        
        # All 4 trades counted in PnL
        self.assertEqual(m["total_trades"], 4)
        self.assertAlmostEqual(m["total_pnl"], 20.0 - 10.0 - 271.0 + 50.0)
        
        # Only 2 trades with real risk in R-metrics
        self.assertEqual(m["cnt_r"], 2)
        self.assertEqual(m["count_missing_risk"], 2)
        
        # R values: [2.0, -1.0]
        # Expectancy R = (2.0 - 1.0) / 2 = 0.5
        self.assertAlmostEqual(m["expectancy_r"], 0.5)
        
        # Median R of [2.0, -1.0] = (2.0 + -1.0) / 2 = 0.5
        self.assertAlmostEqual(m["median_r"], 0.5)

    def test_dust_risk_treated_as_missing(self):
        """
        Risk values below $1.0 (MIN_RISK_USD) should also be treated as
        missing — they indicate broken risk calculation, not actual risk.
        """
        trades = [
            {"pnl_net": -50.0, "one_r_money": 0.001},  # dust
            {"pnl_net": 30.0, "one_r_money": 0.5},       # below floor
            {"pnl_net": -20.0, "one_r_money": 5.0},      # real risk
        ]
        
        m = self.tm.new_metrics()
        for t in trades:
            self.tm.accumulate_trade(m, t)
        self.tm.finalize(m)
        
        # Only 1 trade with real risk (one_r_money=$5)
        self.assertEqual(m["cnt_r"], 1)
        self.assertEqual(m["count_missing_risk"], 2)
        
        # R = -20 / 5 = -4.0
        self.assertAlmostEqual(m["expectancy_r"], -4.0)

    def test_metrics_huge_pnl_handling(self):
        """
        Verify that huge numbers do not break calculations (precision issues etc),
        although the fix in pnl_math prevents them from occurring naturally.
        """
        trades = [
            {"pnl_net": 1e9, "risk_amount": 1e6}, # 1000R
            {"pnl_net": -1e6, "risk_amount": 1e6}, # -1R
        ]
        m = self.tm.new_metrics()
        for t in trades:
            self.tm.accumulate_trade(m, t)
        self.tm.finalize(m)
        
        self.assertAlmostEqual(m["total_pnl"], 1e9 - 1e6)
        self.assertEqual(m["wins"], 1)
        self.assertEqual(m["losses"], 1)
        
        # PF: 1e9 / 1e6 = 1000
        self.assertAlmostEqual(m["profit_factor_net"], 1000.0)

if __name__ == "__main__":
    unittest.main()
