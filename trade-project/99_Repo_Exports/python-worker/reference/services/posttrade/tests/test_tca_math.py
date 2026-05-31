import unittest


class TestTCAMath(unittest.TestCase):
    def test_effective_spread_buy(self):
        from services.posttrade.tca_math import effective_spread_bps

        # mid=100, trade_px=100.01 => buy eff spread = 2*(0.01)/100*1e4 = 2 bps
        v = effective_spread_bps(trade_px=100.01, mid_t=100.0, side="LONG")
        self.assertAlmostEqual(v, 2.0, places=6)

    def test_effective_spread_sell(self):
        from services.posttrade.tca_math import effective_spread_bps

        # sell at 99.99 vs mid=100 => 2*(100-99.99)/100*1e4 = 2 bps
        v = effective_spread_bps(trade_px=99.99, mid_t=100.0, side="SHORT")
        self.assertAlmostEqual(v, 2.0, places=6)

    def test_realized_spread_and_impact(self):
        from services.posttrade.tca_math import permanent_impact_bps, realized_spread_bps

        # Buy at px 100.01, mid_t=100, mid_t+Δ=100.02
        # realized = 2*(100.01-100.02)/100*1e4 = -2 bps
        rs = realized_spread_bps(trade_px=100.01, mid_t=100.0, mid_t_delta=100.02, side="BUY")
        self.assertAlmostEqual(rs, -2.0, places=6)

        # impact = (100.02-100.0)/100*1e4 = 2 bps
        imp = permanent_impact_bps(mid_t=100.0, mid_t_delta=100.02, side="BUY")
        self.assertAlmostEqual(imp, 2.0, places=6)

    def test_implementation_shortfall(self):
        from services.posttrade.tca_math import implementation_shortfall_bps

        # Buy: decision_mid=100, fill=100.03 => 3 bps + fee 0.5 => 3.5
        is_bps = implementation_shortfall_bps(vwap_fill_px=100.03, decision_mid=100.0, side="LONG", fee_bps=0.5)
        self.assertAlmostEqual(is_bps, 3.5, places=6)

    def test_none_on_zero_mid(self):
        """None returned when mid_t is 0 (division guard)."""
        from services.posttrade.tca_math import effective_spread_bps

        v = effective_spread_bps(trade_px=100.0, mid_t=0.0, side="LONG")
        self.assertIsNone(v)

    def test_none_unknown_side(self):
        """None returned when side is unknown."""
        from services.posttrade.tca_math import effective_spread_bps

        v = effective_spread_bps(trade_px=100.0, mid_t=100.0, side="UNKNOWN")
        self.assertIsNone(v)


if __name__ == "__main__":
    unittest.main()
