
import unittest

from services.position_sizing import calculate_qty_fixed_risk, round_price_conservative


class TestPositionSizing(unittest.TestCase):
    def test_calculate_qty_fixed_risk(self):
        # Linear: risk 10, sl_dist 2 => qty 5
        res = calculate_qty_fixed_risk(10.0, 2.0, 100.0, 0.001, 0.001, 1000.0)
        self.assertTrue(res.ok)
        self.assertAlmostEqual(res.qty, 5.0)
        self.assertAlmostEqual(res.risk_usd, 10.0)
        self.assertAlmostEqual(res.notional, 500.0)

    def test_step_rounding(self):
        # risk 10, sl_dist 2.001 => raw_qty 4.9975
        # step 0.1 => floor(49.975) = 49 * 0.1 = 4.9
        res = calculate_qty_fixed_risk(10.0, 2.001, 100.0, 0.1, 0.001, 1000.0)
        self.assertTrue(res.ok)
        self.assertAlmostEqual(res.qty, 4.9)
        self.assertTrue(res.risk_usd < 10.0) # 4.9 * 2.001 = 9.8049

    def test_min_notional(self):
        # risk 1, sl_dist 10 => qty 0.1
        # entry 10 => notional 1.0. Min 5.0 => bump to 0.5 (notional 5.0)
        res = calculate_qty_fixed_risk(1.0, 10.0, 10.0, 0.001, 0.001, 1000.0, min_notional=5.0)
        self.assertTrue(res.ok)
        self.assertEqual(res.qty, 0.5)
        self.assertEqual(res.reason, "min_notional_bumps_risk")

    def test_round_price_conservative(self):
        tick = 0.5
        # LONG TP (Above): 100.9 -> 100.5 (Floor). 100.4 -> 100.0.
        self.assertEqual(round_price_conservative(100.9, tick, 1, is_tp=True), 100.5)
        self.assertEqual(round_price_conservative(100.4, tick, 1, is_tp=True), 100.0)

        # LONG SL (Below): 99.1 -> 99.5 (Ceil/Tighter). 99.6 -> 100.0 (Ceil).
        self.assertEqual(round_price_conservative(99.1, tick, 1, is_tp=False), 99.5)

        # SHORT TP (Below): 99.1 -> 99.5 (Ceil/Closer).
        self.assertEqual(round_price_conservative(99.1, tick, -1, is_tp=True), 99.5)

        self.assertEqual(round_price_conservative(100.9, tick, -1, is_tp=False), 100.5)

    def test_percent_risk_fallback(self):
        # Env mock
        import os
        from unittest.mock import MagicMock, patch

        from common.balance_provider import _GLOBAL_CACHE
        from services.position_sizing import apply_position_sizing_to_ctx
        _GLOBAL_CACHE.invalidate()   # ensure cache is clean

        ctx = MagicMock()
        ctx.stop_dist = 2.0
        ctx.entry_price = 100.0
        ctx.specs = MagicMock()
        ctx.specs.lot_step = 0.01
        ctx.specs.min_lot = 0.01
        ctx.specs.max_lot = 100.0

        # RISK_USD_PER_TRADE=0, DEPOSIT=100, RISK_PCT=5 => risk 5.0
        # qty = 5.0 / 2.0 = 2.5
        with patch.dict(os.environ, {
            "RISK_USD_PER_TRADE": "0",
            "ACCOUNT_DEPOSIT_USD": "100",
            "RISK_PERCENT": "5.0",
            "RISK_MIN_NOTIONAL_USD": "5",
            "BALANCE_PROVIDER_MODE": "static",   # use static to avoid Redis/REST in unit test
        }):
            # Mock append_dq_flag to avoid import errors or side effects
            with patch("common.dq_flags.append_dq_flag") as mock_dq:
                cfg = {"TP_MODE": "RR"}
                apply_position_sizing_to_ctx(ctx, cfg, "BTCUSDT")

                self.assertEqual(ctx.risk_usd, 5.0)
                self.assertEqual(ctx.qty, 2.5)
                self.assertTrue(ctx.sizing_ok)

    def test_failure_dq(self):
        import os
        from unittest.mock import MagicMock, patch

        from services.position_sizing import apply_position_sizing_to_ctx

        ctx = MagicMock()
        ctx.stop_dist = 0.0 # Invalid
        ctx.entry_price = 100.0

        with patch.dict(os.environ, {"RISK_USD_PER_TRADE": "10"}), \
             patch("common.dq_flags.append_dq_flag") as mock_dq:

             apply_position_sizing_to_ctx(ctx, {"TP_MODE": "RR"}, "BTCUSDT")
             mock_dq.assert_called_with(ctx, "sizing_no_levels")

if __name__ == "__main__":
    unittest.main()
