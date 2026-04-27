import sys
from pathlib import Path
import unittest

PYWORKER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYWORKER))

from services.orderflow.liquidity_resiliency import LiquidityResiliencyTracker


class TestLiquidityResiliency(unittest.TestCase):
    def test_stress_and_recovery(self):
        tr = LiquidityResiliencyTracker(
            ema_alpha=0.5,
            stress_spread_mult=1.5,
            stress_depth_drop_frac=0.5,
            recover_spread_mult=1.1,
            recover_depth_drop_frac=0.2,
            recover_hold_ms=100,
        )

        # baseline
        out = tr.update(ts_ms=1000, spread_bps=10.0, depth_usd=10000.0)
        self.assertEqual(out['liq_stress_active'], 0)

        # enter stress
        out = tr.update(ts_ms=1100, spread_bps=30.0, depth_usd=10000.0)
        self.assertEqual(out['liq_stress_active'], 1)

        # still stressed => timer grows
        out = tr.update(ts_ms=1150, spread_bps=30.0, depth_usd=10000.0)
        self.assertGreater(out['liq_recovery_time_ms'], 0)

        # recover (hold)
        out = tr.update(ts_ms=1300, spread_bps=10.0, depth_usd=10000.0)
        out = tr.update(ts_ms=1500, spread_bps=10.0, depth_usd=10000.0)
        self.assertEqual(out['liq_stress_active'], 0)

    def test_fail_open_bad_ts(self):
        """Bad ts_ms => returns safe defaults without crashing."""
        tr = LiquidityResiliencyTracker()
        out = tr.update(ts_ms=0, spread_bps=10.0, depth_usd=5000.0)
        self.assertIn('liq_recovery_time_ms', out)
        self.assertEqual(out['liq_recovery_time_ms'], 0)

    def test_fragility_score_range(self):
        """Fragility score must be in [0, 1]."""
        tr = LiquidityResiliencyTracker(ema_alpha=0.5)
        tr.update(ts_ms=1000, spread_bps=10.0, depth_usd=10000.0)
        for spread, depth in [(50.0, 1000.0), (5.0, 50000.0), (10.0, 10000.0)]:
            out = tr.update(ts_ms=2000, spread_bps=spread, depth_usd=depth)
            self.assertGreaterEqual(out['liq_fragility_score'], 0.0)
            self.assertLessEqual(out['liq_fragility_score'], 1.0)

    def test_depth_drop_triggers_stress(self):
        """A sudden depth drop should trigger stress even if spread is normal.

        With ema_alpha=0.5, after 1 warmup tick depth_ema=10000.
        On stress tick: depth_ema updates to (0.5*10000 + 0.5*500)=5250,
        depth_ratio = 500/5250 ≈ 0.095 which is < (1 - 0.5) = 0.5 => stress.
        """
        tr = LiquidityResiliencyTracker(
            ema_alpha=0.5,
            stress_spread_mult=2.0,
            stress_depth_drop_frac=0.5,
            recover_spread_mult=1.1,
            recover_depth_drop_frac=0.2,
            recover_hold_ms=50,
        )
        # establish baseline
        tr.update(ts_ms=1000, spread_bps=10.0, depth_usd=10000.0)
        # depth drops ~95% => well below stress_depth_drop_frac=0.5 threshold
        out = tr.update(ts_ms=1100, spread_bps=10.0, depth_usd=500.0)
        self.assertEqual(out['liq_stress_active'], 1)


if __name__ == '__main__':
    unittest.main()
