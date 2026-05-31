import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from services.orderflow.liquidity_resiliency import LiquidityResiliencyTracker


class TestLiquidityResiliencyV1(unittest.TestCase):
    def _make_tracker(self, **kwargs) -> LiquidityResiliencyTracker:
        defaults = dict(
            ema_alpha=0.5,
            stress_spread_mult=1.5,
            stress_depth_drop_frac=0.5,
            recover_spread_mult=1.1,
            recover_depth_drop_frac=0.2,
            recover_hold_ms=200,
        )
        defaults.update(kwargs)
        return LiquidityResiliencyTracker(**defaults)

    def _establish_baseline(self, tr: LiquidityResiliencyTracker, n: int = 10) -> None:
        """Feed n identical ticks so EMA converges to baseline before stressing."""
        for i in range(n):
            tr.update(ts_ms=1000 + i * 10, spread_bps=1.0, depth_usd=10_000.0)

    def test_no_stress_initially(self) -> None:
        """First tick: no stress (EMA not established yet)."""
        tr = self._make_tracker()
        out = tr.update(ts_ms=1000, spread_bps=1.0, depth_usd=10_000.0)
        self.assertEqual(out["liq_stress_active"], 0)
        self.assertEqual(out["liq_recovery_time_ms"], 0)

    def test_stress_triggered_by_wide_spread(self) -> None:
        """Wide spread (>=1.5x converged baseline) triggers stress.

        After EMA convergence (n=10 ticks of 1.0), spread_ema ≈ 1.0.
        Then a spike to 3.0 gives spread_ratio = 3.0 / (0.5*1.0 + 0.5*3.0) = 3.0/2.0 = 1.5
        which exactly meets stress_spread_mult=1.5 (>=).
        """
        tr = self._make_tracker()
        self._establish_baseline(tr)
        # After convergence: spread_ema ~= 1.0
        # EMA updates to 0.5*1.0 + 0.5*3.0 = 2.0, ratio = 3.0/2.0 = 1.5 >= 1.5 → stress
        out = tr.update(ts_ms=2000, spread_bps=3.0, depth_usd=10_000.0)
        self.assertEqual(out["liq_stress_active"], 1)
        self.assertGreaterEqual(out["liq_recovery_time_ms"], 0)

    def test_stress_and_recover(self) -> None:
        """Full stress-to-recovery cycle works correctly."""
        tr = self._make_tracker()
        self._establish_baseline(tr, n=10)

        # trigger stress with large spike
        # EMA after spike: 0.5*1.0 + 0.5*3.0 = 2.0; ratio = 3.0/2.0 = 1.5 >= 1.5 → stress
        t = 2000
        out = tr.update(ts_ms=t, spread_bps=3.0, depth_usd=10_000.0)
        self.assertEqual(out["liq_stress_active"], 1)
        self.assertGreaterEqual(out["liq_recovery_time_ms"], 0)

        # still stressed at t+200
        t += 200
        out = tr.update(ts_ms=t, spread_bps=3.0, depth_usd=10_000.0)
        self.assertEqual(out["liq_stress_active"], 1)
        self.assertGreaterEqual(out["liq_recovery_time_ms"], 200)

        # recover condition satisfied — EMA drifts down, start recovery candidate
        # Need spread_ratio <= recover_spread_mult=1.1
        # EMA ≈ 2.0 after the spikes; send 1.0 → EMA = 0.5*2.0+0.5*1.0 = 1.5, ratio=1.0/1.5=0.67 <= 1.1 → ok
        t += 10
        out = tr.update(ts_ms=t, spread_bps=1.0, depth_usd=10_000.0)
        self.assertEqual(out["liq_stress_active"], 1)  # still stressed; hold not met yet

        # after hold period (recover_hold_ms=200 elapsed since candidate)
        t += 250
        out = tr.update(ts_ms=t, spread_bps=1.0, depth_usd=10_000.0)
        self.assertEqual(out["liq_stress_active"], 0)
        self.assertEqual(out["liq_recovery_time_ms"], 0)

    def test_stress_triggered_by_depth_drop(self) -> None:
        """Depth drop >50% of converged baseline triggers stress."""
        tr = self._make_tracker()
        self._establish_baseline(tr)
        # After convergence: depth_ema ~= 10_000. Send depth=1_000 (90% drop).
        # EMA = 0.5*10000 + 0.5*1000 = 5500; ratio = 1000/5500 = 0.18 < (1-0.5)=0.5 → stress
        out = tr.update(ts_ms=2000, spread_bps=1.0, depth_usd=1_000.0)
        self.assertEqual(out["liq_stress_active"], 1)

    def test_output_keys_present(self) -> None:
        """Output dict must contain all expected keys."""
        tr = self._make_tracker()
        out = tr.update(ts_ms=1000, spread_bps=1.0, depth_usd=10_000.0)
        expected_keys = {
            "liq_stress_active", "liq_recovery_time_ms", "liq_fragility_score",
            "liq_spread_ema", "liq_depth_ema_usd", "liq_spread_ratio", "liq_depth_ratio",
        }
        for k in expected_keys:
            self.assertIn(k, out, f"Missing key: {k}")

    def test_fragility_score_bounded(self) -> None:
        """Fragility score should always be in [0, 1]."""
        tr = self._make_tracker()
        for spread, depth in [(0.5, 50_000.0), (10.0, 100.0), (1.0, 10_000.0)]:
            out = tr.update(ts_ms=1000, spread_bps=spread, depth_usd=depth)
            self.assertGreaterEqual(out["liq_fragility_score"], 0.0)
            self.assertLessEqual(out["liq_fragility_score"], 1.0)

    def test_recover_candidate_reset_on_relapse(self) -> None:
        """If recovery candidate is set but spread widens again, candidate resets."""
        tr = self._make_tracker()
        self._establish_baseline(tr, n=10)
        # trigger stress
        tr.update(ts_ms=2000, spread_bps=3.0, depth_usd=10_000.0)
        # start recovery candidate (recovery condition met)
        tr.update(ts_ms=2100, spread_bps=1.0, depth_usd=10_000.0)
        # relapse before hold completes
        tr.update(ts_ms=2150, spread_bps=5.0, depth_usd=10_000.0)
        # back to normal — candidate should be reset, so not recovered yet
        out = tr.update(ts_ms=2200, spread_bps=1.0, depth_usd=10_000.0)
        self.assertEqual(out["liq_stress_active"], 1)


if __name__ == "__main__":
    unittest.main()
