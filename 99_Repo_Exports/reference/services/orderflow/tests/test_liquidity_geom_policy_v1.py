import unittest
import sys
import os


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from services.orderflow.liquidity_geom_policy import evaluate_liq_geom, slope_min


class TestSlopeMin(unittest.TestCase):
    def test_min_of_two_positives(self) -> None:
        self.assertEqual(slope_min(100.0, 200.0), 100.0)
        self.assertEqual(slope_min(200.0, 100.0), 100.0)

    def test_zero_treated_as_missing(self) -> None:
        """Zero on one side means 'unknown', should not bias min."""
        self.assertEqual(slope_min(0.0, 200.0), 200.0)
        self.assertEqual(slope_min(100.0, 0.0), 100.0)

    def test_both_zero(self) -> None:
        self.assertEqual(slope_min(0.0, 0.0), 0.0)


class TestLiquidityGeomPolicyV1(unittest.TestCase):
    """Common setup: slope breaches all three thresholds."""

    def _base_kwargs(self) -> dict:
        return dict(
            slope_bid=100.0,  # below thr_slope=500
            slope_ask=100.0,
            dws_bps=10.0,     # above thr_dws=5.0
            recovery_ms=5000,  # above thr_recovery_ms=1000
            thr_slope=500.0,
            thr_dws=5.0,
            thr_recovery_ms=1000,
            tighten_cap_bps=10.0,
            tighten_mult=1.0,
        )

    # --- profile=default ---

    def test_default_only_annotate(self) -> None:
        """default profile: flags raised, but no tighten and no veto."""
        d = evaluate_liq_geom(profile="default", **self._base_kwargs())
        self.assertTrue(d.flags, "Expected some flags")
        self.assertEqual(d.tighten_add_bps, 0.0)
        self.assertFalse(d.veto)
        self.assertEqual(d.veto_reason, "")

    # --- profile=soft ---

    def test_soft_only_annotate(self) -> None:
        """soft profile: same as default — annotate only."""
        d = evaluate_liq_geom(profile="soft", **self._base_kwargs())
        self.assertTrue(d.flags)
        self.assertEqual(d.tighten_add_bps, 0.0)
        self.assertFalse(d.veto)

    # --- profile=strict ---

    def test_strict_tighten(self) -> None:
        """strict profile: flags raised + tighten, but no veto."""
        d = evaluate_liq_geom(profile="strict", **self._base_kwargs())
        self.assertTrue(d.flags)
        self.assertGreater(d.tighten_add_bps, 0.0)
        self.assertFalse(d.veto)

    def test_strict_tighten_bounded_by_cap(self) -> None:
        """Tighten add should never exceed tighten_cap_bps."""
        d = evaluate_liq_geom(
            profile="strict",
            slope_bid=1.0, slope_ask=1.0,   # extreme breach
            dws_bps=9999.0,
            recovery_ms=999_999,
            thr_slope=500.0, thr_dws=5.0, thr_recovery_ms=1000,
            tighten_cap_bps=10.0, tighten_mult=100.0,  # huge mult
        )
        self.assertLessEqual(d.tighten_add_bps, 10.0)

    # --- profile=hard ---

    def test_hard_veto(self) -> None:
        """hard profile: tighten + veto with correct reason prefix."""
        d = evaluate_liq_geom(profile="hard", **self._base_kwargs())
        self.assertTrue(d.veto)
        self.assertTrue(d.veto_reason.startswith("liq_geom:"))
        self.assertGreater(d.tighten_add_bps, 0.0)

    def test_hard_veto_flags_in_reason(self) -> None:
        """Veto reason should list exactly the raised flags."""
        d = evaluate_liq_geom(profile="hard", **self._base_kwargs())
        for flag in d.flags:
            self.assertIn(flag, d.veto_reason)

    # --- no breach ---

    def test_no_flags_no_action(self) -> None:
        """When no threshold is breached, no flags, no tighten, no veto."""
        d = evaluate_liq_geom(
            profile="hard",
            slope_bid=10_000.0, slope_ask=10_000.0,  # high slope = good
            dws_bps=0.1,    # low DWS = tight book
            recovery_ms=0,  # no stress
            thr_slope=500.0, thr_dws=5.0, thr_recovery_ms=1000,
            tighten_cap_bps=10.0, tighten_mult=1.0,
        )
        self.assertFalse(d.flags)
        self.assertEqual(d.tighten_add_bps, 0.0)
        self.assertFalse(d.veto)

    # --- disabled thresholds ---

    def test_zero_threshold_disables_check(self) -> None:
        """Zero threshold means gate is disabled for that metric."""
        d = evaluate_liq_geom(
            profile="hard",
            slope_bid=1.0, slope_ask=1.0,  # would breach if threshold enabled
            dws_bps=99.0,
            recovery_ms=99999,
            thr_slope=0.0,   # disabled
            thr_dws=0.0,     # disabled
            thr_recovery_ms=0,  # disabled
            tighten_cap_bps=10.0, tighten_mult=1.0,
        )
        self.assertFalse(d.flags)
        self.assertFalse(d.veto)

    # --- unknown profile fallback ---

    def test_unknown_profile_treated_as_default(self) -> None:
        """Unknown profile strings should fall back to 'default' (annotate only)."""
        d = evaluate_liq_geom(profile="INVALID_XYZ", **self._base_kwargs())
        self.assertEqual(d.tighten_add_bps, 0.0)
        self.assertFalse(d.veto)

    # --- slope_min field ---

    def test_slope_min_field_populated(self) -> None:
        """slope_min in result should be min(slope_bid, slope_ask)."""
        d = evaluate_liq_geom(
            profile="default",
            slope_bid=300.0, slope_ask=100.0,
            dws_bps=0.0, recovery_ms=0,
            thr_slope=0.0, thr_dws=0.0, thr_recovery_ms=0,
            tighten_cap_bps=10.0, tighten_mult=1.0,
        )
        self.assertAlmostEqual(d.slope_min, 100.0, places=6)


if __name__ == "__main__":
    unittest.main()
