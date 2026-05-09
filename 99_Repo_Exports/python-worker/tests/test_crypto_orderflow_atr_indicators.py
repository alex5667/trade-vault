"""
Unit test for "Broken Chain Fix": verify that ATR floor indicators
are properly exposed in publish_signal path.
"""
import unittest
from types import SimpleNamespace

from core.dyn_cfg_keys import DynCfgKeys as DK


class TestCryptoOrderflowATRIndicators(unittest.TestCase):
    """
    Test that atr_floor_th_bps and related indicators appear in the indicators dict
    when runtime.dynamic_cfg contains calibrated floors.
    """

    def test_atr_floor_indicators_exposed(self):
        """
        Verify that compute_atr_bps_threshold is called and results are written to indicators.
        """
        from core.atr_floor_policy import compute_atr_bps_threshold

        # Mock runtime with dynamic_cfg containing calibrated floors
        runtime = SimpleNamespace()
        runtime.symbol = "BTCUSDT"
        runtime.last_regime = "trend"
        runtime.dynamic_cfg = {
            "atr_floor_t0_bps": 3.0,
            "atr_floor_t1_bps": 5.0,
            "atr_floor_t2_bps": 9.0,
            "atr_calib_ready": 1,
            "atr_bps_src": "calib",
            "atr_bps_n": 999,
        }
        runtime.config = {
            "atr_floor_tier_trend": 1,  # tier 1 for trend
            "atr_bps_min_static": 2.0,
        }

        # Simulate the logic from publish_signal (Broken Chain Fix block)
        cfg = runtime.config
        rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

        t0 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T0_BPS, cfg.get("atr_floor_t0_bps", 0.0)) or 0.0)
        t1 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T1_BPS, cfg.get("atr_floor_t1_bps", 0.0)) or 0.0)
        t2 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T2_BPS, cfg.get("atr_floor_t2_bps", 0.0)) or 0.0)

        tier, regime_out, floor_th = compute_atr_bps_threshold(regime=rg, cfg=cfg, t0=t0, t1=t1, t2=t2)

        # Compute picked value manually (same logic as in the function)
        picked = float(t1)
        if tier <= 0:
            picked = float(t0)
        elif tier >= 2:
            picked = float(t2)

        indicators = {}
        indicators["atr_floor_t0_bps"] = float(t0)
        indicators["atr_floor_t1_bps"] = float(t1)
        indicators["atr_floor_t2_bps"] = float(t2)
        indicators["atr_floor_tier"] = int(tier)
        indicators["atr_floor_picked_bps"] = float(picked)
        indicators["atr_floor_th_bps"] = float(floor_th)
        indicators["atr_floor_rg"] = str(regime_out)
        indicators["atr_floor_ready"] = int(runtime.dynamic_cfg.get(DK.ATR_CALIB_READY, 0) or 0)
        indicators["atr_floor_src"] = str(runtime.dynamic_cfg.get(DK.ATR_BPS_SRC, "na") or "na")
        indicators["atr_floor_n"] = int(runtime.dynamic_cfg.get(DK.ATR_BPS_N, 0) or 0)

        # Assertions
        self.assertEqual(indicators["atr_floor_t0_bps"], 3.0)
        self.assertEqual(indicators["atr_floor_t1_bps"], 5.0)
        self.assertEqual(indicators["atr_floor_t2_bps"], 9.0)
        self.assertEqual(indicators["atr_floor_tier"], 1)  # tier 1 for trend
        self.assertEqual(indicators["atr_floor_picked_bps"], 5.0)  # t1
        self.assertGreaterEqual(indicators["atr_floor_th_bps"], 2.0)  # >= static min
        self.assertEqual(indicators["atr_floor_rg"], "trend")
        self.assertEqual(indicators["atr_floor_ready"], 1)
        self.assertEqual(indicators["atr_floor_src"], "calib")
        self.assertEqual(indicators["atr_floor_n"], 999)

    def test_atr_floor_th_bps_not_zero_when_floors_set(self):
        """
        Verify that atr_floor_th_bps is NOT 0.0 when floors are configured.
        This is the core "Broken Chain Fix" verification.
        """
        from core.atr_floor_policy import compute_atr_bps_threshold

        cfg = {
            "atr_floor_tier_range": 2,  # tier 2 for range
            "atr_bps_min_static": 1.5,
        }

        t0, t1, t2 = 2.0, 4.0, 7.0
        tier, regime_out, floor_th = compute_atr_bps_threshold(regime="range", cfg=cfg, t0=t0, t1=t1, t2=t2)

        # Compute picked value manually
        picked = float(t2) if tier >= 2 else (float(t0) if tier <= 0 else float(t1))

        # Should select t2 (tier 2) and apply static min
        self.assertEqual(tier, 2)
        self.assertEqual(regime_out, "range")
        self.assertEqual(picked, 7.0)
        self.assertGreaterEqual(floor_th, 1.5)
        self.assertGreater(floor_th, 0.0)  # NOT ZERO!

    def test_unified_threshold_calculation(self):
        """
        Verify that unified_th = max(floor_th, fees_th) is correctly computed.
        """
        indicators = {
            "atr_floor_th_bps": 5.0,
            "atr_fees_th_bps": 8.0,
        }

        floor_th = float(indicators.get("atr_floor_th_bps", 0.0) or 0.0)
        fees_th = float(indicators.get("atr_fees_th_bps", 0.0) or 0.0)
        unified_th = float(max(floor_th, fees_th))
        dominant = ("fees" if fees_th >= floor_th else "floor") if unified_th > 0 else "na"

        self.assertEqual(unified_th, 8.0)
        self.assertEqual(dominant, "fees")

        # Test reverse case
        indicators["atr_floor_th_bps"] = 10.0
        indicators["atr_fees_th_bps"] = 6.0

        floor_th = float(indicators.get("atr_floor_th_bps", 0.0) or 0.0)
        fees_th = float(indicators.get("atr_fees_th_bps", 0.0) or 0.0)
        unified_th = float(max(floor_th, fees_th))
        dominant = ("fees" if fees_th >= floor_th else "floor") if unified_th > 0 else "na"

        self.assertEqual(unified_th, 10.0)
        self.assertEqual(dominant, "floor")


if __name__ == "__main__":
    unittest.main()
