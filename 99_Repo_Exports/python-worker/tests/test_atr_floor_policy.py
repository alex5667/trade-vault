import unittest
from core.atr_floor_policy import compute_atr_bps_threshold

class TestATRFloorPolicy(unittest.TestCase):
    def test_tier_selection(self):
        # Default map: trend->0, range->1, thin->2
        cfg = {
            "atr_floor_tier_trend": 0,
            "atr_floor_tier_range": 1,
            "atr_floor_tier_thin": 2,
            "atr_bps_min_static": 2.0
        }
        
        # Trend -> t0
        t, rg, th = compute_atr_bps_threshold(regime="trend", cfg=cfg, t0=3, t1=5, t2=8)
        self.assertEqual(t, 0)
        self.assertEqual(th, 3.0) # max(2.0, 3.0)

        # Range -> t1
        t, rg, th = compute_atr_bps_threshold(regime="range", cfg=cfg, t0=3, t1=5, t2=8)
        self.assertEqual(t, 1)
        self.assertEqual(th, 5.0)

        # Thin -> t2
        t, rg, th = compute_atr_bps_threshold(regime="thin", cfg=cfg, t0=3, t1=5, t2=8)
        self.assertEqual(t, 2)
        self.assertEqual(th, 8.0)

    def test_static_min_enforcement(self):
        cfg = {
            "atr_floor_tier_range": 1,
            "atr_bps_min_static": 10.0
        }
        # If calib is low (e.g. 5.0), static min (10.0) should override
        t, rg, th = compute_atr_bps_threshold(regime="range", cfg=cfg, t0=1, t1=5, t2=8)
        self.assertEqual(t, 1)
        self.assertEqual(th, 10.0)

    def test_unknown_regime(self):
        cfg = {"atr_floor_tier_default": 1}
        # regime "???" -> default tier 1
        t, rg, th = compute_atr_bps_threshold(regime="???", cfg=cfg, t0=3, t1=5, t2=8)
        self.assertEqual(t, 1)
        self.assertEqual(th, 5.0)

if __name__ == '__main__':
    unittest.main()
