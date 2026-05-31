import unittest
from types import SimpleNamespace

from core.weak_progress import compute_weak_progress


class TestWeakProgressConfig(unittest.TestCase):
    def test_dual_thresholds(self):
        # Setup mock bar: High Range (0.5), Small Body (0.1)
        # ATR = 1.0
        # Range/ATR = 0.5
        # Body/ATR = 0.1

        bar = SimpleNamespace(
            open=100.0,
            high=100.5,
            low=100.0,
            close=100.1,
            delta_sum=100.0, # High delta -> look for weak progress
            fp_bucket_px=0.1
        )
        atr = 1.0

        # Case 1: Loose Range (0.6), Tight Body (0.05)
        # Range (0.5) < 0.6 -> Weak Range TRUE
        # Body (0.1) > 0.05 -> Weak Body FALSE
        # Result: Weak Any = TRUE (OR logic)
        cfg1 = {
            "weak_progress_range_atr": 0.6,
            "weak_progress_body_atr": 0.05,
            "tick_size_px": 0.1,
        }
        res1 = compute_weak_progress(bar, atr, cfg1)
        self.assertTrue(res1.weak_range, "Range should be weak (0.5 < 0.6)")
        self.assertFalse(res1.weak_body, "Body should NOT be weak (0.1 > 0.05)")
        self.assertTrue(res1.weak_any, "Should be weak any")

        # Case 2: Tight Range (0.4), Loose Body (0.2)
        # Range (0.5) > 0.4 -> Weak Range FALSE
        # Body (0.1) < 0.2 -> Weak Body TRUE
        # Result: Weak Any = TRUE
        cfg2 = {
            "weak_progress_range_atr": 0.4,
            "weak_progress_body_atr": 0.2,
            "tick_size_px": 0.1,
        }
        res2 = compute_weak_progress(bar, atr, cfg2)
        self.assertFalse(res2.weak_range, "Range should NOT be weak (0.5 > 0.4)")
        self.assertTrue(res2.weak_body, "Body should be weak (0.1 < 0.2)")
        self.assertTrue(res2.weak_any, "Should be weak any")

        # Case 3: Both Tight
        # Range (0.5) > 0.4
        # Body (0.1) > 0.05
        # Result: Weak Any = FALSE (ignore eff for now)
        cfg3 = {
            "weak_progress_range_atr": 0.4,
            "weak_progress_body_atr": 0.05,
            "tick_size_px": 0.1,
            "weak_progress_eff_max": 0.0001 # strict eff
        }
        res3 = compute_weak_progress(bar, atr, cfg3)
        self.assertFalse(res3.weak_range)
        self.assertFalse(res3.weak_body)

    def test_default_fallback(self):
        # Defaults: Range 0.35, Body 0.25 (as updated in code)
        bar = SimpleNamespace(
            open=100.0, high=100.4, low=100.0, close=100.3, delta_sum=100.0
        )
        # Range 0.4, Body 0.3
        # Both > defaults (0.4 > 0.35, 0.3 > 0.25) -> Not Weak
        atr = 1.0
        res = compute_weak_progress(bar, atr, {})
        self.assertFalse(res.weak_range)
        self.assertFalse(res.weak_body)

if __name__ == '__main__':
    unittest.main()
