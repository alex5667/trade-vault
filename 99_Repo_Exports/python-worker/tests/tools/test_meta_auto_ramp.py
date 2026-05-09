
import argparse
import unittest

from tools.meta_auto_ramp_v1 import decide_ramp


class TestMetaAutoRamp(unittest.TestCase):
    def setUp(self):
        self.args = argparse.Namespace(
            min_samples=400,
            min_pos=60,
            min_pr_auc=0.55,
            max_ece=0.08,
            max_brier=0.0,
            min_precision_at_200=0.55,
            bad_pr_auc=0.48,
            bad_ece=0.12,
            step_up=0.05,
            step_down=0.10,
            max_share=0.50,
            ramp_after_good=2,
            ramp_down_after_bad=1,
            freeze_after_bad=3,
        )

    def test_insuff_data(self):
        report = {"counts": {"n": 100, "pos": 10}} # Too small
        dec = decide_ramp(report, 0.1, 0, 0, self.args)
        self.assertEqual(dec.decision, "hold")
        self.assertIn("dataset_too_small", dec.reason)

    def test_good_metrics_increment_streak(self):
        # pr_auc=0.60, ece=0.05 -> Good
        report = {
            "counts": {"n": 1000, "pos": 100},
            "metrics": {"pr_auc": 0.60, "ece": 0.05, "precision_at_200": 0.60}
        }
        dec = decide_ramp(report, 0.1, 0, 0, self.args)
        self.assertEqual(dec.decision, "hold") # Not yet ramp (need 2 streak)
        self.assertEqual(dec.good_streak, 1)

        # Streak 2 -> Ramp Up
        dec = decide_ramp(report, 0.1, 1, 0, self.args)
        self.assertEqual(dec.decision, "ramp_up")
        self.assertAlmostEqual(dec.new_share, 0.15)
        self.assertEqual(dec.good_streak, 2)

    def test_bad_metrics_ramp_down(self):
        # pr_auc=0.40 -> Bad (<0.48)
        report = {
            "counts": {"n": 1000, "pos": 100},
            "metrics": {"pr_auc": 0.40, "ece": 0.05}
        }
        dec = decide_ramp(report, 0.2, 0, 0, self.args)
        self.assertEqual(dec.decision, "ramp_down")
        self.assertAlmostEqual(dec.new_share, 0.10)
        self.assertEqual(dec.bad_streak, 1)

    def test_freeze_on_sustained_bad(self):
        report = {
            "counts": {"n": 1000, "pos": 100},
            "metrics": {"pr_auc": 0.40, "ece": 0.05}
        }
        # bad streak 2 -> 3 => Freeze
        dec = decide_ramp(report, 0.1, 0, 2, self.args)
        self.assertEqual(dec.decision, "freeze")
        self.assertEqual(dec.new_share, 0.0)
        self.assertEqual(dec.bad_streak, 3)

    def test_neutral_decays_streaks(self):
        # pr_auc=0.50 (between 0.48 and 0.55) -> Neutral
        report = {
            "counts": {"n": 1000, "pos": 100},
            "metrics": {"pr_auc": 0.50, "ece": 0.05}
        }
        dec = decide_ramp(report, 0.1, 5, 0, self.args)
        self.assertEqual(dec.decision, "hold")
        self.assertEqual(dec.good_streak, 4) # Decayed

if __name__ == "__main__":
    unittest.main()
