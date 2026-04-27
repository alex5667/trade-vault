import unittest
import sys
import os

# Add parent dir to sys.path to allow importing tools
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from tools.meta_dq_rules_v1 import dq_freeze_decision


class TestDQRules(unittest.TestCase):
    def test_freeze_on_no_coverage(self):
        report = {"metrics": {"dq_present_n": 0}}
        freeze, reason, _ = dq_freeze_decision(report, cfg2={"ramp_dq_present_min": 10})
        self.assertTrue(freeze)
        self.assertEqual(reason, "dq_coverage_too_low")

    def test_freeze_on_worst_bucket(self):
        report = {
            "metrics": {
                "dq_present_n": 1000,
                "worst_dq_bucket_pr_auc": 0.40,
                "worst_dq_bucket_ece": 0.05,
            }
        }
        cfg2 = {"ramp_dq_present_min": 10, "ramp_worst_dq_pr_auc_min": 0.52}
        freeze, reason, _ = dq_freeze_decision(report, cfg2=cfg2)
        self.assertTrue(freeze)
        self.assertEqual(reason, "dq_worst_bucket_pr_auc_low")

    def test_ok_when_thresholds_met(self):
        report = {
            "dq_present_n": 1000,
            "dq_health_mean": 0.90,
            "corr_meta_p_dq_health": 0.2,
            "worst_dq_bucket_pr_auc": 0.60,
            "worst_dq_bucket_ece": 0.05,
        }
        cfg2 = {
            "ramp_dq_present_min": 10,
            "ramp_dq_health_mean_min": 0.75,
            "ramp_dq_corr_min": -0.10,
            "ramp_worst_dq_pr_auc_min": 0.52,
            "ramp_worst_dq_ece_max": 0.12,
        }
        freeze, reason, _ = dq_freeze_decision(report, cfg2=cfg2)
        self.assertFalse(freeze)
        self.assertEqual(reason, "ok")


if __name__ == "__main__":
    unittest.main()
