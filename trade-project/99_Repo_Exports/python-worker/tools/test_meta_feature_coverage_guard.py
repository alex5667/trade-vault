"""Unit tests for meta-feature coverage guard (P29)."""

import unittest
from types import SimpleNamespace

from core.meta_feature_coverage import apply_meta_coverage_guard, compute_meta_feature_coverage


class TestMetaFeatureCoverage(unittest.TestCase):

    def test_compute_coverage_full(self):
        model_feats = ["f1", "f2", "f3"]
        missing = []
        cov = compute_meta_feature_coverage(model_feats, missing)
        self.assertEqual(cov.model_total, 3)
        self.assertEqual(cov.model_missing, 0)
        self.assertEqual(cov.coverage, 1.0)
        self.assertEqual(cov.missing_rate, 0.0)

    def test_compute_coverage_partial(self):
        model_feats = ["f1", "f2", "f3", "f4"]
        missing = ["f1", "other_f"]
        cov = compute_meta_feature_coverage(model_feats, missing)
        self.assertEqual(cov.model_total, 4)
        self.assertEqual(cov.model_missing, 1)  # only f1 is in model
        self.assertEqual(cov.coverage, 0.75)
        self.assertEqual(cov.missing_model_features, ["f1"])

    def test_apply_guard_enforce_ok(self):
        cov = SimpleNamespace(coverage=0.9, model_missing=1)
        mode, reason = apply_meta_coverage_guard("ENFORCE", cov, min_coverage=0.85)
        self.assertEqual(mode, "ENFORCE")
        self.assertEqual(reason, "")

    def test_apply_guard_enforce_fail_coverage(self):
        cov = SimpleNamespace(coverage=0.8, model_missing=2)
        mode, reason = apply_meta_coverage_guard("ENFORCE", cov, min_coverage=0.85)
        self.assertEqual(mode, "SHADOW")
        self.assertIn("LOW_COVERAGE", reason)

    def test_apply_guard_enforce_fail_missing_count(self):
        cov = SimpleNamespace(coverage=0.9, model_missing=10)
        mode, reason = apply_meta_coverage_guard("ENFORCE", cov, min_coverage=0.8, max_missing=5)
        self.assertEqual(mode, "SHADOW")
        self.assertIn("TOO_MANY_MISSING", reason)

    def test_apply_guard_shadow_stays_shadow(self):
        cov = SimpleNamespace(coverage=0.1, model_missing=90)
        mode, reason = apply_meta_coverage_guard("SHADOW", cov)
        self.assertEqual(mode, "SHADOW")
        self.assertEqual(reason, "")

    def test_empty_model(self):
        cov = compute_meta_feature_coverage([], ["f1"])
        self.assertEqual(cov.coverage, 1.0)
        self.assertEqual(cov.model_total, 0)


if __name__ == "__main__":
    unittest.main()
