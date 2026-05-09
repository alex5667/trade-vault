import unittest

from core.meta_features_v3 import build_meta_features_v3


class TestMetaFeaturesV3(unittest.TestCase):
    def test_build_schema(self):
        # Empty inputs
        feat, missing = build_meta_features_v3(
            evidence={}, indicators={},
            runtime_snap=None, runtime_prev_snap=None
        )

        # Check V3 keys
        self.assertIn("burst_ctr", missing)
        self.assertIn("burst_exc", missing)

        # Check V1 key
        self.assertIn("have", feat)

    def test_build_with_evidence(self):
        # Mock evidence with burst snapshot
        evidence = {
            "burst_ctr": 1.5,
            "burst_exc": 2.0,
            "burst_churn": 0.5,
            "burst_pen": 0.1
        }

        feat, missing = build_meta_features_v3(
            evidence=evidence, indicators={},
            runtime_snap=None, runtime_prev_snap=None
        )

        self.assertIn("burst_ctr", feat)
        self.assertAlmostEqual(feat["burst_ctr"], 1.5)
        self.assertNotIn("burst_ctr", missing)

        self.assertIn("burst_exc", feat)
        self.assertAlmostEqual(feat["burst_exc"], 2.0)

        # Ensure V2 features are also handled (missing if no snap)
        self.assertIn("qimb_l1", missing)

if __name__ == "__main__":
    unittest.main()
