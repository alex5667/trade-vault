
import unittest
from core.meta_features_v1 import build_meta_features, meta_missing_stats, META_FEATURE_COLS

class TestMetaFeaturesV1(unittest.TestCase):
    def test_build_meta_features_all_present(self):
        evidence = {
            "score_breakdown": {"base_score": 0.8, "final_score_raw": 0.9},
            "delta_z": 1.5,
            "obi": 0.5,
        }
        indicators = {}
        indicators_with_v4 = {"scenario_v4": "trend"}
        legs = {"ofi_leg": 1.0}
        runtime = None
        meta_ctx = {
            "rule_score": 0.9,
            "have": 3.0,
            "need": 3.0,
            "ok_soft": 0.0,
            "exec_risk_norm": 0.2,
            "exec_risk_bps": 5.0,
        }
        
        feat, stats = build_meta_features(evidence, indicators, indicators_with_v4, legs, runtime, meta_ctx)
        
        self.assertEqual(feat["base_score"], 0.8)
        self.assertEqual(feat["delta_z"], 1.5)
        self.assertEqual(feat["obi"], 0.5)
        self.assertEqual(feat["scn_is_trend"], 1.0)
        self.assertEqual(feat["leg_ofi_leg"], 1.0)
        
        # Check all cols are present
        for col in META_FEATURE_COLS:
            self.assertIn(col, feat)
            
    def test_meta_missing_stats(self):
        feat = {"a": 1.0, "b": 2.0}
        present = {"a", "b"} # simulated
        schema_version = "v1"
        feature_names = ["a", "b", "c"]
        
        stats = meta_missing_stats(feat, present, schema_version, feature_names)
        self.assertEqual(stats["missing_count"], 1)
        self.assertEqual(stats["missing_rate"], 1/3)
        self.assertEqual(stats["missing_cols"], ["c"])

if __name__ == "__main__":
    unittest.main()
