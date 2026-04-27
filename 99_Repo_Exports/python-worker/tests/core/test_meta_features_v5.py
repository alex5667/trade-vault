import unittest
from core.meta_features_v5 import build_meta_features_v5, META_FEAT_V5_COLS, META_FEAT_V5_NEW_COLS

class TestMetaFeaturesV5(unittest.TestCase):
    def test_build_meta_features_v5_basics(self):
        evidence = {
            # V3/V4 base
            "rule_score": 0.8,
            "exec_risk_norm": 0.5,
            # V5 keys
            "tick_time_age_ms": 100,
            "tick_time_age_abs_ema_ms": 50,
            "tick_event_stream_skew_abs_ema_ms": 10,
            "data_health": 1.0,
            "book_health_ok": 1,
            "tick_unknown_side_ema": 0.0,
        }
        
        feat, missing = build_meta_features_v5(
            evidence=evidence,
            indicators=evidence,  # simplistic reuse
        )
        
        # Check V5 specific columns
        for col in META_FEAT_V5_NEW_COLS:
            self.assertIn(col, feat, f"Missing V5 col: {col}")
            self.assertEqual(feat[col], evidence[col])

        # Check that missing columns are tracked in 'missing' list
        # V2 features might be missing from feat if snap is None, but should be in missing list
        missing_cols_in_feat = set(META_FEAT_V5_COLS) - set(feat.keys())
        for col in missing_cols_in_feat:
            self.assertIn(col, missing, f"Column {col} missing from feat but not in missing list")

        # Ensure we have at least V1 + V5 columns (V1 usually always present as they come from evidence)
        # and some base keys.
        self.assertTrue(len(feat) > len(META_FEAT_V5_NEW_COLS))
        
    def test_build_meta_features_v5_fallback_indicators(self):
        evidence = {
            "rule_score": 0.8,
        }
        indicators = {
             "tick_time_age_ms": 123,
        }
        
        feat, missing = build_meta_features_v5(
            evidence=evidence,
            indicators=indicators,
        )
        
        self.assertEqual(feat.get("tick_time_age_ms"), 123.0)
        
    def test_build_meta_features_v5_fallback_nested(self):
        evidence = {
            "rule_score": 0.8,
            "indicators": {
                "tick_time_age_ms": 456,
            }
        }
        
        feat, missing = build_meta_features_v5(
            evidence=evidence,
            indicators={},
        )
        
        self.assertEqual(feat.get("tick_time_age_ms"), 456.0)

    def test_build_meta_features_v5_missing(self):
        evidence = {"rule_score": 0.5}
        feat, missing = build_meta_features_v5(evidence=evidence, indicators={})
        
        # should be 0.0 and in missing list
        self.assertIn("tick_time_age_ms", missing)
        self.assertEqual(feat.get("tick_time_age_ms"), 0.0)

if __name__ == "__main__":
    unittest.main()
