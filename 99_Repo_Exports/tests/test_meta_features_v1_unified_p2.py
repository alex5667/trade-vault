import unittest
import math
from core.meta_features_v1 import build_meta_features_v1, META_FEAT_V1_COLS

class TestMetaFeaturesV1Unified(unittest.TestCase):
    def test_build_meta_features_unified(self):
        # Mock inputs
        row = {
            "have": 5,
            "need": 10,
            "ok_soft": 0,
            "rule_score": 0.85,
            "exec_risk_norm": 0.1,
            "exec_risk_bps": 12.0,
            "scenario_v4": "trend_up",
            "age_ms": 1500,
            "spread_bps": 5.5,
            "volatility_15m_bps": 20.0,
            "ofi_15m": 1.5,
            "delta_z": 1.0,
        }
        
        # In runtime, build_meta_features_v1 is called with various Dicts
        feat, missing = build_meta_features_v1(
            evidence=row,
            indicators=row,
            indicators_with_v4=row,
            legs=row,
            have=row['have'],
            need=row['need'],
            ok_soft=row['ok_soft'],
            rule_score=row['rule_score'],
            exec_risk_norm=row['exec_risk_norm'],
            exec_risk_bps=row['exec_risk_bps'],
            ml_scenario=row['scenario_v4']
        )
        
        # Verify core fields
        self.assertEqual(feat["have"], 5.0)
        self.assertEqual(feat["need"], 10.0)
        self.assertEqual(feat["rule_score"], 0.85)
        self.assertEqual(feat["scn_is_trend"], 1.0)
        self.assertEqual(feat["age_ms"], 1500.0)
        self.assertEqual(feat["spread_bps"], 5.5)
        self.assertEqual(feat["delta_z"], 1.0)
        
        # Verify all canonical columns exist in output
        for col in META_FEAT_V1_COLS:
            self.assertIn(col, feat, f"Column {col} missing from output")

    def test_build_meta_features_missing_data(self):
        # Test with minimal data
        feat, missing = build_meta_features_v1(
            evidence={},
            indicators={},
            indicators_with_v4={},
            legs={},
        )
        # Should have defaults (0.0) for missing keys
        self.assertEqual(feat["age_ms"], 0.0)
        self.assertIn("age_ms", missing)
        self.assertEqual(feat["spread_bps"], 0.0)
        self.assertIn("spread_bps", missing)

if __name__ == "__main__":
    unittest.main()
