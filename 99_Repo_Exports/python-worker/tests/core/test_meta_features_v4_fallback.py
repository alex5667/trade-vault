import unittest
from unittest.mock import MagicMock

from core.meta_features_v4 import build_meta_features_v4, META_FEAT_V4_NEW_COLS
import core.meta_features_v4 as meta_features_v4 # For mocking

class TestMetaFeaturesV4Fallback(unittest.TestCase):
    def setUp(self):
        # Base arguments for build_meta_features_v4
        self.base_args = {
            "runtime_prev_snap": None,
            "indicators_with_v4": None,
            "legs": None,
            "have": 0,
            "need": 0,
            "ok_soft": 0,
            "rule_score": 0.0,
            "exec_risk_norm": 0.0,
            "exec_risk_bps": 0.0,
            "ml_scenario": ""
        }

    def test_fallback_priority_runtime_snap(self):
        """Test that runtime_snap takes precedence over everything."""
        # Setup: feature in runtime calc, evidence, and indicators
        
        original_compute = meta_features_v4.compute_microstructure_v4
        try:
            # Mock return value
            mock_micro = {k: 100.0 for k in META_FEAT_V4_NEW_COLS}
            meta_features_v4.compute_microstructure_v4 = MagicMock(return_value=mock_micro)
            
            # Evidence has keys, indicators arg has keys, evidence['indicators'] has keys
            # BUT runtime_snap is provided, so it should use computed values (100.0)
            
            evidence = {k: 200.0 for k in META_FEAT_V4_NEW_COLS}
            evidence["indicators"] = {k: 400.0 for k in META_FEAT_V4_NEW_COLS}
            indicators = {k: 300.0 for k in META_FEAT_V4_NEW_COLS}
            
            # Use real snap object mock
            snap_mock = {"bids": [[100, 1]], "asks": [[101, 1]]}
            
            feat, missing = build_meta_features_v4(
                evidence=evidence,
                indicators=indicators,
                runtime_snap=snap_mock, 
                **self.base_args
            )
            
            # Check key
            k = "mp_mid_bps"
            expected = 100.0
            
            # It should be 100.0 from mock_micro
            self.assertEqual(feat[k], expected)
            
        finally:
             meta_features_v4.compute_microstructure_v4 = original_compute

    def test_fallback_priority_evidence(self):
        """Test that evidence takes precedence over indicators/nested indicators."""
        # No runtime snap
        runtime_snap = None
        
        evidence = {k: 200.0 for k in META_FEAT_V4_NEW_COLS}
        evidence["indicators"] = {k: 400.0 for k in META_FEAT_V4_NEW_COLS}
        indicators = {k: 300.0 for k in META_FEAT_V4_NEW_COLS}
        
        feat, missing = build_meta_features_v4(
            evidence=evidence,
            indicators=indicators,
            runtime_snap=runtime_snap,
            **self.base_args
        )
        
        k = "mp_mid_bps"
        self.assertEqual(feat[k], 200.0)

    def test_fallback_priority_indicators_arg(self):
        """Test that indicators arg takes precedence over nested indicators in evidence."""
        # No runtime snap, no direct evidence key
        runtime_snap = None
        
        evidence = {"indicators": {k: 400.0 for k in META_FEAT_V4_NEW_COLS}}
        # But key NOT in evidence directly
        
        indicators = {k: 300.0 for k in META_FEAT_V4_NEW_COLS}
        
        feat, missing = build_meta_features_v4(
            evidence=evidence, # Has nested
            indicators=indicators, # Has direct
            runtime_snap=runtime_snap,
            **self.base_args
        )
        
        k = "mp_mid_bps"
        # Should be indicators arg -> 300.0
        self.assertEqual(feat[k], 300.0)

    def test_fallback_priority_nested_indicators(self):
        """Test that nested indicators in evidence are used if nothing else is available."""
        # No runtime snap, no direct evidence, no indicators arg
        runtime_snap = None
        evidence = {"indicators": {k: 400.0 for k in META_FEAT_V4_NEW_COLS}}
        indicators = {}
        
        feat, missing = build_meta_features_v4(
            evidence=evidence,
            indicators=indicators,
            runtime_snap=runtime_snap,
            **self.base_args
        )
        
        k = "mp_mid_bps"
        # Since it's not anywhere else, it should pick from nested "indicators" -> 400.0
        self.assertEqual(feat[k], 400.0)

    def test_fallback_default_zero(self):
        """Test that it returns 0.0 if missing everywhere."""
        runtime_snap = None
        evidence = {}
        indicators = {}
        
        feat, missing = build_meta_features_v4(
            evidence=evidence,
            indicators=indicators,
            runtime_snap=runtime_snap,
            **self.base_args
        )
        
        k = "mp_mid_bps"
        self.assertEqual(feat[k], 0.0)
        self.assertIn(k, missing)

if __name__ == "__main__":
    unittest.main()
