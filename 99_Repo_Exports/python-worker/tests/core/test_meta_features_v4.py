import unittest
from core.meta_features_v4 import build_meta_features_v4, META_FEAT_V4_NEW_COLS

class TestMetaFeaturesV4(unittest.TestCase):
    def test_build_schema_v4(self):
        # Empty inputs
        feat, missing = build_meta_features_v4(
            evidence={}, indicators={}, 
            runtime_snap=None, runtime_prev_snap=None
        )
        
        # Check V4 keys exist
        for k in META_FEAT_V4_NEW_COLS:
            self.assertIn(k, feat) # either 0.0 or from indicators
            if k not in feat or feat[k] == 0.0:
                 pass # Might be missing or default 0.0
                 
        # Since snap is None, they should be 0.0 and in missing usually?
        # The code implementation sets them to 0.0 if not found, and adds to missing.
        # Let's check "mp_mid_bps"
        self.assertEqual(feat["mp_mid_bps"], 0.0)
        self.assertIn("mp_mid_bps", missing)
        
    def test_build_with_snap(self):
        bids = [[10, 100], [9, 100], [8, 100], [7, 100], [6, 100]]
        asks = [[11, 100], [12, 100], [13, 100], [14, 100], [15, 100]]
        snap = {"bids": bids, "asks": asks}
        
        feat, missing = build_meta_features_v4(
            evidence={}, indicators={}, 
            runtime_snap=snap, runtime_prev_snap=None
        )
        
        self.assertAlmostEqual(feat["depth_bid_5"], 500.0)
        self.assertNotIn("depth_bid_5", missing)
        
        # Check defaults from V3/V2 are still there
        self.assertIn("have", feat) # V1 default

if __name__ == "__main__":
    unittest.main()
