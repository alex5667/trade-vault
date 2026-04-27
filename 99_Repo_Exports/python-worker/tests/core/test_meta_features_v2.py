import unittest
from core.meta_features_v2 import build_meta_features_v2, META_FEAT_V2_COLS

class TestMetaFeaturesV2(unittest.TestCase):
    def test_build_schema(self):
        # Empty inputs
        feat, missing = build_meta_features_v2(
            evidence={}, indicators={}, 
            runtime_snap=None, runtime_prev_snap=None
        )
        
        # V1 keys should be present (with 0.0 or missing)
        # V2 keys should be missing because no snap
        self.assertIn("qimb_l1", missing)
        self.assertIn("ofi_ml", missing)
        
        # Check basic V1 key
        self.assertIn("have", feat)
        
    def test_build_with_snap(self):
        snap = {
            "bids": [[10, 100]], "asks": [[11, 100]]
        }
        prev = {
            "bids": [[10, 100]], "asks": [[11, 100]]
        }
        
        feat, missing = build_meta_features_v2(
            evidence={}, indicators={},
            runtime_snap=snap,
            runtime_prev_snap=prev
        )
        
        self.assertIn("qimb_l1", feat)
        self.assertAlmostEqual(feat["qimb_l1"], 0.0) # Balanced
        self.assertNotIn("qimb_l1", missing)
        
        self.assertIn("ofi_ml", feat) # Prev provided
        self.assertNotIn("ofi_ml", missing)

    def test_missing_prev_snap(self):
        snap = {"bids": [[10, 100]], "asks": [[11, 100]]}
        feat, missing = build_meta_features_v2(
            evidence={}, indicators={},
            runtime_snap=snap,
            runtime_prev_snap=None
        )
        
        self.assertIn("qimb_l1", feat) # QIMB works with just snap
        self.assertIn("ofi_ml", missing) # OFI needs prev

if __name__ == "__main__":
    unittest.main()
