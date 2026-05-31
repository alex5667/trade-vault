import unittest

from core.meta_features_v2 import META_FEAT_V2_HASH, build_meta_features_v2


class TestMetaFeaturesV2Compat(unittest.TestCase):
    def test_hash_format(self):
        # Should be full sha256 (64 chars)
        self.assertEqual(len(META_FEAT_V2_HASH), 64)

    def test_kwarg_compat(self):
        # Mock inputs
        evidence = {}
        indicators = {}
        # New signature call
        feat, missing = build_meta_features_v2(evidence, indicators, have=1, need=1)
        self.assertIsInstance(feat, dict)
        self.assertIsInstance(missing, list)

    def test_old_signature_call(self):
        # Mock inputs
        evidence = {}
        indicators = {}
        # Old signature call (passing args that end up in kwargs)
        feat, missing = build_meta_features_v2(evidence, indicators, rule_score=0.99)
        self.assertIsInstance(feat, dict)

if __name__ == "__main__":
    unittest.main()
