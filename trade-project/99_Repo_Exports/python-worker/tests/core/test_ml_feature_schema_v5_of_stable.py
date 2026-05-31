import json
import os
import tempfile
import unittest

from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF
from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OFStable

SCHEMA_HASH = "b792f630e013"


class TestMLFeatureSchemaV5OFStable(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.deny_path = os.path.join(self.temp_dir.name, "deny.json")
        os.environ["ML_FEATURE_DENYLIST_PATH"] = self.deny_path

    def tearDown(self):
        if "ML_FEATURE_DENYLIST_PATH" in os.environ:
            del os.environ["ML_FEATURE_DENYLIST_PATH"]
        if "ML_FEATURE_DENYLIST_ALLOW_CORE" in os.environ:
            del os.environ["ML_FEATURE_DENYLIST_ALLOW_CORE"]
        self.temp_dir.cleanup()

    def test_default_behavior_no_file(self):
        if os.path.exists(self.deny_path):
            os.remove(self.deny_path)

        s = MLFeatureSchemaV5OFStable()
        self.assertEqual(s.denylist_hash16, "na")
        self.assertTrue(len(s.num_keys) > 0)

    def test_denylist_filtering(self):
        core = MLFeatureSchemaV4OF()

        # Let's say v5 adds: lob_imb_5, lob_imb_10
        # and we want to deny one v5 feature that is NOT in core.
        # But wait, we don't know exact v5 keys here, so we instantiate it first to find a non-core feature
        s_base = MLFeatureSchemaV5OFStable()
        non_core = [k for k in s_base.num_keys if k not in core.num_keys]
        if not non_core:
            self.skipTest("No non-core features in v5 to filter")

        target_deny = non_core[0]

        with open(self.deny_path, "w", encoding="utf-8") as f:
            json.dump({"deny_num": [target_deny]}, f)

        s = MLFeatureSchemaV5OFStable()
        self.assertNotEqual(s.denylist_hash16, "na")
        self.assertNotIn(target_deny, s.num_keys)
        # Check core keys are untouched
        for k in core.num_keys:
            self.assertIn(k, s.num_keys)

    def test_core_protection(self):
        core = MLFeatureSchemaV4OF()
        target_deny = core.num_keys[0] # Try to deny a core feature

        with open(self.deny_path, "w", encoding="utf-8") as f:
            json.dump({"deny_num": [target_deny]}, f)

        s = MLFeatureSchemaV5OFStable()
        # By default, core features are protected
        self.assertIn(target_deny, s.num_keys)

        # Now allow core denial
        os.environ["ML_FEATURE_DENYLIST_ALLOW_CORE"] = "1"
        s2 = MLFeatureSchemaV5OFStable()
        self.assertNotIn(target_deny, s2.num_keys)

if __name__ == "__main__":
    unittest.main()
