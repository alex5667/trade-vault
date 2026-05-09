import unittest

from core.feature_registry import get_schema_info
from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF
from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF

SCHEMA_HASH = "523c6117fe13"


class TestMLFeatureSchemaV5OF(unittest.TestCase):
    def test_v5_schema_superset(self):
        """v5_of must be a strict superset of v4_of and maintain order for the original keys."""
        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()

        # Check num keys
        self.assertTrue(len(v5.num_keys) > len(v4.num_keys))
        v4_num_in_v5 = v5.num_keys[:len(v4.num_keys)]
        self.assertEqual(v4.num_keys, v4_num_in_v5)

        # Check bool keys
        self.assertTrue(len(v5.bool_keys) > len(v4.bool_keys))
        v4_bool_in_v5 = v5.bool_keys[:len(v4.bool_keys)]
        self.assertEqual(v4.bool_keys, v4_bool_in_v5)

    def test_feature_registry_v5_resolution(self):
        info_v5 = get_schema_info("v5")
        info_v5_of = get_schema_info("v5_of")

        self.assertEqual(info_v5.ver, "v5_of")
        self.assertEqual(info_v5_of.ver, "v5_of")

        # Ensure that feature names list has both num and bool keys
        v5 = MLFeatureSchemaV5OF()
        expected_names = []
        for k in v5.num_keys:
            expected_names.append(f"n:{k}")
        for k in v5.bool_keys:
            expected_names.append(f"b:{k}")

        expected_names += ["dir:LONG", "dir:SHORT"]
        expected_names += ["bucket:trend", "bucket:range", "bucket:other"]
        expected_names += [f"hour:{h}" for h in range(24)]
        expected_names += [f"dow:{d}" for d in range(7)]

        self.assertEqual(info_v5.feature_names, expected_names)

if __name__ == "__main__":
    unittest.main()
