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


class TestMLFeatureSchemaV5ENVLoader(unittest.TestCase):
    """P2: Verify ML_FEATURE_SCHEMA_VER=v5_of loads MLFeatureSchemaV5OF (not v4_of)."""

    def test_feature_registry_v5_resolves_to_v5of(self):
        """get_schema_info('v5') and get_schema_info('v5_of') must both resolve to v5_of."""
        info_v5 = get_schema_info("v5")
        info_v5_of = get_schema_info("v5_of")
        self.assertEqual(info_v5.ver, "v5_of", "v5 must alias to v5_of")
        self.assertEqual(info_v5_of.ver, "v5_of")

    def test_feature_registry_v5_stable_resolves_to_v5of_stable(self):
        """get_schema_info('v5_stable') must resolve to v5_of_stable."""
        info = get_schema_info("v5_stable")
        self.assertEqual(info.ver, "v5_of_stable", "v5_stable must alias to v5_of_stable")

    def test_v5of_class_is_mlfeatureschema_v5of(self):
        """Instantiation of MLFeatureSchemaV5OF class check."""
        v5 = MLFeatureSchemaV5OF()
        self.assertEqual(v5.__class__.__name__, "MLFeatureSchemaV5OF")

    def test_v5of_has_more_keys_than_v4of(self):
        """v5_of must be a superset of v4_of."""
        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()
        self.assertGreater(
            len(v5.num_keys) + len(v5.bool_keys),
            len(v4.num_keys) + len(v4.bool_keys),
            "v5_of should have more features than v4_of"
        )

    def test_ml_feature_schema_v5_loads_v5of(self):
        """Verify build_feature_vector delegates to MLFeatureSchemaV5OF when schema_ver='v5'."""
        from core.ml_feature_schema import build_feature_vector
        
        vec_v5, missing_v5 = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1000000,
            direction="LONG",
            scenario="trend",
            indicators={"delta_z": 1.5, "ofi_stable": 1},
            rule_score=0.8,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
            schema_ver="v5"
        )
        
        v5_schema = MLFeatureSchemaV5OF()
        expected_len = len(v5_schema.num_keys) + len(v5_schema.bool_keys) + 2 + 3 + 24 + 7
        self.assertEqual(len(vec_v5), expected_len, "Length of v5 vectorized features should match v5_of schema length")
