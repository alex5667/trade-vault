
import unittest

from core.meta_features_v5 import META_FEAT_V5_NEW_COLS, build_meta_features_v5


class TestMetaFeaturesV5DQ(unittest.TestCase):
    def test_dq_keys_present(self):
        """Verify new DQ keys are in the NEW_COLS list."""
        self.assertIn("tick_ts_source_now_ema", META_FEAT_V5_NEW_COLS)
        self.assertIn("tick_ts_source_stream_id_ema", META_FEAT_V5_NEW_COLS)
        self.assertIn("tick_event_age_abs_ema_ms", META_FEAT_V5_NEW_COLS)

    def test_alias_logic(self):
        """Verify fallback to tick_time_age_abs_ema_ms if tick_event_age_abs_ema_ms is missing."""
        evidence = {
            "tick_time_age_abs_ema_ms": 123.45
        }
        feat, missing = build_meta_features_v5(evidence, {}, ml_scenario="test")

        self.assertEqual(feat["tick_event_age_abs_ema_ms"], 123.45)
        self.assertNotIn("tick_event_age_abs_ema_ms", missing)

    def test_canonical_priority(self):
        """Verify canonical key takes precedence over alias."""
        evidence = {
            "tick_event_age_abs_ema_ms": 999.0,
            "tick_time_age_abs_ema_ms": 111.0
        }
        feat, missing = build_meta_features_v5(evidence, {}, ml_scenario="test")

        self.assertEqual(feat["tick_event_age_abs_ema_ms"], 999.0)

    def test_new_dq_keys_extraction(self):
        """Verify extraction of new DQ keys from evidence."""
        evidence = {
            "tick_ts_source_now_ema": 1000.0,
            "tick_ts_source_stream_id_ema": 5.0
        }
        feat, missing = build_meta_features_v5(evidence, {}, ml_scenario="test")

        self.assertEqual(feat["tick_ts_source_now_ema"], 1000.0)
        self.assertEqual(feat["tick_ts_source_stream_id_ema"], 5.0)
        self.assertNotIn("tick_ts_source_now_ema", missing)
        self.assertNotIn("tick_ts_source_stream_id_ema", missing)

    def test_nested_indicators_priority(self):
        """Verify priority: evidence > indicators > indicators dict inside evidence."""
        # Case 1: evidence wins
        evidence = {"tick_ts_source_now_ema": 100.0}
        indicators = {"tick_ts_source_now_ema": 200.0}
        feat, _ = build_meta_features_v5(evidence, indicators, ml_scenario="test")
        self.assertEqual(feat["tick_ts_source_now_ema"], 100.0)

        # Case 2: indicators wins if evidence missing
        evidence = {}
        indicators = {"tick_ts_source_now_ema": 200.0}
        feat, _ = build_meta_features_v5(evidence, indicators, ml_scenario="test")
        self.assertEqual(feat["tick_ts_source_now_ema"], 200.0)

        # Case 3: nested indicators wins if others missing
        evidence = {"indicators": {"tick_ts_source_now_ema": 300.0}}
        indicators = {}
        feat, _ = build_meta_features_v5(evidence, indicators, ml_scenario="test")
        self.assertEqual(feat["tick_ts_source_now_ema"], 300.0)

if __name__ == "__main__":
    unittest.main()
