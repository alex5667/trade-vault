import unittest
from unittest.mock import MagicMock, patch

from core.meta_model_lr import MetaModelLR
from core.of_confirm_engine import OFConfirmEngine


class TestMissingMetrics(unittest.TestCase):
    def setUp(self):
        self.engine = OFConfirmEngine()
        self.engine._replay_mode = True

    @patch("core.of_confirm_engine.feature_missing_total")
    @patch("core.of_confirm_engine.meta_feature_missing_total")
    @patch("core.of_confirm_engine.meta_feature_seen_total")
    @patch("core.meta_model_lr.MetaModelLR.load")
    def test_metrics_emission(self, mock_load, mock_mfst, mock_mfmt, mock_fmt):
        # 1. Setup a dummy model with known features
        dummy_model = MetaModelLR(
            features=["f1", "f2", "f3"],
            intercept=0.0,
            coef=[0.1, 0.2, 0.3],
            schema_name="test_schema",
            schema_version=1
        )
        mock_load.return_value = dummy_model

        self.engine._meta_model = dummy_model
        # Ensure the paths match what the engine expects to see set if it were loaded
        self.engine._meta_model_path = "dummy/path"

        runtime = MagicMock()
        runtime.book_state = MagicMock()

        cfg = {"meta_model_enable": 1, "meta_model_mode": "SHADOW", "meta_model_path": "dummy/path"}
        indicators = {}

        # Patch build_meta_features_v1 via the module where OFConfirmEngine is defined (core.of_confirm_engine)
        # Note: OFConfirmEngine imports it. We need to patch 'core.of_confirm_engine.build_meta_features_v1'.
        with patch("core.of_confirm_engine.build_meta_features_v1") as mock_build:
            # Returns (features_dict, missing_list)
            # f1 is present (in dict, not in missing)
            # f2, f3 are missing
            mock_build.return_value = ({"f1": 1.0}, ["f2", "f3"])

            self.engine.build(
                symbol="BTCUSDT",
                tf="1m",
                direction="LONG",
                tick_ts_ms=1000,
                price=100.0,
                delta_z=1.0,
                runtime=runtime,
                cfg=cfg,
                indicators=indicators
            )

            # 4. Verify calls on the local mocks
            # seen: f1, f2, f3
            self.assertEqual(mock_mfst.call_count, 3)
            # Any order is fine
            calls = [c.kwargs for c in mock_mfst.call_args_list]
            # args[0] is runtime (positional)
            # kwargs are schema, feature

            # Simple check
            mock_mfst.assert_any_call(runtime, schema="test_schema", feature="f1")
            mock_mfst.assert_any_call(runtime, schema="test_schema", feature="f2")
            mock_mfst.assert_any_call(runtime, schema="test_schema", feature="f3")

            # missing: f2, f3
            self.assertEqual(mock_mfmt.call_count, 2)
            mock_mfmt.assert_any_call(runtime, schema="test_schema", feature="f2")
            mock_mfmt.assert_any_call(runtime, schema="test_schema", feature="f3")

            # legacy missing: f2, f3
            self.assertEqual(mock_fmt.call_count, 2)
            mock_fmt.assert_any_call(runtime, feature="f2")
            mock_fmt.assert_any_call(runtime, feature="f3")

if __name__ == "__main__":
    unittest.main()
