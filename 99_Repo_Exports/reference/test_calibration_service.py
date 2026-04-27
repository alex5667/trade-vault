#!/usr/bin/env python3
"""
Unit tests for LocalCalibrationService
"""

import unittest
from unittest.mock import Mock
from calibration.local_calibration_service import LocalCalibrationService


class TestLocalCalibrationService(unittest.TestCase):

    def setUp(self):
        # Mock LCStoreV2
        self.mock_store = Mock()
        self.service = LocalCalibrationService(self.mock_store)

    def test_initial_calibration(self):
        """Test calibration with no historical data"""
        # Mock empty store
        self.mock_store.get_metric_cfg.return_value = None

        # Create mock context with a metric that's in the default list
        ctx = Mock()
        ctx.symbol = "BTCUSDT"
        ctx.session = "mixed"
        ctx.regime_label = "mixed"
        ctx.metrics = {"deltaSpike_z": 1.0}
        ctx.calibrated = {}

        self.service.apply_calibration(ctx)

        # Should have calibrated the metric with neutral values
        self.assertIn("deltaSpike_z", ctx.calibrated)
        calibrated = ctx.calibrated["deltaSpike_z"]
        self.assertEqual(calibrated["value"], 1.0)
        self.assertEqual(calibrated["quantile"], 0.5)

    def test_calibration_with_historical_data(self):
        """Test calibration with mock historical configuration"""
        # Mock store with configuration
        mock_cfg = Mock()
        mock_cfg.threshold = 2.0
        mock_cfg.q90 = 1.8
        mock_cfg.q95 = 2.2
        mock_cfg.q98 = 2.5
        mock_cfg.cdf_points = [
            {"x": 0.5, "y": 0.1},
            {"x": 1.0, "y": 0.5},
            {"x": 1.5, "y": 0.9},
        ]

        self.mock_store.get_metric_cfg.return_value = mock_cfg

        # Mock eval_local_quantile function
        import calibration.local_calibration_service as calib_module
        original_eval = calib_module.eval_local_quantile
        calib_module.eval_local_quantile = lambda points, value: 0.7

        try:
            ctx = Mock()
            ctx.symbol = "BTCUSDT"
            ctx.session = "mixed"
            ctx.regime_label = "mixed"
            ctx.metrics = {"deltaSpike_z": 2.5}
            ctx.calibrated = {}

            self.service.apply_calibration(ctx)

            calibrated = ctx.calibrated["deltaSpike_z"]
            self.assertEqual(calibrated["value"], 2.5)
            self.assertTrue(calibrated["is_extreme"])  # 2.5 > 2.0 threshold
            self.assertEqual(calibrated["threshold"], 2.0)
            self.assertEqual(calibrated["quantile"], 0.7)
            self.assertEqual(calibrated["p90"], 2.5)

        finally:
            calib_module.eval_local_quantile = original_eval

    def test_calibration_stats(self):
        """Test getting calibration statistics"""
        # Mock configurations for different metrics
        def mock_get_metric_cfg(symbol, session, regime, metric):
            mock_cfg = Mock()
            mock_cfg.q90 = 1.8
            mock_cfg.q95 = 2.2
            mock_cfg.q98 = 2.5
            mock_cfg.threshold = 2.0
            mock_cfg.count_samples = 1000
            mock_cfg.cdf_points = [1, 2, 3, 4, 5]  # Add cdf_points
            return mock_cfg

        self.mock_store.get_metric_cfg.side_effect = mock_get_metric_cfg

        stats = self.service.get_calibration_stats("BTCUSDT", "mixed", "mixed")

        self.assertIn("deltaSpike_z", stats)

        delta_stats = stats["deltaSpike_z"]
        self.assertEqual(delta_stats["q90"], 1.8)
        self.assertEqual(delta_stats["threshold"], 2.0)
        self.assertEqual(delta_stats["count_samples"], 1000)
        self.assertEqual(delta_stats["cdf_points_count"], 5)

    def test_no_store_no_calibration(self):
        """Test behavior when store is None"""
        service = LocalCalibrationService(None)

        ctx = Mock()
        ctx.metrics = {"test": 1.0}
        ctx.calibrated = {}

        # Should not raise exception
        service.apply_calibration(ctx)
        # calibrated should remain empty since store is None
        self.assertEqual(len(ctx.calibrated), 0)


if __name__ == '__main__':
    unittest.main()
