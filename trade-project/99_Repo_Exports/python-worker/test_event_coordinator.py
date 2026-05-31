"""
Unit tests for EventCoordinator.

Tests event coordination logic in isolation.
"""

import time
import unittest
from unittest.mock import Mock, MagicMock, patch
from types import SimpleNamespace

from handlers.coordination.event_coordinator import EventCoordinator


class TestEventCoordinator(unittest.TestCase):
    """Test suite for EventCoordinator."""
    
    def setUp(self):
        """Create fresh coordinator for each test."""
        self.data_processor = Mock()
        self.cache_service = Mock()
        self.session_service = Mock()
        self.calibration_service = Mock()
        self.health_metrics = Mock()
        self.logger = Mock()
        
        self.coordinator = EventCoordinator(
            symbol="BTCUSDT",
            data_processor=self.data_processor,
            cache_service=self.cache_service,
            session_service=self.session_service,
            calibration_service=self.calibration_service,
            health_metrics=self.health_metrics,
            logger=self.logger,
        )
    
    def test_initialization(self):
        """Test coordinator initializes correctly."""
        self.assertEqual(self.coordinator.symbol, "BTCUSDT")
        self.assertEqual(self.coordinator._data_processor, self.data_processor)
        self.assertEqual(self.coordinator.health_metrics, self.health_metrics)
    
    def test_set_health_callback(self):
        """Test setting health callback."""
        callback = Mock()
        self.coordinator.set_health_callback(callback)
        self.assertEqual(self.coordinator._emit_health_callback, callback)
    
    def test_on_bar_closed_success(self):
        """Test successful bar close handling."""
        # Setup
        bar = SimpleNamespace(ts_open=1000000, ts_close=1060000)
        pivots = {"R1": 50000, "S1": 49000}
        ctx = SimpleNamespace(ts=1000000, obi=0.5)
        
        self.cache_service.get_pivots_bundle.return_value = pivots
        self.data_processor.build_signal_ctx.return_value = ctx
        
        # Execute
        result = self.coordinator.on_bar_closed(bar)
        
        # Verify
        self.assertIsNotNone(result)
        self.assertEqual(result.ts, 1060000)  # Should be updated to ts_close
        self.cache_service.ensure_pivots_bundle.assert_called_once_with(1060000)
        self.data_processor.build_signal_ctx.assert_called_once_with(pivots=pivots)
        self.session_service.attach_to_ctx.assert_called_once_with(ctx)
        self.calibration_service.calibrate_context.assert_called_once_with(ctx)
    
    def test_on_bar_closed_with_health_callback(self):
        """Test bar close with health metrics callback."""
        bar = SimpleNamespace(ts_close=1060000)
        ctx = SimpleNamespace(ts=1000000)
        health_callback = Mock()
        
        self.data_processor.build_signal_ctx.return_value = ctx
        self.coordinator.set_health_callback(health_callback)
        
        # Execute
        self.coordinator.on_bar_closed(bar)
        
        # Verify health callback was called
        health_callback.assert_called_once_with(
            self.health_metrics,
            symbol="BTCUSDT",
            ctx=ctx
        )
    
    def test_on_bar_closed_no_health_callback(self):
        """Test bar close without health callback doesn't crash."""
        bar = SimpleNamespace(ts_close=1060000)
        ctx = SimpleNamespace(ts=1000000)
        
        self.data_processor.build_signal_ctx.return_value = ctx
        # No health callback set
        
        # Should not crash
        result = self.coordinator.on_bar_closed(bar)
        self.assertIsNotNone(result)
    
    def test_on_bar_closed_fallback_timestamp(self):
        """Test timestamp fallback when ts_close missing."""
        bar = SimpleNamespace(ts_open=1000000)  # No ts_close
        ctx = SimpleNamespace(ts=0)
        
        self.data_processor.build_signal_ctx.return_value = ctx
        
        # Execute
        result = self.coordinator.on_bar_closed(bar)
        
        # Should use ts_open + 60000
        self.assertEqual(result.ts, 1060000)
    
    def test_on_bar_closed_pivots_failure(self):
        """Test handling of pivot retrieval failure."""
        bar = SimpleNamespace(ts_close=1060000)
        ctx = SimpleNamespace(ts=0)
        
        self.cache_service.ensure_pivots_bundle.side_effect = Exception("Redis down")
        self.cache_service.get_pivots_bundle.return_value = None
        self.data_processor.build_signal_ctx.return_value = ctx
        
        # Should not crash
        result = self.coordinator.on_bar_closed(bar)
        self.assertIsNotNone(result)
        self.data_processor.build_signal_ctx.assert_called_once_with(pivots=None)
    
    def test_on_bar_closed_build_ctx_failure(self):
        """Test handling of context building failure."""
        bar = SimpleNamespace(ts_close=1060000)
        
        self.data_processor.build_signal_ctx.side_effect = Exception("Build failed")
        
        # Should return None and log warning
        result = self.coordinator.on_bar_closed(bar)
        
        self.assertIsNone(result)
        self.logger.warning.assert_called_once()
    
    def test_on_bar_closed_health_callback_failure(self):
        """Test that health callback failure doesn't crash pipeline."""
        bar = SimpleNamespace(ts_close=1060000)
        ctx = SimpleNamespace(ts=0)
        health_callback = Mock(side_effect=Exception("Health failed"))
        
        self.data_processor.build_signal_ctx.return_value = ctx
        self.coordinator.set_health_callback(health_callback)
        
        # Should not crash
        result = self.coordinator.on_bar_closed(bar)
        self.assertIsNotNone(result)
        self.logger.debug.assert_called()
    
    def test_on_bar_closed_no_health_metrics(self):
        """Test bar close when health_metrics is None."""
        coordinator = EventCoordinator(
            symbol="BTCUSDT",
            data_processor=self.data_processor,
            cache_service=self.cache_service,
            session_service=self.session_service,
            calibration_service=self.calibration_service,
            health_metrics=None,  # No health metrics
        )
        
        bar = SimpleNamespace(ts_close=1060000)
        ctx = SimpleNamespace(ts=0)
        self.data_processor.build_signal_ctx.return_value = ctx
        
        # Should work fine without health metrics
        result = coordinator.on_bar_closed(bar)
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
