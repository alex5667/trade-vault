"""
Unit tests for HealthMetricsTracker.

Tests health metrics collection in isolation.
"""

import time
import unittest
from unittest.mock import Mock, MagicMock

from handlers.metrics.health_tracker import HealthMetricsTracker


class TestHealthMetricsTracker(unittest.TestCase):
    """Test suite for HealthMetricsTracker."""
    
    def setUp(self):
        """Create fresh tracker for each test."""
        self.redis_mock = MagicMock()
        self.tracker = HealthMetricsTracker("BTCUSDT", self.redis_mock)
    
    def test_initial_state(self):
        """Test that tracker starts with zero metrics."""
        metrics = self.tracker.get_metrics()
        
        self.assertEqual(metrics["symbol"], "BTCUSDT")
        self.assertEqual(metrics["total_ticks"], 0)
        self.assertEqual(metrics["error_count"], 0)
        self.assertEqual(metrics["error_rate"], 0.0)
        self.assertNotIn("tick_latency_ms", metrics)
        self.assertNotIn("l2_freshness_ms", metrics)
    
    def test_record_tick_latency(self):
        """Test recording tick latencies."""
        self.tracker.record_tick_latency(10.5)
        self.tracker.record_tick_latency(15.2)
        self.tracker.record_tick_latency(12.8)
        
        metrics = self.tracker.get_metrics()
        
        self.assertEqual(metrics["total_ticks"], 3)
        self.assertIn("tick_latency_ms", metrics)
        self.assertAlmostEqual(metrics["tick_latency_ms"]["avg"], 12.833, places=2)
        self.assertEqual(metrics["tick_latency_ms"]["min"], 10.5)
        self.assertEqual(metrics["tick_latency_ms"]["max"], 15.2)
    
    def test_record_l2_freshness(self):
        """Test recording L2 freshness."""
        self.tracker.record_l2_freshness(100.0)
        self.tracker.record_l2_freshness(200.0)
        self.tracker.record_l2_freshness(150.0)
        
        metrics = self.tracker.get_metrics()
        
        self.assertIn("l2_freshness_ms", metrics)
        self.assertAlmostEqual(metrics["l2_freshness_ms"]["avg"], 150.0)
        self.assertEqual(metrics["l2_freshness_ms"]["min"], 100.0)
        self.assertEqual(metrics["l2_freshness_ms"]["max"], 200.0)
    
    def test_record_error(self):
        """Test error tracking."""
        self.tracker.record_tick_latency(10.0)
        self.tracker.record_tick_latency(10.0)
        self.tracker.record_error()
        
        metrics = self.tracker.get_metrics()
        
        self.assertEqual(metrics["error_count"], 1)
        self.assertEqual(metrics["error_rate"], 0.5)  # 1 error / 2 ticks
    
    def test_percentile_calculation(self):
        """Test percentile calculations."""
        # Add 100 values from 1 to 100
        for i in range(1, 101):
            self.tracker.record_tick_latency(float(i))
        
        metrics = self.tracker.get_metrics()
        latency = metrics["tick_latency_ms"]
        
        self.assertEqual(latency["min"], 1.0)
        self.assertEqual(latency["max"], 100.0)
        self.assertAlmostEqual(latency["p50"], 50.0, delta=1.0)
        self.assertAlmostEqual(latency["p95"], 95.0, delta=1.0)
    
    def test_deque_max_length(self):
        """Test that deque respects maxlen."""
        # Add more than 100 values
        for i in range(150):
            self.tracker.record_tick_latency(float(i))
        
        metrics = self.tracker.get_metrics()
        
        # Should only have last 100 values (50-149)
        self.assertEqual(metrics["tick_latency_ms"]["min"], 50.0)
        self.assertEqual(metrics["tick_latency_ms"]["max"], 149.0)
    
    def test_publish_snapshot_interval(self):
        """Test snapshot publishing respects interval."""
        self.tracker._snapshot_interval_sec = 0.1
        
        # First publish should succeed
        result1 = self.tracker.publish_snapshot()
        self.assertTrue(result1)
        self.redis_mock.setex.assert_called_once()
        
        # Immediate second publish should fail (interval not elapsed)
        result2 = self.tracker.publish_snapshot()
        self.assertFalse(result2)
        self.assertEqual(self.redis_mock.setex.call_count, 1)
        
        # After interval, should succeed
        time.sleep(0.15)
        result3 = self.tracker.publish_snapshot()
        self.assertTrue(result3)
        self.assertEqual(self.redis_mock.setex.call_count, 2)
    
    def test_publish_snapshot_force(self):
        """Test forced snapshot publishing."""
        self.tracker._snapshot_interval_sec = 100.0
        
        # Force publish should work immediately
        result = self.tracker.publish_snapshot(force=True)
        self.assertTrue(result)
        self.redis_mock.setex.assert_called_once()
    
    def test_publish_snapshot_redis_failure(self):
        """Test that Redis failures don't crash tracker."""
        self.redis_mock.setex.side_effect = Exception("Redis down")
        
        # Should return False but not raise
        result = self.tracker.publish_snapshot(force=True)
        self.assertFalse(result)
    
    def test_reset(self):
        """Test resetting metrics."""
        self.tracker.record_tick_latency(10.0)
        self.tracker.record_l2_freshness(100.0)
        self.tracker.record_error()
        
        self.tracker.reset()
        
        metrics = self.tracker.get_metrics()
        self.assertEqual(metrics["total_ticks"], 0)
        self.assertEqual(metrics["error_count"], 0)
        self.assertNotIn("tick_latency_ms", metrics)
        self.assertNotIn("l2_freshness_ms", metrics)
    
    def test_concurrent_updates(self):
        """Test thread-safety of metric updates."""
        import threading
        
        def record_metrics():
            for _ in range(100):
                self.tracker.record_tick_latency(10.0)
                self.tracker.record_error()
        
        threads = [threading.Thread(target=record_metrics) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        metrics = self.tracker.get_metrics()
        # Should have recorded 500 ticks and 500 errors
        self.assertEqual(metrics["total_ticks"], 500)
        self.assertEqual(metrics["error_count"], 500)


if __name__ == "__main__":
    unittest.main()
