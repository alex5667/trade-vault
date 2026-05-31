"""
Unit tests for DeliveryHelpers.

Tests delivery utility methods extracted from SignalDispatcher.
"""

import unittest
from unittest.mock import Mock, patch
import json
import time

from services.dispatcher.delivery_helpers import DeliveryHelpers


class TestDeliveryHelpers(unittest.TestCase):
    """Test suite for DeliveryHelpers."""
    
    def test_marker_key(self):
        """Test marker key generation."""
        key = DeliveryHelpers.marker_key("signal:deliver:v2", "notify", "sid123")
        self.assertEqual(key, "signal:deliver:v2:notify:sid123")
    
    def test_delivery_key(self):
        """Test delivery key generation (alias for marker_key)."""
        key = DeliveryHelpers.delivery_key("signal:deliver:v2", "stream", "sid456")
        self.assertEqual(key, "signal:deliver:v2:stream:sid456")
    
    def test_retry_dedup_key(self):
        """Test retry dedup key generation."""
        key = DeliveryHelpers.retry_dedup_key("retry:scheduled", "target", "sid789")
        self.assertEqual(key, "retry:scheduled:target:sid789")
    
    def test_calculate_retry_delay_first_attempt(self):
        """Test retry delay calculation for first attempt."""
        delay = DeliveryHelpers.calculate_retry_delay(0, base_ms=250, max_ms=15000, jitter_ms=250)
        # First attempt: base_ms * 2^0 = 250, plus jitter 0-250
        self.assertGreaterEqual(delay, 250)
        self.assertLessEqual(delay, 500)
    
    def test_calculate_retry_delay_exponential_backoff(self):
        """Test exponential backoff."""
        delay1 = DeliveryHelpers.calculate_retry_delay(1, base_ms=250, max_ms=15000, jitter_ms=0)
        delay2 = DeliveryHelpers.calculate_retry_delay(2, base_ms=250, max_ms=15000, jitter_ms=0)
        
        self.assertEqual(delay1, 500)   # 250 * 2^1
        self.assertEqual(delay2, 1000)  # 250 * 2^2
    
    def test_calculate_retry_delay_max_cap(self):
        """Test retry delay caps at max_ms."""
        delay = DeliveryHelpers.calculate_retry_delay(10, base_ms=250, max_ms=15000, jitter_ms=0)
        self.assertEqual(delay, 15000)  # Capped at max_ms
    
    def test_send_to_dlq_success(self):
        """Test successful DLQ write."""
        redis = Mock()
        redis.xadd = Mock(return_value="1234-0")
        
        result = DeliveryHelpers.send_to_dlq(
            redis_client=redis,
            dlq_stream="dlq:test",
            target="notify",
            sid="sid123",
            env={"foo": "bar"},
            reason="test_failure",
            error="Test error"
        )
        
        self.assertTrue(result)
        redis.xadd.assert_called_once()
        
        # Verify payload structure
        call_args = redis.xadd.call_args
        self.assertEqual(call_args[0][0], "dlq:test")
        
        payload_json = call_args[0][1]["data"]
        payload = json.loads(payload_json)
        
        self.assertEqual(payload["target"], "notify")
        self.assertEqual(payload["sid"], "sid123")
        self.assertEqual(payload["reason"], "test_failure")
        self.assertEqual(payload["error"], "Test error")
        self.assertEqual(payload["env"], {"foo": "bar"})
        self.assertIn("ts", payload)
    
    def test_send_to_dlq_failure(self):
        """Test DLQ write failure handling."""
        redis = Mock()
        redis.xadd = Mock(side_effect=Exception("Redis error"))
        logger = Mock()
        
        result = DeliveryHelpers.send_to_dlq(
            redis_client=redis,
            dlq_stream="dlq:test",
            target="notify",
            sid="sid123",
            env={},
            reason="test",
            error="error",
            logger=logger
        )
        
        self.assertFalse(result)
        logger.error.assert_called_once()
    
    def test_get_dlq_stream_for_target_notify(self):
        """Test DLQ stream selection for notify target."""
        stream = DeliveryHelpers.get_dlq_stream_for_target(
            target="notify",
            dlq_notify="dlq:notify",
            dlq_signal_stream="dlq:signal",
            dlq_audit="dlq:audit",
            dlq_manual="dlq:manual",
            dlq_snapshot="dlq:snapshot",
            dlq_default="dlq:default"
        )
        self.assertEqual(stream, "dlq:notify")
    
    def test_get_dlq_stream_for_target_signal_stream(self):
        """Test DLQ stream selection for signal_stream target."""
        stream = DeliveryHelpers.get_dlq_stream_for_target(
            target="signal_stream",
            dlq_notify="dlq:notify",
            dlq_signal_stream="dlq:signal",
            dlq_audit="dlq:audit",
            dlq_manual="dlq:manual",
            dlq_snapshot="dlq:snapshot",
            dlq_default="dlq:default"
        )
        self.assertEqual(stream, "dlq:signal")
    
    def test_get_dlq_stream_for_target_unknown(self):
        """Test DLQ stream selection for unknown target."""
        stream = DeliveryHelpers.get_dlq_stream_for_target(
            target="unknown_target",
            dlq_notify="dlq:notify",
            dlq_signal_stream="dlq:signal",
            dlq_audit="dlq:audit",
            dlq_manual="dlq:manual",
            dlq_snapshot="dlq:snapshot",
            dlq_default="dlq:default"
        )
        self.assertEqual(stream, "dlq:default")


if __name__ == "__main__":
    unittest.main()
