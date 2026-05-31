"""
Unit tests for SignalDispatcherConfig.

Tests configuration loading from environment variables.
"""

import unittest
import os
from unittest.mock import patch

from services.dispatcher.config import SignalDispatcherConfig


class TestSignalDispatcherConfig(unittest.TestCase):
    """Test suite for SignalDispatcherConfig."""
    
    def test_default_values(self):
        """Test that default values are loaded correctly."""
        # Clear potentially set ENV variables to test true defaults
        with patch.dict(os.environ, {}, clear=True):
            config = SignalDispatcherConfig.from_env()
            
            # Test a few critical defaults
            self.assertEqual(config.outbox_stream, "stream:signals:outbox")
            self.assertEqual(config.dlq_stream, "stream:signals:dlq")
            self.assertEqual(config.group, "signals-outbox-group")
            self.assertEqual(config.read_count, 200)
            self.assertEqual(config.done_ttl_sec, 86400)
    
    def test_env_override(self):
        """Test that environment variables override defaults."""
        with patch.dict(os.environ, {
            'SIGNAL_OUTBOX_STREAM': 'custom:stream',
            'SIGNAL_OUTBOX_READ_COUNT': '500',
            'SIGNAL_OUTBOX_DONE_TTL_SEC': '3600'  # Correct ENV name
        }):
            config = SignalDispatcherConfig.from_env()
            
            self.assertEqual(config.outbox_stream, 'custom:stream')
            self.assertEqual(config.read_count, 500)
            self.assertEqual(config.done_ttl_sec, 3600)
    
    def test_boolean_parsing(self):
        """Test boolean environment variable parsing."""
        with patch.dict(os.environ, {
            'DECISION_TRACE_STORE_ENABLED': '0'
        }):
            config = SignalDispatcherConfig.from_env()
            self.assertFalse(config.trace_store_enabled)
        
        with patch.dict(os.environ, {
            'DECISION_TRACE_STORE_ENABLED': '1'
        }):
            config = SignalDispatcherConfig.from_env()
            self.assertTrue(config.trace_store_enabled)
    
    def test_consumer_includes_pid(self):
        """Test that consumer name includes process ID."""
        config = SignalDispatcherConfig.from_env()
        
        # Should contain the PID
        self.assertIn(str(os.getpid()), config.consumer)
        self.assertIn(str(os.getpid()), config.retry_consumer)
    
    def test_int_parsing(self):
        """Test integer environment variable parsing."""
        with patch.dict(os.environ, {
            'SIGNAL_OUTBOX_READ_COUNT': '123',
            'SIGNAL_RETRY_BASE_MS': '500',
            'SIGNAL_MAX_ATTEMPTS': '5'
        }):
            config = SignalDispatcherConfig.from_env()
            
            self.assertEqual(config.read_count, 123)
            self.assertEqual(config.retry_base_ms, 500)
            self.assertEqual(config.max_attempts, 5)
    
    def test_float_parsing(self):
        """Test float environment variable parsing."""
        with patch.dict(os.environ, {
            'SIGNAL_DIAG_SAMPLE': '0.1',
            'DECISION_TRACE_LOG_SAMPLE_RATE': '0.05'
        }):
            config = SignalDispatcherConfig.from_env()
            
            self.assertEqual(config.diag_sample, 0.1)
            self.assertEqual(config.trace_log_sample_rate, 0.05)
    
    def test_all_dlq_streams_configurable(self):
        """Test that all DLQ streams are independently configurable."""
        with patch.dict(os.environ, {
            'SIGNAL_DLQ_STREAM': 'dlq:default',
            'SIGNAL_DLQ_NOTIFY_STREAM': 'dlq:notify',
            'SIGNAL_DLQ_SIGNAL_STREAM': 'dlq:signal',
            'SIGNAL_DLQ_AUDIT_STREAM': 'dlq:audit',
            'SIGNAL_DLQ_MANUAL_STREAM': 'dlq:manual',
            'SIGNAL_DLQ_SNAPSHOT_STREAM': 'dlq:snapshot'
        }):
            config = SignalDispatcherConfig.from_env()
            
            self.assertEqual(config.dlq_stream, 'dlq:default')
            self.assertEqual(config.dlq_notify, 'dlq:notify')
            self.assertEqual(config.dlq_signal_stream, 'dlq:signal')
            self.assertEqual(config.dlq_audit, 'dlq:audit')
            self.assertEqual(config.dlq_manual, 'dlq:manual')
            self.assertEqual(config.dlq_snapshot, 'dlq:snapshot')


if __name__ == "__main__":
    unittest.main()
