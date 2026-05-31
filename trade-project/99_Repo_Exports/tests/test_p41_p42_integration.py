import unittest
import json
import os
import sys
from unittest.mock import MagicMock, patch

# Mock redis before importing our tools
sys.modules['redis'] = MagicMock()

# Relative imports or path insertion based on where this script is run
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../python-worker')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../tools')))

import meta_cov_ops_validate_v1 as preflight
import services.orderflow.auto_apply_guard as guard

class TestP41P42Integration(unittest.TestCase):

    def test_preflight_parse_entry_nested(self):
        """Test P41 payload merging from nested json/payload."""
        # Case 1: Double-encoded (simulating some redis return scenarios)
        raw_fields_double = {
            b'event': b'POSITION_CLOSED',
            b'payload': json.dumps(json.dumps({
                'meta_enforce_cov_bucket': 'a',
                'meta_enforce_applied': 1,
                'r_mult': 1.5
            })).encode('utf-8')
        }
        parsed = preflight._parse_entry(raw_fields_double)
        self.assertEqual(parsed['meta_enforce_cov_bucket'], 'a')
        self.assertEqual(parsed['meta_enforce_applied'], 1)

        # Case 2: Single-encoded (standard redis)
        raw_fields_single = {
            b'event': b'POSITION_CLOSED',
            b'payload': json.dumps({
                'meta_enforce_cov_bucket': 'b',
                'meta_enforce_applied': 2,
                'r_mult': 2.0
            }).encode('utf-8')
        }
        parsed2 = preflight._parse_entry(raw_fields_single)
        self.assertEqual(parsed2['meta_enforce_cov_bucket'], 'b')
        self.assertEqual(parsed2['meta_enforce_applied'], 2)

    def test_is_position_closed(self):
        """Test trade closure detection."""
        self.assertTrue(preflight._is_position_closed({'event': 'POSITION_CLOSED'}))
        self.assertTrue(preflight._is_position_closed({'status': 'POSITION_CLOSED'}))
        self.assertFalse(preflight._is_position_closed({'event': 'FILL'}))

    @patch('services.orderflow.auto_apply_guard._connect_redis')
    def test_multi_reason_guard_single(self, mock_connect):
        """Test auto_apply_guard with single tick_gate reason."""
        mock_cli = MagicMock()
        mock_connect.return_value = mock_cli
        
        # Mock pipeline results: [block_val, meta_raw, ts_raw]
        mock_cli.pipeline.return_value.execute.return_value = [b"1", '{"blocked": true, "ts_ms": 1000}', "1000"]
        
        with patch.dict(os.environ, {"AUTO_APPLY_BLOCK_REASONS": "tick_gate"}):
            blocked, meta = guard.get_block_state(redis_url="redis://localhost")
            self.assertTrue(blocked)
            self.assertEqual(meta['reason'], 'tick_gate')

    @patch('services.orderflow.auto_apply_guard._connect_redis')
    @patch('services.orderflow.auto_apply_guard._now_ms')
    def test_multi_reason_guard_multiple(self, mock_now, mock_connect):
        """Test auto_apply_guard with multiple reasons, meta_cov blocking."""
        mock_now.return_value = 2000
        mock_cli = MagicMock()
        mock_connect.return_value = mock_cli
        
        # tick_gate (indices 0,1,2) -> no block
        # meta_cov (indices 3,4,5) -> soft block via fresh meta
        mock_cli.pipeline.return_value.execute.return_value = [
            None, None, None, # tick_gate
            None, '{"blocked": true, "reason": "stale"}', "1950" # meta_cov
        ]
        
        with patch.dict(os.environ, {"AUTO_APPLY_BLOCK_REASONS": "tick_gate,meta_cov"}):
            blocked, meta = guard.get_block_state(redis_url="redis://localhost", max_meta_age_ms=1000)
            self.assertTrue(blocked)
            self.assertEqual(meta['reason'], 'meta_cov')

if __name__ == '__main__':
    unittest.main()
