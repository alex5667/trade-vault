import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import io

# Adjust path to import the tool
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from tools import meta_cov_ops_validate_v1

class TestMetaCovOpsValidate(unittest.TestCase):
    
    def setUp(self):
        self.mock_redis = MagicMock()
        self.mock_redis_cls = patch('tools.meta_cov_ops_validate_v1.redis.from_url').start()
        self.mock_redis_cls.return_value = self.mock_redis
        
        # Default environment variables
        self.env_patcher = patch.dict(os.environ, {
            "META_COV_SOURCE_STREAM": "metrics:u_test",
            "TRADE_EVENTS_STREAM": "events:u_test",
            "DYN_CFG_KEY": "settings:u_test",
            "META_COV_PREFLIGHT_MIN_OF_GATE": "10",
            "META_COV_PREFLIGHT_MIN_TRADES": "5"
        })
        self.env_patcher.start()

    def tearDown(self):
        patch.stopall()

    def test_main_success(self):
        """Test happy path where everything is OK."""
        # 1. Config exists
        self.mock_redis.exists.return_value = 1
        
        # 2. Source Stream OK
        self.mock_redis.xlen.side_effect = lambda k: 100 if k == "metrics:u_test" else 50
        # Trade stream OK
        
        # xrevrange returns list of (id, fields)
        self.mock_redis.xrevrange.side_effect = [
            [(b'1-0', {'meta_feature_coverage': '1', 'meta_enforce_cov_bucket': '1'})],  # source
            [(b'2-0', {
                'event': 'POSITION_CLOSED',
                'r_mult': '1.5',
                'meta_enforce_cov_bucket': 'a',
                'meta_enforce_applied': '1',
            })],  # trade
        ]

        with patch('sys.stdout', new=io.StringIO()) as mock_stdout:
            ret = meta_cov_ops_validate_v1.main()
            self.assertEqual(ret, 0)
            self.assertIn('"ok": 1', mock_stdout.getvalue())

    def test_config_missing_hard_fail(self):
        """Test hard fail when dynamic config is missing."""
        self.mock_redis.exists.return_value = 0
        
        ret = meta_cov_ops_validate_v1.main()
        self.assertEqual(ret, 1)

    def test_source_stream_missing_fields_soft_block(self):
        """Test soft block when source stream is missing required fields."""
        self.mock_redis.exists.return_value = 1
        self.mock_redis.xlen.return_value = 100
        
        # Source stream missing 'meta_enforce_cov_bucket'
        self.mock_redis.xrevrange.return_value = [
            (b'1-0', {'meta_feature_coverage': '1'}) 
        ]
        
        with patch('sys.stdout', new=io.StringIO()) as mock_stdout:
            ret = meta_cov_ops_validate_v1.main()
            self.assertEqual(ret, 2)
            self.assertIn('"status": "soft-block"', mock_stdout.getvalue())

    def test_trade_stream_insufficient_count_soft_block(self):
        """Test soft block when trade stream length is insufficient."""
        self.mock_redis.exists.return_value = 1
        
        # Source stream OK
        # Trade stream too short
        def xlen_side_effect(key):
            if key == "metrics:u_test":
                return 100
            if key == "events:u_test":
                return 2  # User req min 5
            return 0
        self.mock_redis.xlen.side_effect = xlen_side_effect
        
        # Mock xrevrange for source verification (first call)
        self.mock_redis.xrevrange.return_value = [(b'1-0', {'meta_feature_coverage': '1', 'meta_enforce_cov_bucket': '1'})]
        
        with patch('sys.stdout', new=io.StringIO()) as mock_stdout:
            ret = meta_cov_ops_validate_v1.main()
            self.assertEqual(ret, 2)
            self.assertIn('soft-block', mock_stdout.getvalue())

if __name__ == '__main__':
    unittest.main()
