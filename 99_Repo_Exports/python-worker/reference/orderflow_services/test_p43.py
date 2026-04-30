import unittest
import sys
import os
import json
from unittest.mock import MagicMock, patch

# Ensure we can import from current directory
sys.path.append(os.getcwd())

# Try to import modules to test
try:
    from orderflow_services import meta_cov_outcome_guard_v1
    from orderflow_services import nightly_meta_enforce_cov_ops_bundle_v1
    from orderflow_services import meta_cov_rollout_exporter_v1
except ImportError:
    # If standard import fails, try direct file import via util or sys.path hack
    sys.path.append(os.path.join(os.getcwd(), 'orderflow_services'))
    import meta_cov_outcome_guard_v1
    import nightly_meta_enforce_cov_ops_bundle_v1
    import meta_cov_rollout_exporter_v1

class TestMetaCovOutcomeGuard(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        self.patcher = patch('redis.Redis')
        self.mock_redis_cls = self.patcher.start()
        # Configure both constructor and from_url to return our instance
        self.mock_redis_cls.return_value = self.mock_redis
        self.mock_redis_cls.from_url.return_value = self.mock_redis
        
    def tearDown(self):
        self.patcher.stop()

    def test_guard_logic_ok(self):
        # Setup clean state: not stale, last run ok, preflight ok, not quarantined
        self.mock_redis.get.side_effect = lambda k: {
            'settings:dynamic_cfg': json.dumps({
                'meta_cov_ops_last_ts_ms': 1000, # Mock NOW-small
                'meta_cov_ops_last_ok': 1
                'meta_cov_ops_last_preflight_rc': 0
                'meta_cov_ops_last_blocked_reasons': []
                'meta_cov_quarantine_until_ms_a': 0
            })
        }.get(k)
        
        with patch('time.time', return_value=1.1): # 1100ms
             with patch('sys.argv', ['script', '--apply', '0']):
                 meta_cov_outcome_guard_v1.main()
                 # Should not have set any block keys (apply=0)
                 self.mock_redis.set.assert_not_called()

    def test_guard_logic_block_stale(self):
        # Setup stale state
        self.mock_redis.get.side_effect = lambda k: {
            'settings:dynamic_cfg': json.dumps({
                'meta_cov_ops_last_ts_ms': 100, # Very old
                'meta_cov_ops_last_ok': 1
                'meta_cov_ops_last_preflight_rc': 0
            })
        }.get(k)
        
        with patch('time.time', return_value=30000.0): # 30000 * 1000 = 30,000,000 ms > 21,600,000 ms
             with patch('sys.argv', ['script', '--apply', '1']):
                 meta_cov_outcome_guard_v1.main()
                 # Should set block keys
                 # P43: it sets cfg:suggestions:entry_policy:auto_apply_block:meta_cov (and :meta, :ts_ms)
                 self.assertTrue(self.mock_redis.set.called)
                 # Check call args to verify key name contains meta_cov
                 calls = [str(c) for c in self.mock_redis.set.call_args_list]
                 self.assertTrue(any('meta_cov' in c for c in calls))


class TestOpsBundle(unittest.TestCase):
    def test_bundle_execution_flow(self):
        # Verify that bundle calls correct scripts in sequence
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.returncode = 0
            
            # Run bundle
            nightly_meta_enforce_cov_ops_bundle_v1.main()
            
            # Verify calls
            # Expected scripts:
            # 1. meta_cov_ops_validate_v1
            # 2. meta_cov_outcome_guard_v1 (NEW P43)
            # 3. meta_cov_rollout_controller_v1
            # 4. meta_cov_outcome_auto_apply_v1
            # 5. meta_cov_quarantine_monitor_v1
            
            scripts_called = []
            for call in mock_run.call_args_list:
                # call.args[0] is the command list e.g. [python, script_path, ...]
                cmd = call.args[0]
                script = cmd[1] # script path
                scripts_called.append(os.path.basename(script))
            
            self.assertIn('meta_cov_ops_validate_v1.py', scripts_called)
            self.assertIn('meta_cov_outcome_guard_v1.py', scripts_called)
            self.assertIn('meta_cov_rollout_controller_v1.py', scripts_called)

if __name__ == '__main__':
    unittest.main()
