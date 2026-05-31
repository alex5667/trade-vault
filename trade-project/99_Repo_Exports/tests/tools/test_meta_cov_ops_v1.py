import json
import os
import unittest
from unittest.mock import MagicMock, patch

from tools import meta_cov_quarantine_monitor_v1
from tools import nightly_meta_enforce_cov_ops_bundle_v1


class TestMetaCovOpsBundle(unittest.TestCase):
    @patch("tools.nightly_meta_enforce_cov_ops_bundle_v1.run_command")
    @patch("os.path.exists")
    def test_bundle_apply(self, mock_exists, mock_run):
        # Mock tools exist
        mock_exists.return_value = True
        mock_run.return_value = 0

        # Run with --apply
        with patch("sys.argv", ["script", "--apply", "--emit-metrics"]):
            nightly_meta_enforce_cov_ops_bundle_v1.main()
        
        # Verify calls
        # 1. Rollout
        args_rollout = mock_run.call_args_list[0][0][0]
        self.assertIn("--apply", args_rollout)
        self.assertIn("1", args_rollout)
        
        # 2. Outcome (with --apply 1)
        self.assertEqual(mock_run.call_count, 3)
        args_outcome = mock_run.call_args_list[1][0][0]
        self.assertIn("--apply", args_outcome)
        self.assertIn("1", args_outcome)
        # Verify emit-metrics NOT passed to outcome tool
        self.assertNotIn("--emit-metrics", args_outcome)

    @patch("tools.nightly_meta_enforce_cov_ops_bundle_v1.run_command")
    @patch("os.path.exists")
    @patch.dict(os.environ, {"META_COV_BUNDLE_APPLY": "0"})
    def test_bundle_force_dry_run_via_env(self, mock_exists, mock_run):
        mock_exists.return_value = True
        mock_run.return_value = 0
        
        # Even if --apply is passed
        with patch("sys.argv", ["script", "--apply"]):
            nightly_meta_enforce_cov_ops_bundle_v1.main()
            
        # Should call with --apply 0
        args_outcome = mock_run.call_args_list[1][0][0]
        self.assertIn("--apply", args_outcome)
        self.assertIn("0", args_outcome)
        # Should call monitor with --dry-run
        args_monitor = mock_run.call_args_list[2][0][0]
        self.assertIn("--dry-run", args_monitor)


class TestMetaCovQuarantineMonitor(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        
    @patch("tools.meta_cov_quarantine_monitor_v1.get_redis_client")
    def test_no_violations(self, mock_get_client):
        mock_get_client.return_value = self.mock_redis
        # Setup cfg2: clean
        self.mock_redis.hgetall.return_value = {
            b"meta_cov_quarantine_a": b"0",
            b"meta_enforce_share_cov_a": b"0.1",
            b"meta_cov_outcome_last_apply_ms": str(int(meta_cov_quarantine_monitor_v1.time.time()*1000)).encode()
        }
        
        with patch("tools.meta_cov_quarantine_monitor_v1.logger") as mock_logger:
            with patch("sys.argv", ["script"]):
                meta_cov_quarantine_monitor_v1.main()
            
            # Should log info
            mock_logger.info.assert_any_call("No quarantine violations found.")
            # No notifications
            self.mock_redis.xadd.assert_not_called()

    @patch("tools.meta_cov_quarantine_monitor_v1.get_redis_client")
    def test_violation_quarantined_but_share_nonzero(self, mock_get_client):
        mock_get_client.return_value = self.mock_redis
        # Setup cfg2: Bucket A quarantined but share > 0
        self.mock_redis.hgetall.return_value = {
            b"meta_cov_quarantine_a": b"1",
            b"meta_enforce_share_cov_a": b"0.5",  # VIOLATION
            b"meta_cov_quarantine_until_ms_a": b"9999999999999" # Future
        }

        with patch("tools.meta_cov_quarantine_monitor_v1.logger") as mock_logger:
            with patch("sys.argv", ["script", "--notify"]):
                meta_cov_quarantine_monitor_v1.main()
                
            # Should log error
            args, _ = mock_logger.error.call_args
            self.assertIn("Bucket a: QUARANTINED but share=0.50 > 0!", args[0])
            # Should notify
            self.mock_redis.xadd.assert_called()

    @patch("tools.meta_cov_quarantine_monitor_v1.get_redis_client")
    def test_violation_expired_quarantine(self, mock_get_client):
        mock_get_client.return_value = self.mock_redis
        # Setup cfg2: Bucket B quarantined, share 0 (ok), but expired
        now = int(meta_cov_quarantine_monitor_v1.time.time() * 1000)
        expired = now - 10000
        self.mock_redis.hgetall.return_value = {
            b"meta_cov_quarantine_b": b"1",
            b"meta_enforce_share_cov_b": b"0",
            b"meta_cov_quarantine_until_ms_b": str(expired).encode() # Expired
        }

        with patch("tools.meta_cov_quarantine_monitor_v1.logger") as mock_logger:
            with patch("sys.argv", ["script", "--notify"]):
                meta_cov_quarantine_monitor_v1.main()

            args, _ = mock_logger.error.call_args
            self.assertIn(f"Bucket b: Quarantine EXPIRED (until {expired}) but active=1.", args[0])

if __name__ == "__main__":
    unittest.main()
