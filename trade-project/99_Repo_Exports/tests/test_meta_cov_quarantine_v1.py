import unittest
import json
import time
from unittest.mock import MagicMock, patch
import os
import sys

# Ensure tools are in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../tools")))

# Mock redis before importing tools that might try to connect or import it
with patch.dict(sys.modules, {'redis': MagicMock()}):
    import meta_cov_outcome_auto_apply_v1 as auto_apply
    import apply_meta_enforce_cov_suggestion as applier


class TestMetaCovQuarantine(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        self.mock_redis.pipeline.return_value = self.mock_redis
        
        # Default config
        self.cfg2 = {
            "meta_enforce_share_cov_a": "0.5",
            "meta_cov_quarantine_a": "0",
            "meta_cov_quarantine_until_ms_a": "0"
        }
        self.mock_redis.hgetall.return_value = self.cfg2
        
    def test_quarantine_trigger(self):
        with patch.object(auto_apply, "read_closed_trades") as mock_read:
            with patch.object(auto_apply, "_redis", return_value=self.mock_redis):
                with patch.object(auto_apply, "load_qstate", return_value={"buckets": {}}):
                    with patch.object(auto_apply, "save_qstate"):
                        # Simulate severe tail risk
                        bad_trades = [{"meta_enforce_cov_bucket": "a", "meta_enforce_applied": 1, "r_mult": -1.5} for _ in range(50)]
                        good_trades = [{"meta_enforce_cov_bucket": "a", "meta_enforce_applied": 1, "r_mult": 1.0} for _ in range(50)]
                        ctl_trades = [{"meta_enforce_cov_bucket": "a", "meta_enforce_applied": 0, "r_mult": 0.0} for _ in range(100)]
                        
                        mock_read.return_value = bad_trades + good_trades + ctl_trades
                        
                        # Run
                        with patch.dict(os.environ, {
                            "META_COV_OUTCOME_PANIC_TAIL": "0.45",
                            "META_COV_QUARANTINE_TTL_SEC": "3600",
                            "META_COV_OUTCOME_MIN_N_ENFORCE": "10",
                            "META_COV_OUTCOME_MIN_N_CONTROL": "10"
                        }):
                            with patch("builtins.print") as mock_print:
                                auto_apply.main()
                                
                                # Verify output contains quarantine action
                                args, _ = mock_print.call_args
                                out = json.loads(args[0])
                                
                                self.assertEqual(out["ok"], 1)
                                patch_data = out["patch"]
                                
                                # Check quarantine set
                                self.assertEqual(patch_data.get("meta_cov_quarantine_a"), 1)
                                self.assertEqual(patch_data.get("meta_enforce_share_cov_a"), 0.0)
                                self.assertTrue(patch_data.get("meta_cov_quarantine_until_ms_a") > 0)
                                self.assertEqual(patch_data.get("meta_cov_quarantine_prev_share_a"), 0.5)

    def test_apply_guard_blocks_increase(self):
        with patch.object(applier, "_redis", return_value=self.mock_redis):
            # Setup quarantined state
            now = int(time.time() * 1000)
            future = now + 3600000
            self.cfg2["meta_cov_quarantine_a"] = "1"
            self.cfg2["meta_cov_quarantine_until_ms_a"] = str(future)
            self.cfg2["meta_enforce_share_cov_a"] = "0.0"
            
            # Meta payload trying to increase share WITHOUT clearing quarantine
            meta = {
                "patch": {
                    "meta_enforce_share_cov_a": 0.1
                }
            }
            self.mock_redis.get.side_effect = lambda k: json.dumps(meta) if "meta:" in k else ("sid123" if "latest" in k else None)
            
            # Mock sufficient approvals
            self.mock_redis.scard.return_value = 2
            self.mock_redis.smembers.return_value = {"a1", "a2"}
            
            # Run applier
            with patch("sys.argv", ["prog", "--sid", "sid123"]):
                with patch("builtins.print") as mock_print:
                    applier.main()
                    
                    # Verify blocked
                    args, _ = mock_print.call_args
                    out = json.loads(args[0])
                    
                    self.assertEqual(out["ok"], 0)
                    self.assertEqual(out["reason"], "bucket_quarantined")
                
    def test_apply_guard_allows_clearing(self):
        with patch.object(applier, "_redis", return_value=self.mock_redis):
            # Setup quarantined state
            now = int(time.time() * 1000)
            future = now + 3600000
            self.cfg2["meta_cov_quarantine_a"] = "1"
            self.cfg2["meta_cov_quarantine_until_ms_a"] = str(future)
            
            # Meta payload clearing quarantine AND setting share
            meta = {
                "patch": {
                    "meta_cov_quarantine_a": 0,
                    "meta_cov_quarantine_until_ms_a": 0,
                    "meta_enforce_share_cov_a": 0.1
                }
            }
            self.mock_redis.get.side_effect = lambda k: json.dumps(meta) if "meta:" in k else ("sid123" if "latest" in k else None)
            self.mock_redis.smembers.return_value = {"approver1", "approver2"}
            self.mock_redis.scard.return_value = 2
            
            # Run applier
            with patch.dict(os.environ, {"META_COV_ALLOW_EARLY_UNQUARANTINE": "1"}):
                with patch("sys.argv", ["prog", "--sid", "sid123"]):
                    with patch.object(applier, "_write_cfg2_patch"):
                        with patch("builtins.print") as mock_print:
                            applier.main()
                            # Verify allowed
                            args, _ = mock_print.call_args
                            out = json.loads(args[0])
                            if out["ok"] != 1:
                                print(f"DEBUG REASON: {out.get('reason')}")
                            self.assertEqual(out["ok"], 1, msg=f"Failed: {out.get('reason')}")

    def test_quarantine_release_good_streak(self):
        with patch.object(auto_apply, "read_closed_trades") as mock_read:
            with patch.object(auto_apply, "_redis", return_value=self.mock_redis):
                with patch.object(auto_apply, "load_qstate") as mock_load:
                    with patch.object(auto_apply, "save_qstate"):
                        # Verify quarantine expired
                        now = int(time.time() * 1000)
                        past = now - 1000
                        self.cfg2["meta_cov_quarantine_a"] = "1"
                        self.cfg2["meta_cov_quarantine_until_ms_a"] = str(past)
                        self.cfg2["meta_cov_quarantine_prev_share_a"] = "0.5"
                        
                        # State currently has streak 2, need 3
                        mock_load.return_value = {
                            "buckets": {
                                "a": {"release_streak": 2}
                            }
                        }
                        
                        # Good control trades
                        ctl_trades = [{"meta_enforce_cov_bucket": "a", "meta_enforce_applied": 0, "r_mult": 0.5} for _ in range(50)]
                        mock_read.return_value = ctl_trades
                        
                        # Run
                        with patch.dict(os.environ, {
                            "META_COV_QUARANTINE_GOOD_STREAK_N": "3",
                            "META_COV_QUARANTINE_START_SHARE": "0.02"
                        }):
                            with patch("builtins.print") as mock_print:
                                auto_apply.main()
                                
                                args, _ = mock_print.call_args
                                out = json.loads(args[0])
                                
                                self.assertEqual(out["ok"], 1)
                                patch_data = out["patch"]
                                
                                # Released!
                                self.assertEqual(patch_data.get("meta_cov_quarantine_a"), 0)
                                self.assertEqual(patch_data.get("meta_enforce_share_cov_a"), 0.02)
                                self.assertEqual(patch_data.get("meta_cov_recovery_target_share_a"), 0.5)

if __name__ == "__main__":
    unittest.main()
