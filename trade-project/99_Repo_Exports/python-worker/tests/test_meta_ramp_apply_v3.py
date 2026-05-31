import io
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from domain.evidence_keys import MetaKeys
from tools.meta_ramp_apply_v3 import Quality, _min_hold_active, _trend_gate, main


class TestMetaRampApplyV3(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.report_path = Path(self.test_dir.name) / "report.json"
        # Adjusted format: applier uses report['metrics'] if available
        self.report_data = {
            "schema_name": "meta_feat_v5",
            "metrics": {
                "pr_auc": 0.65,
                "ece": 0.05,
                "dq_health_mean": 0.95
            },
            "worst": {
                "worst_pr_auc": 0.60,
                "worst_ece": 0.08
            }
        }
        self.report_path.write_text(json.dumps(self.report_data))

        self.model_path = Path(self.test_dir.name) / "model.json"
        self.model_data = {"schema_name": "meta_feat_v5"}
        self.model_path.write_text(json.dumps(self.model_data))

    def tearDown(self):
        self.test_dir.cleanup()

    def test_min_hold_active(self):
        now = 1000
        self.assertTrue(_min_hold_active(now, 900, 200)) # 100 < 200
        self.assertFalse(_min_hold_active(now, 700, 200)) # 300 > 200
        self.assertFalse(_min_hold_active(now, 0, 200)) # No last change

    def test_trend_gate_bootstrap(self):
        q = Quality(pr_auc=0.6)
        cfg2 = {}
        ok, reason, details, sev, action = _trend_gate(q, cfg2, "m5")
        self.assertTrue(ok)
        self.assertIn("bootstrap", details)

    def test_trend_gate_fail_pr_auc(self):
        q = Quality(pr_auc=0.60) # drop 0.05 vs baseline 0.65
        cfg2 = {
            "meta_ramp_baseline_pr_auc__m5": "0.65",
            "ramp_trend_pr_auc_drop_max__m5": "0.02"
        }
        ok, reason, details, sev, action = _trend_gate(q, cfg2, "m5")
        self.assertFalse(ok)
        self.assertEqual(sev, "severe") # 0.05 > 0.02 * 2.0
        self.assertEqual(action, "decrease")

    @patch("redis.Redis.from_url")
    @patch("tools.meta_ramp_apply_v3.dq_freeze_decision")
    def test_ramp_up(self, mock_dq, mock_redis_from_url):
        # Setup: Pass quality, current share 0.1
        mock_dq.return_value = (False, "", {})
        mock_r = MagicMock()
        mock_redis_from_url.return_value = mock_r
        # No anti-flap block: now=1000, last_change=500, min_hold=200
        mock_r.hgetall.return_value = {
            "meta_enforce_share": "0.10",
            "ramp_share_step_up__meta_feat_v5": "0.05",
            "ramp_pr_auc_min__meta_feat_v5": "0.60",
            "ramp_ece_max__meta_feat_v5": "0.08",
            "meta_ramp_last_change_ts__meta_feat_v5": str(int(time.time()) - 100000)
        }

        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            test_args = [
                "meta_ramp_apply_v3.py",
                "--report-json", str(self.report_path),
                "--apply", "1",
                "--redis-url", "redis://mock:6379/0"
            ]
            with patch("sys.argv", test_args):
                main()

        # Verify ramp up: 0.10 + 0.05 = 0.15
        calls = [c for c in mock_r.hset.call_args_list if c[1].get('name') == "settings:dynamic_cfg" or c[0][0] == "settings:dynamic_cfg"]
        self.assertTrue(len(calls) > 0)
        patch_data = calls[0][1].get("mapping") or calls[0][0][1]

        self.assertAlmostEqual(float(patch_data[MetaKeys.ENFORCE_SHARE]), 0.15)
        self.assertEqual(patch_data["meta_model_mode"], "ENFORCE")

    @patch("redis.Redis.from_url")
    @patch("tools.meta_ramp_apply_v3.dq_freeze_decision")
    def test_antiflap_blocks_increase(self, mock_dq, mock_redis_from_url):
        mock_dq.return_value = (False, "", {})
        mock_r = MagicMock()
        mock_redis_from_url.return_value = mock_r
        # last_change was very recent
        mock_r.hgetall.return_value = {
            "meta_enforce_share": "0.10",
            "ramp_share_step_up__meta_feat_v5": "0.05",
            "meta_ramp_last_change_ts__meta_feat_v5": str(int(time.time()) - 100),
            "ramp_min_hold_s__meta_feat_v5": "3600"
        }

        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            test_args = [
                "meta_ramp_apply_v3.py",
                "--report-json", str(self.report_path),
                "--apply", "1",
                "--redis-url", "redis://mock:6379/0"
            ]
            with patch("sys.argv", test_args):
                main()

        # Should NOT increase
        calls = [c for c in mock_r.hset.call_args_list if c[1].get('name') == "settings:dynamic_cfg" or c[0][0] == "settings:dynamic_cfg"]
        self.assertTrue(len(calls) > 0)
        patch_data = calls[0][1].get("mapping") or calls[0][0][1]
        self.assertAlmostEqual(float(patch_data[MetaKeys.ENFORCE_SHARE]), 0.10)
        self.assertIn("HOLD_MIN_HOLD", str(patch_data["meta_ramp_last_decision"]))

if __name__ == "__main__":
    unittest.main()
