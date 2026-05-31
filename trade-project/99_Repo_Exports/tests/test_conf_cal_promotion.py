
import unittest
import sys
import os
import json
from unittest.mock import MagicMock, patch, mock_open

# Mocking psycopg2 before import
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()

# Adjust path to import the script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../ml_analysis/tools")))

# Import the module under test
import conf_cal_promotion_manager_v1 as promoter

class TestConfCalPromotion(unittest.TestCase):
    
    def setUp(self):
        self.dsn = "postgres://user:pass@localhost:5432/db"
        
    def test_compute_metrics(self):
        y_true = [0, 1, 1, 0, 1]
        y_prob = [0.2, 0.8, 0.9, 0.4, 0.6]
        
        m = promoter.compute_metrics(y_true, y_prob)
        self.assertEqual(m["n"], 5)
        self.assertLess(m["brier"], 0.25)
        self.assertTrue(0 <= m["ece"] <= 1)
        self.assertTrue(0 <= m["precision_top5p"] <= 1)

    @patch("conf_cal_promotion_manager_v1.fetch_recent_signals")
    @patch("os.path.exists")
    @patch("os.getenv")
    def test_promotion_logic_improvement(self, mock_getenv, mock_exists, mock_fetch):
        env = {
            "SIGNALS_PG_DSN": self.dsn,
            "CONF_CAL_CANDIDATE_BUNDLE_PATH": "/tmp/candidate.json",
            "CONF_CAL_CHAMPION_BUNDLE_PATH": "/tmp/champion.json",
            "CONF_CAL_PROOF_STATE_PATH": "/tmp/proof.json",
            "CONF_CAL_PROMOTION_STATUS_PATH": "/tmp/status.json",
            "CONF_CAL_PROMO_MIN_DELTA_ECE": "0.001"
        }
        mock_getenv.side_effect = lambda k, d=None: env.get(k, d)
        mock_exists.return_value = True
        
        cand_bundle = {"version": "v2", "buckets": {"global": {"method": "identity"}}}
        champ_bundle = {"version": "v1", "buckets": {"global": {"method": "identity"}}}
        
        files_content = {
            "/tmp/candidate.json": json.dumps(cand_bundle),
            "/tmp/champion.json": json.dumps(champ_bundle)
        }
        
        def open_side_effect(file, mode="r", *args, **kwargs):
            if file in files_content and "r" in mode:
                # Return a NEW mock_open for each file to avoid state carryover
                return mock_open(read_data=files_content[file])(file, mode, *args, **kwargs)
            # For other files (like status/proof on write), just return a mock
            return mock_open()(file, mode, *args, **kwargs)
            
        data = [{"raw_conf": 0.1, "label": 0, "context": {}}, {"raw_conf": 0.9, "label": 1, "context": {}}] * 200
        mock_fetch.return_value = data
        
        with patch("builtins.open", side_effect=open_side_effect):
            with patch("os.environ", {"CONF_CAL_PROOF_MIN_N_24H": "10"}):
                with patch("os.rename"), patch("shutil.copy2") as mock_copy, patch("sys.argv", ["script"]):
                    promoter.main()
                    mock_copy.assert_not_called()

    @patch("conf_cal_promotion_manager_v1.fetch_recent_signals")
    @patch("os.path.exists")
    @patch("os.getenv")
    def test_promotion_success(self, mock_getenv, mock_exists, mock_fetch):
        env = {
            "SIGNALS_PG_DSN": self.dsn,
            "CONF_CAL_CANDIDATE_BUNDLE_PATH": "/tmp/candidate.json",
            "CONF_CAL_CHAMPION_BUNDLE_PATH": "/tmp/champion.json",
            "CONF_CAL_PROMO_MIN_DELTA_ECE": "0.01"
        }
        mock_getenv.side_effect = lambda k, d=None: env.get(k, d)
        mock_exists.return_value = True
        
        with patch("conf_cal_promotion_manager_v1.compute_metrics") as mock_metrics:
            mock_metrics.side_effect = [
                {"ece": 0.01, "brier": 0.05, "precision_top5p": 0.8, "n": 500},
                {"ece": 0.10, "brier": 0.20, "precision_top5p": 0.5, "n": 500},
            ]
            mock_fetch.return_value = [{"raw_conf": 0.5, "label": 1, "context": {}}]
            
            # Using side_effect for open to handle binary reads from argparse/gettext if needed, 
            # or just returning mocks for specific files.
            mock_files = {
                "/tmp/candidate.json": json.dumps({"version": "v2", "buckets": {}}),
                "/tmp/champion.json": json.dumps({"version": "v1", "buckets": {}})
            }

            def open_side_effect(file, mode="r", *args, **kwargs):
                if file in mock_files:
                    return mock_open(read_data=mock_files[file])(file, mode, *args, **kwargs)
                if file in ("/tmp/conf_cal_proof_state.json.tmp", "/tmp/conf_cal_promo_status.json"):
                     return mock_open()(file, mode, *args, **kwargs)
                # Fallback to real open for system files (like gettext .mo files)
                return io_open(file, mode, *args, **kwargs)

            # We need to capture the real open before patching
            io_open = open

            with patch("builtins.open", side_effect=open_side_effect):
                with patch("os.rename"), patch("shutil.copy2") as mock_copy, \
                     patch("sys.argv", ["script"]):
                    promoter.main()
                    mock_copy.assert_called()

if __name__ == "__main__":
    unittest.main()
