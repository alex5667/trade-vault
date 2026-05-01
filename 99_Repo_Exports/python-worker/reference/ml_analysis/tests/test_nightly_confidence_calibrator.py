import sys
import unittest
import json
from unittest.mock import patch, MagicMock
from ml_analysis.tools.nightly_confidence_calibrator_v1 import main, _guard_ok

class TestNightlyConfidenceCalibrator(unittest.TestCase):
    def test_guard_ok_logic(self):
        rep = {
            "raw": {"ece": 0.1, "brier": 0.05},
            "cal": {"ece": 0.08, "brier": 0.04}
        }
        # ece diff 0.02 >= 0.01, brier diff 0.01 >= 0.005 -> OK
        ok, res = _guard_ok(rep, min_ece_abs=0.01, min_brier_abs=0.005)
        self.assertTrue(ok)
        self.assertTrue(res["passed"])

        # ece diff 0.009 < 0.01 -> FAIL
        ok, res = _guard_ok(rep, min_ece_abs=0.03, min_brier_abs=0.005)
        self.assertFalse(ok)
        self.assertFalse(res["passed"])

    @patch("ml_analysis.tools.nightly_confidence_calibrator_v1.subprocess.run")
    @patch("ml_analysis.tools.nightly_confidence_calibrator_v1._atomic_replace")
    @patch("ml_analysis.tools.nightly_confidence_calibrator_v1.os.makedirs")
    def test_main_orchestration(self, mock_makedirs, mock_replace, mock_run):
        # 1. Mock runs
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="ok", stderr=""), # build
            MagicMock(returncode=0, stdout="ok", stderr=""), # train
            MagicMock(returncode=0, stdout="ok", stderr=""), # report
        ]
        
        # Mock reading reports and saving status
        # We mock json.load specifically for the training report
        # We mock json.dump for the status report and final status
        
        with patch("ml_analysis.tools.nightly_confidence_calibrator_v1.json.load") as mock_json_load:
            # The report from training
            mock_json_load.return_value = {
                "train_report": {
                    "raw": {"ece": 0.1, "brier": 0.05}, 
                    "cal": {"ece": 0.05, "brier": 0.02}
                }
            }
            
            with patch("ml_analysis.tools.nightly_confidence_calibrator_v1.json.dump") as mock_json_dump:
                with patch("builtins.open", MagicMock()):
                    main(["--min_rows", "10"])
        
        # Check calls
        self.assertEqual(mock_run.call_count, 3)
        args_build = mock_run.call_args_list[0][0][0]
        self.assertIn("ml_analysis.tools.build_edge_stack_dataset_from_redis", args_build)
        
        # deployed=True since guard passed (0.05 <= 0.1-0.001 and 0.02 <= 0.05-0.0005)
        self.assertEqual(mock_replace.call_count, 2)

if __name__ == "__main__":
    unittest.main()
