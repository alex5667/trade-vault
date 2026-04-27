import unittest
from unittest.mock import patch, MagicMock
import sys
import os
from tools import nightly_meta_pipeline_v1

class TestNightlyMetaPipelineV1(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_args = [
            "tools.nightly_meta_pipeline_v1",
            "--in-parquet", "test_input.parquet",
            "--out-model-json", "model.json",
            "--out-report-json", "report.json",
            "--label-col", "y",
            "--apply-ramp",
        ]

    @patch("tools.nightly_meta_pipeline_v1.subprocess.run")
    @patch("tools.nightly_meta_pipeline_v1.run_cmd")
    @patch("tools.nightly_meta_pipeline_v1.Path")
    @patch("tools.nightly_meta_pipeline_v1._module_exists")
    def test_pipeline_v2_preferred(self, mock_exists, mock_path, mock_run_cmd, mock_subprocess_run):
        """Test that v2 modules are chosen when they exist."""
        # Setup mocks
        mock_exists.side_effect = lambda x: True if "v2" in x else False
        mock_path.return_value.exists.return_value = True # File exists checks pass
        mock_subprocess_run.return_value.returncode = 0

        with patch.object(sys, 'argv', self.test_args):
            nightly_meta_pipeline_v1.main()

        # Check Calls to run_cmd (Train, Report)
        calls = [c[0][0] for c in mock_run_cmd.call_args_list]
        
        # 1. Train (v4 fallback/default)
        self.assertTrue(any("train_meta_model_lr_v4" in " ".join(cmd) for cmd in calls))
        
        # 2. Report v2
        report_cmd = next((cmd for cmd in calls if "meta_model_quality_report" in " ".join(cmd)), None)
        self.assertIsNotNone(report_cmd)
        cmd_str = " ".join(report_cmd)
        self.assertIn("meta_model_quality_report_v2", cmd_str)
        self.assertIn("--group-cols", cmd_str)
        self.assertIn("--min-group-n", cmd_str)

        # 3. Ramp v2 - This is called via subprocess.run directly, NOT run_cmd
        subprocess_calls = [c[0][0] for c in mock_subprocess_run.call_args_list]
        ramp_cmd = next((cmd for cmd in subprocess_calls if "meta_auto_ramp" in " ".join(cmd)), None)
        self.assertIsNotNone(ramp_cmd)
        self.assertIn("meta_auto_ramp_v2", " ".join(ramp_cmd))

    @patch("tools.nightly_meta_pipeline_v1.subprocess.run")
    @patch("tools.nightly_meta_pipeline_v1.run_cmd")
    @patch("tools.nightly_meta_pipeline_v1.Path")
    @patch("tools.nightly_meta_pipeline_v1._module_exists")
    def test_pipeline_v1_fallback(self, mock_exists, mock_path, mock_run_cmd, mock_subprocess_run):
        """Test that v1 modules are chosen when v2 is missing."""
        # Setup mocks: v2 files do NOT exist
        mock_exists.return_value = False
        mock_path.return_value.exists.return_value = True # File exists checks pass
        mock_subprocess_run.return_value.returncode = 0

        with patch.object(sys, 'argv', self.test_args):
            nightly_meta_pipeline_v1.main()

        calls = [c[0][0] for c in mock_run_cmd.call_args_list]

        # 2. Report v1
        report_cmd = next((cmd for cmd in calls if "meta_model_quality_report" in " ".join(cmd)), None)
        self.assertIsNotNone(report_cmd)
        cmd_str = " ".join(report_cmd)
        self.assertIn("meta_model_quality_report_v1", cmd_str)
        self.assertNotIn("--group-cols", cmd_str) # v1 shouldn't have new args

        # 3. Ramp v1 - Called via subprocess.run directly
        subprocess_calls = [c[0][0] for c in mock_subprocess_run.call_args_list]
        ramp_cmd = next((cmd for cmd in subprocess_calls if "meta_auto_ramp" in " ".join(cmd)), None)
        self.assertIsNotNone(ramp_cmd)
        self.assertIn("meta_auto_ramp_v1", " ".join(ramp_cmd))

    def test_module_exists_logic(self):
        """Test the helper function logic roughly (though hard to test purely with rel paths)."""
        # We can just check that it returns bool
        self.assertIsInstance(nightly_meta_pipeline_v1._module_exists("random_file.py"), bool)

if __name__ == "__main__":
    unittest.main()
