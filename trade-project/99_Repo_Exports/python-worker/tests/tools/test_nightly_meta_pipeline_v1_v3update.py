
import sys
import unittest
from unittest.mock import patch

from tools import nightly_meta_pipeline_v1


class TestNightlyMetaPipelineV3Dispatch(unittest.TestCase):
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
    def test_pipeline_v3_preferred(self, mock_exists, mock_path, mock_run_cmd, mock_subprocess_run):
        """Test that v3 modules are chosen when they exist."""
        # Setup mocks: v3 exists
        mock_exists.side_effect = lambda x: True if "v3" in x else False
        mock_path.return_value.exists.return_value = True
        mock_subprocess_run.return_value.returncode = 0

        with patch.object(sys, 'argv', self.test_args):
            nightly_meta_pipeline_v1.main()

        calls = [c[0][0] for c in mock_run_cmd.call_args_list]

        # Report v3
        report_cmd = next((cmd for cmd in calls if "meta_model_quality_report" in " ".join(cmd)), None)
        self.assertIsNotNone(report_cmd, "Report command not found")
        cmd_str = " ".join(report_cmd)
        self.assertIn("meta_model_quality_report_v3", cmd_str)
        self.assertIn("--group-cols", cmd_str)
        self.assertIn("--min-group-n", cmd_str)

    @patch("tools.nightly_meta_pipeline_v1.subprocess.run")
    @patch("tools.nightly_meta_pipeline_v1.run_cmd")
    @patch("tools.nightly_meta_pipeline_v1.Path")
    @patch("tools.nightly_meta_pipeline_v1._module_exists")
    def test_pipeline_v3_fallback_to_v2(self, mock_exists, mock_path, mock_run_cmd, mock_subprocess_run):
        """Test fallback to v2 if v3 is missing."""
        # v3 missing, v2 exists
        def exists_side_effect(x):
            if "v3" in x: return False
            if "v2" in x: return True
            return False
        mock_exists.side_effect = exists_side_effect
        mock_path.return_value.exists.return_value = True
        mock_subprocess_run.return_value.returncode = 0

        with patch.object(sys, 'argv', self.test_args):
            nightly_meta_pipeline_v1.main()

        calls = [c[0][0] for c in mock_run_cmd.call_args_list]

        # Report v2
        report_cmd = next((cmd for cmd in calls if "meta_model_quality_report" in " ".join(cmd)), None)
        self.assertIsNotNone(report_cmd)
        cmd_str = " ".join(report_cmd)
        self.assertIn("meta_model_quality_report_v2", cmd_str)
        self.assertIn("--group-cols", cmd_str)

    @patch("tools.nightly_meta_pipeline_v1.subprocess.run")
    @patch("tools.nightly_meta_pipeline_v1.run_cmd")
    @patch("tools.nightly_meta_pipeline_v1.Path")
    @patch("tools.nightly_meta_pipeline_v1._module_exists")
    def test_pipeline_fallback_to_v1(self, mock_exists, mock_path, mock_run_cmd, mock_subprocess_run):
        """Test fallback to v1 if v3 and v2 missing."""
        mock_exists.return_value = False
        mock_path.return_value.exists.return_value = True
        mock_subprocess_run.return_value.returncode = 0

        with patch.object(sys, 'argv', self.test_args):
            nightly_meta_pipeline_v1.main()

        calls = [c[0][0] for c in mock_run_cmd.call_args_list]

        # Report v1
        report_cmd = next((cmd for cmd in calls if "meta_model_quality_report" in " ".join(cmd)), None)
        self.assertIsNotNone(report_cmd)
        cmd_str = " ".join(report_cmd)
        self.assertIn("meta_model_quality_report_v1", cmd_str)
        self.assertNotIn("--group-cols", cmd_str)

if __name__ == "__main__":
    unittest.main()
