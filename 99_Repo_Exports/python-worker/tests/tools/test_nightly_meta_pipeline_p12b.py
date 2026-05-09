
import sys
import unittest
from unittest.mock import MagicMock, patch

# Fix path to import the tool
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from tools import nightly_meta_pipeline_v1


class TestNightlyMetaPipelineP12b(unittest.TestCase):
    def setUp(self):
        self.default_args = [
            "nightly_meta_pipeline_v1.py",
            "--in-parquet", "/tmp/dataset.parquet",
            "--out-model-json", "/tmp/model.json",
            "--out-report-json", "/tmp/report.json",
            "--label-col", "y_label",
        ]

    @patch("tools.nightly_meta_pipeline_v1.subprocess.run")
    @patch("tools.nightly_meta_pipeline_v1.Path")
    def test_v2_scripts_exist(self, mock_path_cls, mock_run):
        """Verify that if v2 scripts exist, they are called with v2 arguments."""
        # Side effect for Path constructor
        def side_effect_path(path_str):
            m = MagicMock()
            p = str(path_str)

            # Logic for exists()
            exists_val = False
            # Input files
            if p == "/tmp/dataset.parquet": exists_val = True
            if p == "/tmp/model.json": exists_val = True
            if p == "/tmp/report.json": exists_val = True

            # v2 scripts
            if "meta_model_quality_report_v2.py" in p: exists_val = True
            if "meta_auto_ramp_v2.py" in p: exists_val = True
            if "meta_guardrails_v1.py" in p: exists_val = True

            m.exists.return_value = exists_val
            m.__str__.return_value = p
            return m

        mock_path_cls.side_effect = side_effect_path

        # Run main with ramp and guard enabled
        with patch.object(sys, "argv", self.default_args + ["--apply-ramp", "--apply-guard"]):
            ret = nightly_meta_pipeline_v1.main()
            self.assertEqual(ret, 0)

        # Verify Report v2
        report_called_v2 = False
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            cmd_str = " ".join(cmd)
            if "meta_model_quality_report_v2" in cmd_str:
                report_called_v2 = True
                self.assertIn("--dataset-parquet", cmd)
                self.assertNotIn("--in-parquet", cmd)

        self.assertTrue(report_called_v2, "Report v2 should be called when script exists")

        # Verify Ramp v2
        ramp_called_v2 = False
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            cmd_str = " ".join(cmd)
            if "meta_auto_ramp_v2" in cmd_str:
                ramp_called_v2 = True

        self.assertTrue(ramp_called_v2, "Ramp v2 should be called when script exists")

        # Verify Guard
        guard_called = False
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            cmd_str = " ".join(cmd)
            if "meta_guardrails_v1" in cmd_str:
                guard_called = True

        self.assertTrue(guard_called, "Guardrails should be called when requested and exists")

    @patch("tools.nightly_meta_pipeline_v1.subprocess.run")
    @patch("tools.nightly_meta_pipeline_v1.Path")
    def test_v2_scripts_missing_fallback(self, mock_path_cls, mock_run):
        """Verify that if v2 scripts are missing, v1 scripts are used."""
        # Side effect for Path constructor
        def side_effect_path(path_str):
            m = MagicMock()
            p = str(path_str)

            # Logic for exists()
            exists_val = False
            # Input files
            if p == "/tmp/dataset.parquet": exists_val = True
            if p == "/tmp/model.json": exists_val = True
            if p == "/tmp/report.json": exists_val = True

            # v2 scripts DO NOT exist
            if "meta_model_quality_report_v2.py" in p: exists_val = False
            if "meta_auto_ramp_v2.py" in p: exists_val = False
            if "meta_guardrails_v1.py" in p: exists_val = False

            m.exists.return_value = exists_val
            m.__str__.return_value = p
            return m

        mock_path_cls.side_effect = side_effect_path

        # Run main
        with patch.object(sys, "argv", self.default_args + ["--apply-ramp", "--apply-guard"]):
            ret = nightly_meta_pipeline_v1.main()
            self.assertEqual(ret, 0)

        # Verify Report v1
        report_called_v1 = False
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            cmd_str = " ".join(cmd)
            if "meta_model_quality_report_v1" in cmd_str:
                report_called_v1 = True
                self.assertIn("--in-parquet", cmd)

        self.assertTrue(report_called_v1, "Report v1 should be called when v2 missing")

        # Verify Ramp v1
        ramp_called_v1 = False
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            cmd_str = " ".join(cmd)
            if "meta_auto_ramp_v1" in cmd_str:
                ramp_called_v1 = True

        self.assertTrue(ramp_called_v1, "Ramp v1 should be called when v2 missing")

        # Verify Guard skipped
        guard_called = False
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            cmd_str = " ".join(cmd)
            if "meta_guardrails_v1" in cmd_str:
                guard_called = True

        self.assertFalse(guard_called, "Guardrails should be skipped if script missing")

if __name__ == "__main__":
    unittest.main()
