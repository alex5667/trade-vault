
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd

# Add python-worker to path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


class TestNightlyMetaPipeline(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.parquet_path = os.path.join(self.test_dir, "test_dataset.parquet")
        self.model_json_path = os.path.join(self.test_dir, "meta_model.json")
        self.report_json_path = os.path.join(self.test_dir, "meta_report.json")
        self.ramp_state_path = os.path.join(self.test_dir, "ramp_state.json")
        self.prom_path = os.path.join(self.test_dir, "meta_quality.prom")

        # Create dummy data
        df = pd.DataFrame({
            "y": np.random.randint(0, 2, 100),
            "score_final_01": np.random.rand(100),
            "have": np.random.randint(0, 10, 100),
            "need": np.random.randint(0, 10, 100),
            "ok_soft": np.random.randint(0, 2, 100),
            "exec_risk_norm": np.random.rand(100),
            "exec_risk_bps": np.random.rand(100),
            "ml_scenario": ["A"] * 50 + ["B"] * 50,
        })
        df.to_parquet(self.parquet_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_quality_report_structure(self):
        # 1. Create a dummy model
        model = {
            "features": ["score_final_01"],
            "intercept": 0.0,
            "coef": [1.0],
            "threshold": 0.5,
            "schema_name": "meta_feat_v1"
        }
        with open(self.model_json_path, "w") as f:
            json.dump(model, f)

        # 2. Run report tool directly
        cmd = [
            sys.executable, "-m", "tools.meta_model_quality_report_v1",
            "--in-parquet", self.parquet_path,
            "--model-json", self.model_json_path,
            "--label-col", "y",
            "--out-json", self.report_json_path
        ]
        subprocess.run(cmd, check=True)

        # 3. Verify structure
        with open(self.report_json_path) as f:
            report = json.load(f)

        self.assertIn("metrics", report)
        self.assertIn("counts", report)
        self.assertIn("brier", report) # Flat compatibility
        self.assertIn("n", report)     # Flat compatibility

        # Check nested matches flat
        self.assertEqual(report["metrics"]["brier"], report["brier"])
        self.assertEqual(report["counts"]["n"], report["n"])

    def test_auto_ramp_robustness(self):
        # 1. Test with nested report (P4b)
        nested_report = {
            "metrics": {"ece": 0.01, "pr_auc": 0.6},
            "counts": {"n": 500, "pos": 100},
            "meta": {"bucket": "global"}
        }
        with open(self.report_json_path, "w") as f:
            json.dump(nested_report, f)

        cmd = [
            sys.executable, "-m", "tools.meta_auto_ramp_v1",
            "--report-json", self.report_json_path,
            "--apply", "0"
        ]
        res = subprocess.run(cmd, check=True, capture_output=True, text=True)
        out = json.loads(res.stdout)
        self.assertIn("decision", out)
        # Check that it read metrics correctly
        # Note: we can't easily check internal state, but if it runs without crashing and produces a decision, it's good.

        # 2. Test with flat report (Legacy)
        flat_report = {
            "ece": 0.02, "pr_auc": 0.55,
            "n": 500, "pos": 100,
            "meta": {"bucket": "global"}
        }
        with open(self.report_json_path, "w") as f:
            json.dump(flat_report, f)

        res = subprocess.run(cmd, check=True, capture_output=True, text=True)
        out = json.loads(res.stdout)
        self.assertIn("decision", out)

    def test_pipeline_end_to_end(self):
        # Run the full pipeline
        cmd = [
            sys.executable, "-m", "tools.nightly_meta_pipeline_v1",
            "--in-parquet", self.parquet_path,
            "--out-model-json", self.model_json_path,
            "--out-report-json", self.report_json_path,
            "--prom-textfile", self.prom_path,
            "--apply-ramp",
            "--ramp-state", self.ramp_state_path,
            "--ramp-dry-run" # Don't write to redis
        ]

        # We need to make sure train_meta_model_lr_v4 exists.
        # If it doesn't, this test will fail, which is expected as we need to fix it or use available one.
        # Assuming v4 exists as per context.

        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            print("STDOUT:", res.stdout)
            print("STDERR:", res.stderr)

        self.assertEqual(res.returncode, 0)

        self.assertTrue(os.path.exists(self.model_json_path))
        self.assertTrue(os.path.exists(self.report_json_path))
        self.assertTrue(os.path.exists(self.ramp_state_path))
        self.assertTrue(os.path.exists(self.prom_path))

if __name__ == "__main__":
    unittest.main()
