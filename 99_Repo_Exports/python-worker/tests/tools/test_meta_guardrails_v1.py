import json
import os
import subprocess
import tempfile
import unittest
import pandas as pd
import numpy as np

TOOLS_DIR = os.path.join(os.path.dirname(__file__), "../../tools")
SCRIPT_PATH = os.path.join(TOOLS_DIR, "meta_guardrails_v1.py")

class TestMetaGuardrails(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.model_json = os.path.join(self.tmp_dir, "model.json")
        self.dataset_parquet = os.path.join(self.tmp_dir, "dataset.parquet")
        
    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir)

    def _write_model(self, schema="v3", features=["f1", "f2"]):
        with open(self.model_json, "w") as f:
            json.dump({"schema": schema, "features": features}, f)

    def _write_parquet(self, data):
        df = pd.DataFrame(data)
        df.to_parquet(self.dataset_parquet)

    def _run_guard(self, expected_schema="v3", args=[]):
        cmd = [
            "python3", SCRIPT_PATH,
            "--model-json", self.model_json,
            "--dataset-parquet", self.dataset_parquet,
            "--expected-schema", expected_schema,
            "--apply", "0"
        ] + args
        
        # We need to set PYTHONPATH to include python-worker root
        env = os.environ.copy()
        python_worker_root = os.path.abspath(os.path.join(TOOLS_DIR, ".."))
        env["PYTHONPATH"] = python_worker_root
        
        res = subprocess.run(cmd, env=env, capture_output=True, text=True)
        return res

    def test_happy_path(self):
        self._write_model(schema="v3", features=["f1", "f2"])
        self._write_parquet({"f1": [1.0]*100, "f2": [2.0]*100})
        
        res = self._run_guard(expected_schema="v3")
        self.assertEqual(res.returncode, 0)
        self.assertIn("DECISION: freeze=0", res.stdout)
        self.assertIn("Schema check passed", res.stdout)

    def test_schema_mismatch(self):
        self._write_model(schema="v4", features=["f1"])
        self._write_parquet({"f1": [1.0]*10})
        
        # Expect v3 (default) but model has v4
        res = self._run_guard(args=["--require-schema", "v3"])
        self.assertEqual(res.returncode, 0) # Script exits 0 but freezes
        self.assertIn("DECISION: freeze=1", res.stdout)
        self.assertIn("Schema mismatch", res.stdout)

    def test_missingness_high(self):
        self._write_model(schema="v3", features=["f1"])
        # 10% missing, default max is 5%
        data = {"f1": [1.0]*90 + [None]*10} 
        self._write_parquet(data)
        
        res = self._run_guard(expected_schema="v3")
        self.assertEqual(res.returncode, 0)
        self.assertIn("DECISION: freeze=1", res.stdout)
        self.assertIn("High global missing", res.stdout)

    def test_critical_feature_missing(self):
        self._write_model(schema="v3", features=["f1", "crit_feat"])
        # crit_feat completely missing from columns
        data = {"f1": [1.0]*10}
        self._write_parquet(data)
        
        res = self._run_guard(expected_schema="v3", args=["--crit-features", "crit_feat"])
        self.assertEqual(res.returncode, 0)
        self.assertIn("DECISION: freeze=1", res.stdout)
        self.assertIn("Critical feature 'crit_feat' not found", res.stdout)

if __name__ == "__main__":
    unittest.main()
