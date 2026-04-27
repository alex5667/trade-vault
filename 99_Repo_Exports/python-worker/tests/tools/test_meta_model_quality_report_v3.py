
import unittest
import json
import os
import tempfile
import pandas as pd
import numpy as np
import time
from unittest.mock import patch, MagicMock
from tools import meta_model_quality_report_v3

class TestMetaModelQualityReportV3(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.dataset_path = os.path.join(self.tmp_dir.name, "dataset.parquet")
        self.model_path = os.path.join(self.tmp_dir.name, "model.json")
        self.out_path = os.path.join(self.tmp_dir.name, "report.json")
        self.prom_path = os.path.join(self.tmp_dir.name, "metrics.prom")

        # Create dummy model
        self.model_data = {
            "intercept": -1.0,
            "coef": [0.5, 0.5],
            "features": ["f1", "f2"],
            "schema_name": "v3_test"
        }
        with open(self.model_path, "w") as f:
            json.dump(self.model_data, f)

        # Create data for DataFrame
        self.data = []
        for i in range(100):
            row = {
                "y": i % 2,
                "f1": float(i) * 0.1,
                "evidence": {"f2": float(i) * 0.05},
                "indicators": {"unused": 0},
                "t_ts_ms": 1678886400000 + (i * 3600000) # Starts at 00:00, +1h each row
            }
            self.data.append(row)
        
        self.df = pd.DataFrame(self.data)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_feature_extraction_priority(self):
        row = {"f": 10}
        evidence = {"f": 20}
        indicators = {"f": 30}
        
        val = meta_model_quality_report_v3._get_feat_value("f", row, evidence, indicators)
        self.assertEqual(val, 10)
        
        val = meta_model_quality_report_v3._get_feat_value("f", {}, evidence, indicators)
        self.assertEqual(val, 20)
        
        val = meta_model_quality_report_v3._get_feat_value("f", {}, {}, indicators)
        self.assertEqual(val, 30)

    def test_dynamic_grouping(self):
        # 1678886400000 is 2023-03-15 00:00:00 UTC
        ts = 1678886400000 
        row = {"t_ts_ms": ts}
        
        st = time.gmtime(ts / 1000.0)
        print(f"DEBUG: ts={ts} st={st}")
        
        # DOW: Wed = 2
        dow = meta_model_quality_report_v3._derive_group_value("dow_bucket", row, {}, {})
        self.assertEqual(dow, "2")
        
        # Session: 13:20 UTC -> london (8-16)
        sess = meta_model_quality_report_v3._derive_group_value("session_bucket", row, {}, {})
        # If this fails, check printed st
        self.assertEqual(sess, "london")

    @patch("tools.meta_model_quality_report_v3.pd.read_parquet")
    def test_full_run(self, mock_read_parquet):
        mock_read_parquet.return_value = self.df
        
        with patch("sys.argv", [
            "meta_model_quality_report_v3.py",
            "--model-json", self.model_path,
            "--dataset-parquet", self.dataset_path,
            "--out-json", self.out_path,
            "--prom-textfile", self.prom_path,
            "--group-cols", "session_bucket,dow_bucket",
            "--min-group-n", "5"
        ]):
            meta_model_quality_report_v3.main()
        
        self.assertTrue(os.path.exists(self.out_path))
        with open(self.out_path) as f:
            report = json.load(f)
        
        self.assertIn("metrics", report)
        self.assertIn("groups", report)
        
        # Keys are composite e.g. "session_bucket=asia|dow_bucket=2"
        keys = list(report["groups"].keys())
        self.assertTrue(len(keys) > 0)
        print(f"DEBUG: Group Keys: {keys}")
        self.assertTrue(any("session_bucket" in k for k in keys))
        self.assertTrue(any("dow_bucket" in k for k in keys))
        
        # Check prom file
        with open(self.prom_path) as f:
            content = f.read()
        self.assertIn("meta_quality_brier", content)
        self.assertIn('schema="v3_test"', content)

if __name__ == "__main__":
    unittest.main()
