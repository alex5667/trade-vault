import unittest
import os
import json
import six
import sys
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock pandas before importing tools
with patch("pandas.read_parquet"), patch("pandas.DataFrame.to_parquet"):
    from tools import meta_model_quality_report_v2
    from tools import meta_auto_ramp_v2

class TestMetaV2Tools(unittest.TestCase):
    def setUp(self):
        self.model_path = "/tmp/model.json"
        self.report_path = "/tmp/report.json"
        self.prom_path = "/tmp/metrics.prom"
        self.data_path = "/tmp/data.parquet"
        
        # Create dummy model
        model = {
            "features": ["f1"],
            "coef": [1.0],
            "intercept": 0.0,
            "schema_name": "test_schema",
            "schema_version": "v1"
        }
        with open(self.model_path, "w") as f:
            json.dump(model, f)
            
    def test_quality_report_v2(self):
        # Mock pandas dataframe validation
        mock_df = MagicMock()
        mock_df.columns = ["y", "f1", "evidence", "indicators", "regime_bucket", "session_bucket"]
        
        # Mock data iteration
        def to_dict_side_effect():
            return {
                "y": 1,
                "f1": 0.5,
                "evidence": {},
                "indicators": {},
                "regime_bucket": "R1",
                "session_bucket": "S1"
            }
        
        row_mock = MagicMock()
        row_mock.to_dict.side_effect = to_dict_side_effect
        mock_df.iloc.__getitem__.return_value = row_mock
        mock_df.__len__.return_value = 10
        
        # Mock label column
        import numpy as np
        mock_y = MagicMock()
        mock_y.astype.return_value.to_numpy.return_value = np.array([1] * 10)
        mock_df.__getitem__.return_value = mock_y

        with patch("pandas.read_parquet", return_value=mock_df):
            argv = [
                "prog",
                "--model-json", self.model_path,
                "--dataset-parquet", self.data_path,
                "--out-json", self.report_path,
                "--prom-textfile", self.prom_path,
                "--min-group-n", "1"
            ]
            with patch.object(sys, 'argv', argv):
                meta_model_quality_report_v2.main()

        # Check output
        self.assertTrue(os.path.exists(self.report_path))
        with open(self.report_path) as f:
            report = json.load(f)
        self.assertIn("metrics", report)
        
        # Check prom file
        if os.path.exists(self.prom_path):
             with open(self.prom_path) as f:
                 content = f.read()
                 self.assertIn("meta_quality", content)

    def test_auto_ramp_v2(self):
        # Create a report for ramp
        report = {
            "schema": {"name": "test_schema"},
            "metrics": {
                "ece": 0.05,
                "pr_auc": 0.10,
                "precision_top5p": 0.15
            },
            "worst": {
                "coverage_groups": 10,
                "worst_ece": 0.07,
                "worst_pr_auc": 0.09,
                "worst_precision_top5p": 0.14,
                "worst_ece_group": "g1",
                "worst_pr_auc_group": "g2",
                "worst_precision_top5p_group": "g3"
            }
        }
        with open(self.report_path, "w") as f:
            json.dump(report, f)
            
        # Mock Redis
        with patch("tools.meta_auto_ramp_v2.redis") as mock_redis:
            mock_r = MagicMock()
            if mock_redis:
                 mock_redis.Redis.return_value = mock_r
            mock_r.hgetall.return_value = {} 
            
            argv = [
                "prog",
                "--report-json", self.report_path,
                "--apply", "0"
            ]
            with patch.object(sys, 'argv', argv):
                meta_auto_ramp_v2.main()

            # Run with apply=1
            argv = [
                "prog",
                "--report-json", self.report_path,
                "--apply", "1"
            ]
            with patch.object(sys, 'argv', argv):
                meta_auto_ramp_v2.main()
                
            # Verify redis write (only if redis was successfully mocked/imported)
            if mock_redis:
                 mock_r.hset.assert_called()

if __name__ == "__main__":
    unittest.main()
