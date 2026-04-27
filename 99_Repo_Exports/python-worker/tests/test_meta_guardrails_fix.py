import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from io import StringIO

# Add current directory to path so we can import tools
sys.path.append(os.getcwd())

from tools.meta_guardrails_v1 import main

class TestMetaGuardrails(unittest.TestCase):
    @patch('tools.meta_guardrails_v1._get_redis_client')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.exists')
    def test_dataset_not_found_no_nameerror(self, mock_exists, mock_parse_args, mock_get_redis):
        # Mocking arguments
        args = MagicMock()
        args.model_json = "/tmp/non_existent_model.json"
        args.dataset_parquet = "/tmp/non_existent_dataset.parquet"
        args.fallback_model_json = ""
        args.report_json = ""
        args.notify_stream = "notify:telegram"
        args.redis_url = "redis://localhost:6379/0"
        args.dyn_key = "settings:dynamic_cfg"
        args.freeze_key = "meta_guard_freeze"
        args.reason_key = "meta_guard_reason"
        args.apply = 0
        args.prom_textfile = ""
        args.ignore_dq = False
        args.expected_schema = ""
        args.require_schema = ""
        
        mock_parse_args.return_value = args
        
        # Mocking os.path.exists to fail for model (fatally) or dataset
        def side_effect(path):
            if path == args.model_json:
                return True # Model exists
            return False # Dataset doesn't exist
        mock_exists.side_effect = side_effect
        
        # Mocking _load_json to return minimal model
        with patch('tools.meta_guardrails_v1._load_json', return_value={"schema": "v4"}):
            # Capture stdout
            with patch('sys.stdout', new=StringIO()) as fake_out:
                try:
                    main()
                except SystemExit as e:
                    self.assertEqual(e.code, 0 if e.code is None else e.code)
                
                output = fake_out.getvalue()
                print(output)
                self.assertIn("FAIL: Dataset not found", output)
                self.assertIn("DECISION: freeze=1 reason='Dataset not found'", output)

    @patch('tools.meta_guardrails_v1._get_redis_client')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.exists')
    @patch('pandas.read_parquet')
    def test_empty_dataset_no_nameerror(self, mock_read_parquet, mock_exists, mock_parse_args, mock_get_redis):
        import pandas as pd
        # Mocking arguments
        args = MagicMock()
        args.model_json = "/tmp/model.json"
        args.dataset_parquet = "/tmp/empty.parquet"
        args.fallback_model_json = ""
        args.report_json = ""
        args.notify_stream = "notify:telegram"
        args.redis_url = "redis://localhost:6379/0"
        args.dyn_key = "settings:dynamic_cfg"
        args.freeze_key = "meta_guard_freeze"
        args.reason_key = "meta_guard_reason"
        args.apply = 0
        args.prom_textfile = ""
        args.ignore_dq = False
        args.expected_schema = ""
        args.require_schema = ""
        args.crit_features = "f1"
        args.max_miss_mean = 0.05
        args.max_miss_crit = 0.20
        
        mock_parse_args.return_value = args
        mock_exists.return_value = True # Everything exists
        
        # Mocking empty dataframe
        mock_read_parquet.return_value = pd.DataFrame()
        
        # Mocking _load_json
        with patch('tools.meta_guardrails_v1._load_json', return_value={"features": ["f1"], "schema": "v4"}):
            # Capture stdout
            with patch('sys.stdout', new=StringIO()) as fake_out:
                main()
                
                output = fake_out.getvalue()
                print(output)
                self.assertIn("FAIL: Dataset /tmp/empty.parquet is empty.", output)
                self.assertIn("DECISION: freeze=1", output)

if __name__ == '__main__':
    unittest.main()
