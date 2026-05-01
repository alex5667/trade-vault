"""
Tests for Train Confidence Calibrator V2 (Schema 2, Platt/Isotonic/Beta).
"""

import json
import os
import tempfile
import unittest
import numpy as np
from typing import List, Dict, Any

# We import the module under test. 
# Adjust path if needed or use sys.path hack if running isolated.
# Assuming we run via python -m pytest
from ml_analysis.tools import train_confidence_calibrator_v2 as trainer

class TestTrainConfidenceCalibratorV2(unittest.TestCase):
    
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.jsonl_path = os.path.join(self.tmp_dir, "data.jsonl")
        self.bundle_path = os.path.join(self.tmp_dir, "bundle.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir)

    def _make_jsonl(self, rows: List[Dict[str, Any]]):
        with open(self.jsonl_path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    def test_platt_exact_fit(self):
        # Create synthetic data perfectly separable by logistic
        # logit(p) -> y
        # Let's say p_true = sigmoid(2 * logit(p_raw) + 0.5)
        # We start with p_raw, calculate p_true, sample y.
        # But deterministic test: just set y approx p_true?
        # Platt fits: logit(p_cal) = a * logit(p_raw) + b
        
        # Data
        rng = np.random.RandomState(42)
        p_raw = rng.uniform(0.1, 0.9, 1000)
        logit_raw = np.log(p_raw / (1.0 - p_raw))
        
        # Target calibration parameters
        a_true = 1.5
        b_true = -0.5
        
        logit_cal = a_true * logit_raw + b_true
        p_cal_true = 1.0 / (1.0 + np.exp(-logit_cal))
        
        # Sample y from p_cal_true
        y = (rng.rand(1000) < p_cal_true).astype(int)
        
        rows = []
        for yi, pi in zip(y, p_raw):
            rows.append({"y": int(yi), "indicators": {"confidence_v1": pi}})
            
        self._make_jsonl(rows)
        
        # Run Trainer via main (subprocess/argparse) or internal functions
        # Let's use internal function `train_and_evaluate` for unit test
        params, report = trainer.train_and_evaluate(y.tolist(), p_raw.tolist(), "platt", 1e-6)
        
        print(f"Fitted Platt: a={params.get('a')}, b={params.get('b')}")
        self.assertTrue(abs(params["a"] - a_true) < 0.5, f"Slope {params['a']} should be close to {a_true}")
        # Intercept might be noisy with binary y, but check reasonable range
        self.assertTrue(abs(params["b"] - b_true) < 0.5)

    def test_isotonic_boundaries(self):
        # Piecewise monotonic
        # p: 0.1 -> y=0, p: 0.9 -> y=1
        rows = []
        for _ in range(50):
            rows.append({"y": 0, "indicators": {"confidence_v1": 0.1}})
        for _ in range(50):
            rows.append({"y": 1, "indicators": {"confidence_v1": 0.9}})
            
        self._make_jsonl(rows)
        
        # Need to handle sklearn import check
        try:
            import sklearn
            has_sklearn = True
        except ImportError:
            has_sklearn = False

        params, report = trainer.train_and_evaluate(
            [0]*50 + [1]*50, 
            [0.1]*50 + [0.9]*50, 
            "isotonic", 
            1e-6
        )
        
        if has_sklearn:
            self.assertTrue("boundaries" in params)
            self.assertTrue("values" in params)
            self.assertTrue(len(params["boundaries"]) > 0)
        else:
            # Fallback
            self.assertTrue(len(params.get("boundaries", [])) == 0)

    def test_beta_calibration(self):
        # Just check it runs and produces params a,b,c
        y = [0, 0, 1, 1] * 25
        p = [0.2, 0.3, 0.8, 0.9] * 25
        params, report = trainer.train_and_evaluate(y, p, "beta", 1e-6)
        
        self.assertIn("a", params)
        self.assertIn("b", params)
        self.assertIn("c", params)
        self.assertTrue(isinstance(params["a"], float))

    def test_schema_v2_output(self):
        # Run main and check output file structure
        rows = [{"y": 1, "indicators": {"confidence_v1": 0.8}, "context": {"session": "A"}}] * 100
        rows += [{"y": 0, "indicators": {"confidence_v1": 0.2}, "context": {"session": "B"}}] * 100
        self._make_jsonl(rows)
        
        trainer_args = [
            "--in_jsonl", self.jsonl_path,
            "--out_bundle", self.bundle_path,
            "--method", "platt",
            "--bucket_by", "session",
            "--min_rows", "10"
        ]
        
        # Mock sys.argv? Or call main if modified to accept args?
        # My main() uses argparse.parse_args().
        # I can mock sys.argv
        import sys
        from unittest.mock import patch
        
        with patch.object(sys, 'argv', ["prog"] + trainer_args):
            trainer.main()
            
        self.assertTrue(os.path.exists(self.bundle_path))
        
        with open(self.bundle_path) as f:
            data = json.load(f)
            
        self.assertEqual(data.get("schema_version"), 2)
        self.assertIn("meta", data)
        self.assertIn("buckets", data)
        self.assertIn("global", data["buckets"])
        self.assertIn("A", data["buckets"])
        self.assertIn("B", data["buckets"])
        
        # Check buckets have method/params/metrics
        self.assertEqual(data["buckets"]["A"]["method"], "platt")
        self.assertIn("metrics", data["buckets"]["A"])

if __name__ == '__main__':
    unittest.main()
