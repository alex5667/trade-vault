
import os
import json
import unittest
import math
from unittest.mock import MagicMock, patch
import sys
import time

# Add tools to path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Make sure we can import the worker even if redis is missing (handled inside worker)
try:
    from tools.policy_effectiveness_report_worker_v1 import (
        _acc_add,
        _precision_top_p,
        _ece,
        _metrics,
        Acc,
    )
except ImportError:
    # If standard redis missing, we mock it
    pass

class TestPolicyEffectivenessWorker(unittest.TestCase):
    def test_metrics_empty(self):
        acc = Acc()
        m = _metrics(acc, top_p=0.05, ece_bins=10)
        self.assertEqual(m["n"], 0.0)
        self.assertEqual(m["expectancy_r"], 0.0)
        self.assertEqual(m["precision_top5p"], 0.0)
        self.assertEqual(m["ece"], 0.0)

    def test_metrics_simple(self):
        acc = Acc()
        # 4 trades
        # 1. score=0.9, y=1, r=1.0 
        # 2. score=0.8, y=0, r=-1.0
        # 3. score=0.2, y=0, r=-0.5
        # 4. score=0.1, y=1, r=2.0
        
        _acc_add(acc, 0.9, 1, 1.0)
        _acc_add(acc, 0.8, 0, -1.0)
        _acc_add(acc, 0.2, 0, -0.5)
        _acc_add(acc, 0.1, 1, 2.0)

        # Expectancy = (1 - 1 - 0.5 + 2) / 4 = 1.5 / 4 = 0.375
        # top 25% = top 1 (score 0.9) -> win -> 1.0
        m = _metrics(acc, top_p=0.25, ece_bins=10) 
        self.assertEqual(m["n"], 4.0)
        self.assertAlmostEqual(m["expectancy_r"], 0.375)
        self.assertEqual(m["precision_top5p"], 1.0)

        # Test ECE with 10 bins
        # 0.9 -> bin 9. 1 sample. p=0.9. y=1. err=0.1. w=0.25 -> 0.025
        # 0.8 -> bin 8. 1 sample. p=0.8. y=0. err=0.8. w=0.25 -> 0.2
        # 0.2 -> bin 2. 1 sample. p=0.2. y=0. err=0.2. w=0.25 -> 0.05
        # 0.1 -> bin 1. 1 sample. p=0.1. y=1. err=0.9. w=0.25 -> 0.225
        # sum = 0.025 + 0.2 + 0.05 + 0.225 = 0.5
        self.assertAlmostEqual(m["ece"], 0.5)

if __name__ == "__main__":
    unittest.main()
