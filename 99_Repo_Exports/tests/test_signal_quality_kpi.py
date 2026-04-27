"""Unit tests for P47 Signal Quality KPIs logic."""


import unittest
import numpy as np
import sys
import os

# Fix path to include python-worker/services
sys.path.insert(0, os.path.join(os.getcwd(), 'python-worker'))

from services.orderflow.signal_quality_kpi_worker_v1 import calculate_kpis, calculate_ece

class TestSignalQualityKPIs(unittest.TestCase):
    
    def test_calculate_ece_perfect(self):
        # Perfect calibration: prob matches label average exactly in bins
        # Bin 1 (0.0-0.1): empty
        # Bin ...
        # Bin 8 (0.8-0.9): 10 items, all prob 0.85, 85% positive
        
        # Simple case: 2 bins (0-0.5, 0.5-1.0)
        # 10 items in low bin: prob=0.2, label=0 (should be 20% but is 0%) -> err 0.2
        # 10 items in high bin: prob=0.8, label=1 (should be 80% but is 100%) -> err 0.2
        
        # Wait, ECE = weighted method.
        # Let's try perfect case.
        probs = [0.8] * 10
        labels = [1] * 8 + [0] * 2 # 8/10 = 0.8
        
        # If we use 10 bins, 0.8 falls into one bin.
        # Bin avg prob = 0.8
        # Bin avg acc = 0.8
        # Error = 0
        ece = calculate_ece(labels, probs, n_bins=10)
        self.assertAlmostEqual(ece, 0.0)

    def test_calculate_ece_bad(self):
        # Terrible calibration
        # Probs 0.9, but labels all 0
        probs = [0.9] * 10
        labels = [0] * 10
        
        # Bin avg prob = 0.9
        # Bin avg acc = 0.0
        # Diff = 0.9
        # Weight = 1.0
        ece = calculate_ece(labels, probs, n_bins=10)
        self.assertAlmostEqual(ece, 0.9)

    def test_calculate_kpis_basic(self):
        # Mock rows
        rows = [
            {'r_mult': 2.0, 'y': 1},
            {'r_mult': -1.0, 'y': 0},
            {'r_mult': 0.5, 'y': 1}, # Win rate min assumed 0.0 in env? Def default is 0.0
        ]
        
        # We need MIN_N defaults to 30 in worker, but we can't change constant easily here unless we mock it or 
        # relying on the fact that `calculate_kpis` checks `MIN_N`.
        # Wait, MIN_N is global in the module. I should patch it or provide enough rows.
        
        # Let's generate 30 rows
        rows = [{'r_mult': 1.0, 'y': 1, 'score': 0.9} for _ in range(30)]
        
        kpis = calculate_kpis(rows)
        self.assertEqual(kpis['n'], 30)
        self.assertEqual(kpis['expectancy_r'], 1.0)
        self.assertEqual(kpis['win_rate'], 1.0)
        
    def test_precision_top5p(self):
        # 100 rows
        # Top 5% = 5 rows.
        # Make top 5 scores have y=1, others y=0
        rows = []
        for i in range(100):
            score = i / 100.0 # 0.0 to 0.99
            # Top 5 are 0.95, 0.96, 0.97, 0.98, 0.99
            is_top = score >= 0.95
            y = 1 if is_top else 0
            rows.append({'r_mult': 1.0 if y else -1.0, 'y': y, 'score': score})
            
        kpis = calculate_kpis(rows)
        self.assertEqual(kpis['n'], 100)
        self.assertEqual(kpis.get('precision_top5p'), 1.0) # 5/5 wins
        
    def test_expectancy_r(self):
         rows = [{'r_mult': 1.0, 'y': 1} for _ in range(15)] + [{'r_mult': -1.0, 'y': 0} for _ in range(15)]
         kpis = calculate_kpis(rows)
         self.assertEqual(kpis['n'], 30)
         self.assertAlmostEqual(kpis['expectancy_r'], 0.0)

if __name__ == '__main__':
    unittest.main()
