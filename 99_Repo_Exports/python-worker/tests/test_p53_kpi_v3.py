import unittest
import json
from unittest.mock import MagicMock, patch
from services.orderflow.tools.signal_quality_kpi_worker_v3 import _ece, _precision_top_p, _win_label, _extract_score_prob

class TestKPIV3(unittest.TestCase):

    def test_win_label(self):
        self.assertEqual(_win_label(1.5, 0.0), 1)
        self.assertEqual(_win_label(-0.5, 0.0), 0)
        self.assertEqual(_win_label(None, 0.0), None)

    def test_ece_perfect(self):
        # Perfect calibration: prob matches label average exactly in bins
        probs = [0.8] * 10
        labels = [1] * 8 + [0] * 2 # 8/10 = 0.8
        # Bin avg prob = 0.8, Bin avg acc = 0.8
        ece = _ece(probs, labels, bins=10)
        self.assertAlmostEqual(ece, 0.0)

    def test_precision_top_p(self):
        # 100 items, top 5% = 5 items
        scores = [i/100.0 for i in range(100)]
        # Top scores are 0.95, 0.96, 0.97, 0.98, 0.99. Make 3 of them wins (y=1)
        ys = [0] * 95 + [1, 1, 1, 0, 0] # indices 95-99 are top scores
        prec = _precision_top_p(scores, ys, 0.05)
        self.assertEqual(prec, 0.6) # 3/5

    def test_extract_score_prob(self):
        d = {"ml_p": 0.7, "ml_p_cal": 0.75, "other": 123}
        score, prob = _extract_score_prob(d, ["ml_p_cal", "ml_p"], ["ml_p_cal", "ml_p"])
        self.assertEqual(score, 0.75)
        self.assertEqual(prob, 0.75)
        
        d2 = {"score": 0.5}
        score2, prob2 = _extract_score_prob(d2, ["ml_p", "score"], ["ml_p"])
        self.assertEqual(score2, 0.5)
        self.assertEqual(prob2, None)

if __name__ == "__main__":
    unittest.main()
