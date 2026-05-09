import json
import os
import unittest
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock

from tools.meta_ab_winner_evaluator_v1 import compute_metrics, evaluate_models, make_decision


class TestMetaABWinnerEvaluator(unittest.TestCase):
    def setUp(self):
        # Create a mock MetaModelLR
        self.mock_model = MagicMock()
        self.mock_model.predict_proba.side_effect = lambda x: x.get("p", 0.5)

        # Create a temporary ndjson dataset
        self.dataset = NamedTemporaryFile(delete=False, suffix=".ndjson", mode="w")
        rows = [
            {"ok": 1, "r_mult": 0.5, "p": 0.7, "indicators": {"p": 0.7, "f1": 1}},  # Eligible, p=0.7
            {"ok": 1, "r_mult": -1.2, "p": 0.8, "indicators": {"p": 0.8, "f1": 1}}, # Eligible, p=0.8, tail risk
            {"ok": 0, "r_mult": 1.0, "p": 0.9, "indicators": {"p": 0.9, "f1": 1}},  # Not eligible
            {"ok": 1, "r_mult": 0.1, "p": 0.2, "indicators": {"p": 0.2, "f1": 1}},  # Eligible, p=0.2 (below p_min)
        ]
        for r in rows:
            self.dataset.write(json.dumps(r) + "\n")
        self.dataset.close()
        self.dataset_path = self.dataset.name

    def tearDown(self):
        if os.path.exists(self.dataset_path):
            os.remove(self.dataset_path)

    def test_compute_metrics(self):
        p = [0.7, 0.8]
        r = [0.5, -1.2]
        m = compute_metrics(p, r)
        self.assertAlmostEqual(m["exp_r"], (0.5 - 1.2) / 2)
        self.assertEqual(m["tail_risk"], 0.5) # 1 out of 2 is <= -1.0
        self.assertEqual(m["n"], 2)

    def test_evaluate_models(self):
        # Mock models with different predictions
        model_champ = MagicMock()
        model_champ.predict_proba.side_effect = lambda x: x.get("p")

        model_chall = MagicMock()
        model_chall.predict_proba.side_effect = lambda x: (x.get("p") + 0.05) if x.get("p") is not None else None

        # At p_min=0.75:
        # Champ gets row 2 (p=0.8)
        # Chall gets row 1 (p=0.7+0.05=0.75) and row 2 (p=0.8+0.05=0.85)

        results = evaluate_models(self.dataset_path, 0.75, model_champ, model_chall)

        self.assertEqual(results["champion"]["n"], 1)
        self.assertEqual(results["challenger"]["n"], 2)
        self.assertEqual(results["n_eligible"], 3)

    def test_make_decision_challenger_wins(self):
        metrics = {
            "champion": {"exp_r": 0.01, "tail_risk": 0.1, "n": 100},
            "challenger": {"exp_r": 0.02, "tail_risk": 0.1, "n": 100}
        }
        # Delta ExpR = 0.01 (> 0.005), Tail ratio = 1.0 (< 1.1)
        winner, reason = make_decision(metrics, min_delta_exp_r=0.005, tail_slack=0.1)
        self.assertEqual(winner, "challenger")

    def test_make_decision_challenger_too_risky(self):
        metrics = {
            "champion": {"exp_r": 0.01, "tail_risk": 0.1, "n": 100},
            "challenger": {"exp_r": 0.03, "tail_risk": 0.15, "n": 100}
        }
        # Delta ExpR = 0.02 (> 0.005), Tail ratio = 1.5 (> 1.1)
        winner, reason = make_decision(metrics, min_delta_exp_r=0.005, tail_slack=0.1)
        self.assertEqual(winner, "champion")
        self.assertIn("too risky", reason)

    def test_make_decision_challenger_underperforms(self):
        metrics = {
            "champion": {"exp_r": 0.02, "tail_risk": 0.1, "n": 100},
            "challenger": {"exp_r": 0.021, "tail_risk": 0.1, "n": 100}
        }
        # Delta ExpR = 0.001 (< 0.005), Tail ratio = 1.0 (< 1.1)
        winner, reason = make_decision(metrics, min_delta_exp_r=0.005, tail_slack=0.1)
        self.assertEqual(winner, "champion")
        self.assertIn("underperforms", reason)

if __name__ == "__main__":
    unittest.main()
