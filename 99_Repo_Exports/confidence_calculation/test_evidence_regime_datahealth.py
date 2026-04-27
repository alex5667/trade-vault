import sys
import os
import unittest
from typing import Any, Dict, List

# Add python-worker to path to simulate production environment
sys.path.append(os.path.abspath("python-worker"))

# Import ConfidenceScorer
# We need to make sure the relative imports inside confidence_scorer work.
# If this fails, we might need to adjust how we import or run the test.
try:
    from handlers.crypto_orderflow.scoring.confidence_scorer import ConfidenceScorer
except ImportError:
    # Fallback: try to see if we can import assuming we are in python-worker root
    sys.path.append(os.getcwd())
    from handlers.crypto_orderflow.scoring.confidence_scorer import ConfidenceScorer

class MockRuntime:
    def __init__(self, config=None, **kwargs):
        self.config = config or {}
        for k, v in kwargs.items():
            setattr(self, k, v)

class MockCtx:
    def __init__(self, ind: Dict[str, Any], confs: List[str], rt: Any, evidence: Dict[str, Any]):
        self.ind = ind
        self.confirmations = confs
        self.rt = rt
        self.evidence = evidence

    def __getattr__(self, name: str) -> Any:
        if name in self.evidence:
             return self.evidence[name]
        if name in self.ind:
            return self.ind[name]
        cfg = getattr(self.rt, "config", None)
        if isinstance(cfg, dict) and name in cfg:
            return cfg[name]
        return getattr(self.rt, name)

class TestConfidenceScorerRegime(unittest.TestCase):
    def setUp(self):
        self.scorer = ConfidenceScorer(None) # of_engine not used in score()

    def _score(self, evidence, indicators=None, confirmations=None, runtime_attrs=None):
        if indicators is None: indicators = {}
        if confirmations is None: confirmations = []
        if runtime_attrs is None: runtime_attrs = {}

        runtime = MockRuntime(**runtime_attrs)
        # We simulate the ctx construction logic from tick_processor
        ctx = MockCtx(indicators, confirmations, runtime, evidence)
        return self.scorer.score(kind="custom", side="LONG", ctx=ctx)

    def test_regime_aware_weights(self):
        print("\n--- Test Regime Aware Weights ---")
        # Baseline: Neutral
        # high z-score (4.0 -> 1.0)
        # low progress (0.1 -> 0.0)
        # All else neutral
        ev_neutral = {
            "market_mode": "neutral",
            "main_z": 4.0,
            "weakProgress_ratio": 0.1, # progress score 0
            "book_quality": 1.0,
            "atr_q_main": 0.5,
            "obi_window_imbalance_z": 2.5 # obi score 1.0
        }
        score_neutral, parts_neutral = self._score(ev_neutral)
        print(f"Neutral Score: {score_neutral:.4f}, Parts: {parts_neutral}")
        
        # Test 1: Trend Mode
        # Should heavily weight Z-score (which is high) and ignore weak progress (which is low)
        ev_trend = ev_neutral.copy()
        ev_trend["market_mode"] = "momentum_up" # triggers trend
        score_trend, parts_trend = self._score(ev_trend)
        print(f"Trend Score: {score_trend:.4f}, Parts: {parts_trend}")

        # In neutral: w_z=0.35, w_prog=0.15. Z=1, Prog=0.
        # In trend: w_z bumped, w_prog reduced.
        # Since Z is high and Prog is low, Trend mode should result in HIGHER score than Neutral
        self.assertGreater(score_trend, score_neutral, "Trend mode should boost score when Z is high and Progress is low")
        self.assertEqual(parts_trend["regime"], 1.0)

        # Test 2: Range Mode
        # Should heavily weight Progress.
        ev_range = ev_neutral.copy()
        ev_range["market_mode"] = "mean_reversion" # triggers range
        score_range, parts_range = self._score(ev_range)
        print(f"Range Score: {score_range:.4f}, Parts: {parts_range}")

        # In range: w_prog bumped. Since Prog=0, this should LOWER the score compared to neutral.
        self.assertLess(score_range, score_neutral, "Range mode should lower score when Progress is low")
        self.assertEqual(parts_range["regime"], 0.5)

    def test_data_health_calibration(self):
        print("\n--- Test Data Health Calibration ---")
        ev = {
            "market_mode": "neutral",
            "main_z": 4.0, # high
            "data_health": 1.0
        }
        s1, parts1 = self._score(ev)
        print(f"Health 1.0 Score: {s1:.4f}")
        
        # Degraded healthy
        ev_bad = ev.copy()
        ev_bad["data_health"] = 0.5
        s05, parts05 = self._score(ev_bad)
        print(f"Health 0.5 Score: {s05:.4f}, Mult: {parts05['dh_mult']:.4f}")
        
        self.assertLess(s05, s1)
        self.assertAlmostEqual(parts05["dh_mult"], 0.5, places=2)
        
        # Floor check
        ev_floor = ev.copy()
        ev_floor["data_health"] = 0.1
        ev_floor["data_health_floor"] = 0.2
        s_floor, parts_floor = self._score(ev_floor)
        print(f"Health 0.1 (Floor 0.2) Score: {s_floor:.4f}, Mult: {parts_floor['dh_mult']:.4f}")
        self.assertAlmostEqual(parts_floor["dh_mult"], 0.2, places=2)

    def test_evidence_vs_confirmations_priority(self):
        print("\n--- Test Evidence vs Confirmations ---")
        # Evidence says 1.0, Confirmations say 0.5 (if parsed)
        # But here we just check if evidence key is used for bonuses
        
        # Case 1: Evidence has reclaim
        ev = {"reclaim": 1.0, "main_z": 2.0}
        s_ev, parts_ev = self._score(ev)
        print(f"With Reclaim (evidence): {s_ev:.4f}, Bonus: {parts_ev['applied_bonus']:.4f}")
        
        # Case 2: No reclaim
        ev_no = {"main_z": 2.0}
        s_no, parts_no = self._score(ev_no)
        print(f"Without Reclaim: {s_no:.4f}, Bonus: {parts_no['applied_bonus']:.4f}")
        
        self.assertGreater(s_ev, s_no)
        self.assertAlmostEqual(parts_ev["applied_bonus"], 0.05, delta=0.01)

if __name__ == '__main__':
    unittest.main()
