import unittest
from unittest.mock import MagicMock
import os
import sys
from pathlib import Path

# Add python-worker to sys.path
worker_path = Path(__file__).parent.parent
sys.path.insert(0, str(worker_path))

from confidence_calculation.confidence_scorer import _crypto_conf_factor

class TestEnhancedScorer(unittest.TestCase):
    def setUp(self):
        self.ctx = MagicMock()
        self.ctx.market_mode = "neutral"
        self.ctx.atr_q_main = 0.5
        self.ctx.main_z = 3.0
        self.ctx.obi_z = 2.0
        self.ctx.weak_ratio = 1.0
        self.ctx.confirmations = []
        self.ctx.evidence = {}

    def test_base_score(self):
        # Base case
        score, parts = _crypto_conf_factor(self.ctx, "breakout")
        self.assertGreater(score, 0.4)
        print(f"Base score: {score}")

    def test_regime_trend_weights(self):
        self.ctx.market_mode = "momentum_trend"
        self.ctx.confirmations = []
        score_trend, parts_trend = _crypto_conf_factor(self.ctx, "breakout")
        
        self.ctx.market_mode = "range_neutral"
        self.ctx.confirmations = []
        score_range, parts_range = _crypto_conf_factor(self.ctx, "meanrev")
        
        print(f"Trend score: {score_trend}, Range score: {score_range}")
        print(f"Trend parts: {parts_trend}")
        print(f"Range parts: {parts_range}")
        self.assertNotEqual(score_trend, score_range)

    def test_bonuses(self):
        self.ctx.confirmations = ["reclaim", "sweep"]
        self.ctx.evidence = {}
        score_with_bonus, parts = _crypto_conf_factor(self.ctx, "breakout")
        
        self.ctx.confirmations = []
        self.ctx.evidence = {}
        score_no_bonus, _ = _crypto_conf_factor(self.ctx, "breakout")
        
        self.assertGreater(score_with_bonus, score_no_bonus)
        # reclaim (0.05) + sweep (0.03) + synergy (0.02) = 0.10
        self.assertAlmostEqual(parts["bonus"], 0.10, places=2)

    def test_anti_correlation(self):
        # Long trend + High Z + RSI agree -> should dampen RSI bonus
        self.ctx.market_mode = "trend_up"
        self.ctx.main_z = 4.0
        self.ctx.confirmations = ["rsi_agree"]
        self.ctx.evidence = {}
        
        score, parts = _crypto_conf_factor(self.ctx, "breakout")
        # bonus for rsi is 0.02, but dampened by 0.5 -> 0.01 (if regime is trend and main_z > 3.0)
        print(f"Anti-corr parts: {parts}")
        self.assertLess(parts["bonus"], 0.02)

    def test_absent_ml(self):
        os.environ["ML_SCORING_ENABLE"] = "0"
        score, parts = _crypto_conf_factor(self.ctx, "breakout")
        self.assertNotIn("ml_prob", parts)

if __name__ == "__main__":
    unittest.main()
