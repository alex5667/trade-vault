#!/usr/bin/env python3
"""
Unit tests for the new unified scoring system
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime

from scoring.scoring_engine import ScoringResult, QualityResult, SignalQualityLabel
from signal_scoring.config import ScoringConfig


class TestNewScoringSystem(unittest.TestCase):

    def setUp(self):
        self.config = ScoringConfig()

    def test_scoring_result_creation(self):
        """Test ScoringResult creation and properties"""
        result = ScoringResult(
            score=75.0,
            final_score=85.0,
            confidence=0.8,
            quality_label=SignalQualityLabel.A,
            reasons=["high_score", "good_quality"],
            should_emit=True,
            debug={"test": "value"}
        )

        self.assertEqual(result.score, 75.0)
        self.assertEqual(result.final_score, 85.0)
        self.assertEqual(result.confidence, 0.8)
        self.assertEqual(result.quality_label, SignalQualityLabel.A)
        self.assertEqual(result.reasons, ["high_score", "good_quality"])
        self.assertTrue(result.should_emit)
        self.assertEqual(result.debug, {"test": "value"})

    def test_quality_result_creation(self):
        """Test QualityResult creation and properties"""
        result = QualityResult(
            confidence=0.7,
            label=SignalQualityLabel.B,
            reasons=["medium_score"],
            force_reject=False
        )

        self.assertEqual(result.confidence, 0.7)
        self.assertEqual(result.label, SignalQualityLabel.B)
        self.assertEqual(result.reasons, ["medium_score"])
        self.assertFalse(result.force_reject)

    def test_quality_result_force_reject(self):
        """Test QualityResult with force_reject"""
        result = QualityResult(
            confidence=0.2,
            label=SignalQualityLabel.REJECT,
            reasons=["very_low_quality"],
            force_reject=True
        )

        self.assertEqual(result.confidence, 0.2)
        self.assertEqual(result.label, SignalQualityLabel.REJECT)
        self.assertEqual(result.reasons, ["very_low_quality"])
        self.assertTrue(result.force_reject)

    @patch('signal_scoring.engine.LocalCalibrationStore')
    def test_signal_scoring_engine_score_method(self, mock_calib_store):
        """Test SignalScoringEngine.score() method"""
        from signal_scoring.engine import SignalScoringEngine
        from signal_scoring.ctx import SignalContext

        # Mock dependencies
        mock_calib_store.return_value.get_metric_cfg.return_value = None

        # Create engine without quality estimator
        engine = SignalScoringEngine(
            calib_store=mock_calib_store,
            config=self.config,
            quality_estimator=None
        )

        # Create test context
        ctx = SignalContext(
            ts=datetime.now(),
            symbol="BTCUSDT",
            side="buy",
            session="asia",
            regime="trend"
        )

        # Mock compute_confidence to return 80
        with patch.object(engine, 'compute_confidence', return_value=80):
            result = engine.score(ctx)

        # Verify result
        self.assertIsInstance(result, ScoringResult)
        self.assertEqual(result.score, 80.0)  # base score in 0-100 scale (from engineering/engine.py)
        self.assertEqual(result.confidence, 0.8)  # same as base when no estimator
        self.assertEqual(result.quality_label, SignalQualityLabel.C)  # default when no estimator
        self.assertEqual(result.reasons, ["no_quality_estimator", "no_liquidity_context"])
        self.assertTrue(result.should_emit)

    @patch('signal_scoring.engine.LocalCalibrationStore')
    def test_signal_scoring_engine_should_emit_wrapper(self, mock_calib_store):
        """Test SignalScoringEngine.should_emit() wrapper"""
        from signal_scoring.engine import SignalScoringEngine
        from signal_scoring.ctx import SignalContext

        # Mock dependencies
        mock_calib_store.return_value.get_metric_cfg.return_value = None

        # Create engine without quality estimator
        engine = SignalScoringEngine(
            calib_store=mock_calib_store,
            config=self.config,
            quality_estimator=None
        )

        # Create test context
        ctx = SignalContext(
            ts=datetime.now(),
            symbol="BTCUSDT",
            side="buy",
            session="asia",
            regime="trend"
        )

        # Mock compute_confidence
        with patch.object(engine, 'compute_confidence', return_value=90):
            should_emit = engine.should_emit(ctx)

        # Verify should_emit delegates to score result
        self.assertTrue(should_emit)

    @patch('signal_scoring.engine.LocalCalibrationStore')
    def test_signal_scoring_engine_min_confidence_by_symbol(self, mock_calib_store):
        """Test min confidence calculation by symbol"""
        from signal_scoring.engine import SignalScoringEngine

        # Create engine
        engine = SignalScoringEngine(
            calib_store=mock_calib_store,
            config=self.config
        )

        # Test regular symbol
        min_conf = engine._get_min_confidence_for_symbol("BTCUSDT")
        self.assertEqual(min_conf, 30.0)  # default

        # Test gold symbol
        min_conf_gold = engine._get_min_confidence_for_symbol("XAUUSD")
        self.assertEqual(min_conf_gold, 20.0)  # lower threshold for gold

    def test_signal_quality_labels_enum(self):
        """Test SignalQualityLabel enum values"""
        self.assertEqual(SignalQualityLabel.A.value, "A")
        self.assertEqual(SignalQualityLabel.B.value, "B")
        self.assertEqual(SignalQualityLabel.C.value, "C")
        self.assertEqual(SignalQualityLabel.REJECT.value, "REJECT")


if __name__ == '__main__':
    unittest.main()
