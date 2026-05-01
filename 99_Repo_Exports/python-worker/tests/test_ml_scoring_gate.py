# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Unit tests for MLScoringGate — ML Scorer V2 runtime inference.

Tests:
  - fail-open when model not loaded
  - feature extraction from mock context
  - conf01 output clamped [0.05, 0.98]
  - shadow mode returns rule-based result with ml_shadow_* parts
  - score returns None when model unavailable
"""

import math
import os
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import pytest

# Ensure python-worker is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# MLScoringGate unit tests
# ---------------------------------------------------------------------------


class TestMLScoringGateFailOpen:
    """Gate must return (None, {...}) when model is unavailable."""

    def test_no_model_file(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/scorer_v2.joblib", refresh_ms=1)
        conf, parts = gate.score(kind="breakout", side="LONG", ctx=SimpleNamespace())
        assert conf is None
        assert parts.get("ml_status") in ("model_unavailable",)

    def test_no_context(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/scorer_v2.joblib", refresh_ms=1)
        conf, parts = gate.score(kind="breakout", side="LONG", ctx=None)
        assert conf is None

    def test_is_loaded_default_false(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/scorer_v2.joblib")
        assert gate.is_loaded is False

    def test_model_metrics_empty_when_no_model(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/scorer_v2.joblib")
        assert gate.model_metrics == {}


class TestMLScoringGateFeatureExtraction:
    """Feature extraction must produce correct-length vector."""

    def test_feature_count(self):
        from services.ml_scoring_gate import MLScoringGate, _NUMERIC_FEATURE_ATTRS

        gate = MLScoringGate(model_path="/nonexistent/path")

        ctx = SimpleNamespace(
            conf_score=0.85,
            atr_14=50.0,
            delta_spike_z=3.5,
            obi_avg_20=0.3,
            weak_progress_ratio=0.1,
            spread_bps=5.0,
            microprice_shift_bps_20=0.5,
            microprice_velocity_bps=0.2,
            obi_5=0.4,
            obi_20=0.35,
            obi_50=0.3,
            obi_persistence_score=0.6,
            cancel_to_trade_bid_5s=2.0,
            cancel_to_trade_ask_5s=1.5,
            cancel_to_trade_bid_20s=1.8,
            cancel_to_trade_ask_20s=1.3,
            queue_pressure_bid=0.5,
            queue_pressure_ask=0.3,
            market_depth_imbalance=0.1,
        )

        features = gate._extract_features(ctx, "LONG")
        assert features is not None
        # 17 numeric + 6 derived = 23
        expected_count = len(_NUMERIC_FEATURE_ATTRS) + 6
        assert len(features) == expected_count, f"Expected {expected_count}, got {len(features)}"

    def test_direction_encoding(self):
        from services.ml_scoring_gate import MLScoringGate, _NUMERIC_FEATURE_ATTRS

        gate = MLScoringGate(model_path="/nonexistent/path")
        ctx = SimpleNamespace()

        features_long = gate._extract_features(ctx, "LONG")
        features_short = gate._extract_features(ctx, "SHORT")

        n_numeric = len(_NUMERIC_FEATURE_ATTRS)
        # direction_long is at index n_numeric
        assert features_long[n_numeric] == 1.0
        assert features_short[n_numeric] == 0.0

    def test_missing_attrs_default_to_zero(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/path")
        # Empty context — all features should default to 0.0
        ctx = SimpleNamespace()
        features = gate._extract_features(ctx, "LONG")
        assert features is not None
        assert all(math.isfinite(f) for f in features)


class TestMLScoringGateCalibration:
    """conf01 calibration must be clamped [0.05, 0.98]."""

    def test_sigmoid_fallback_positive(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/path")
        conf = gate._calibrate_to_conf01(2.0)
        assert 0.05 <= conf <= 0.98

    def test_sigmoid_fallback_negative(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/path")
        conf = gate._calibrate_to_conf01(-5.0)
        assert 0.05 <= conf <= 0.98

    def test_sigmoid_fallback_zero(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/path")
        conf = gate._calibrate_to_conf01(0.0)
        assert abs(conf - 0.5) < 0.01  # sigmoid(0) = 0.5


class TestMLScoringGateWithMockModel:
    """Test score() with a mock model pack loaded."""

    def _make_mock_gate(self):
        from services.ml_scoring_gate import MLScoringGate

        gate = MLScoringGate(model_path="/nonexistent/path")

        # Simulate loaded model
        import numpy as np

        class MockModel:
            def predict(self, X):
                return np.array([1.5])  # predicted R=1.5

        gate._pack = {
            "kind": "ml_scorer_v2",
            "model": MockModel(),
            "feature_names": gate._build_feature_names() if hasattr(gate, '_build_feature_names') else [],
            "robust_scaler_params": {},
            "calibrator": None,
            "metrics": {"mae_oof": 0.5, "r2_oof": 0.1, "spearman_oof": 0.3},
            "trained_at_ms": 0,
            "n_samples": 5000,
        }
        gate._model = gate._pack["model"]
        gate._feature_names = [f"f_{i}" for i in range(23)]
        gate._scaler_params = {}
        gate._calibrator = None
        return gate

    def test_score_returns_conf01(self):
        gate = self._make_mock_gate()
        ctx = SimpleNamespace()
        conf, parts = gate.score(kind="breakout", side="LONG", ctx=ctx)
        assert conf is not None
        assert 0.05 <= conf <= 0.98
        assert parts.get("ml_status") == "ok"
        assert "ml_predicted_r" in parts

    def test_score_predicted_r_in_parts(self):
        gate = self._make_mock_gate()
        ctx = SimpleNamespace()
        conf, parts = gate.score(kind="breakout", side="LONG", ctx=ctx)
        assert abs(parts["ml_predicted_r"] - 1.5) < 0.01


# ---------------------------------------------------------------------------
# ConfidenceScorer integration — shadow mode
# ---------------------------------------------------------------------------


class TestConfidenceScorerMLShadow:
    """When ff.use_unified_scoring=True and ML gate available, shadow fields must be present."""

    @pytest.mark.asyncio
    async def test_shadow_mode_returns_rule_based_with_ml_fields(self):
        from services.signal_confidence import ConfidenceScorer

        # Create a mock ML gate
        class MockMLGate:
            def score(self, **kwargs):
                return 0.75, {"ml_predicted_r": 1.2, "ml_status": "ok"}

        scorer = ConfidenceScorer(ml_scoring_gate=MockMLGate())
        ff = SimpleNamespace(use_unified_scoring=True)

        with patch.dict(os.environ, {"ML_SCORER_MODE": "shadow"}):
            ctx = SimpleNamespace(
                delta_z=4.0,
                obi_avg_20=0.5,
                obi_sustained_20=True,
                microprice_shift_bps_20=0.3,
                depletion_score=0.1,
                refill_score=0.02,
                spread_bps=5.0,
                l2_age_ms=100,
                impact_proxy=0.1,
            )
            conf, parts = await scorer.score(kind="breakout", side="LONG", ctx=ctx, ff=ff)

        # Should return rule-based result
        assert conf is not None
        assert 0.0 <= conf <= 1.0
        # Should have shadow ML fields
        assert parts.get("ml_shadow_conf01") == 0.75
        assert parts.get("ml_shadow_status") == "ok"
        assert parts.get("scorer_mode") == "shadow"

    @pytest.mark.asyncio
    async def test_enforce_mode_returns_ml_result(self):
        from services.signal_confidence import ConfidenceScorer

        class MockMLGate:
            def score(self, **kwargs):
                return 0.88, {"ml_predicted_r": 2.0, "ml_status": "ok"}

        scorer = ConfidenceScorer(ml_scoring_gate=MockMLGate())
        ff = SimpleNamespace(use_unified_scoring=True)

        with patch.dict(os.environ, {"ML_SCORER_MODE": "enforce"}):
            ctx = SimpleNamespace(delta_z=4.0)
            conf, parts = await scorer.score(kind="breakout", side="LONG", ctx=ctx, ff=ff)

        # Should return ML result directly
        assert conf == 0.88
        assert parts.get("scorer_mode") == "ml_enforce"

    @pytest.mark.asyncio
    async def test_no_ff_uses_rule_based(self):
        from services.signal_confidence import ConfidenceScorer

        scorer = ConfidenceScorer()
        ctx = SimpleNamespace(delta_z=4.0, spread_bps=5.0, l2_age_ms=100, impact_proxy=0.1)
        conf, parts = await scorer.score(kind="breakout", side="LONG", ctx=ctx)

        # Should return rule-based result without shadow fields
        assert conf is not None
        assert 0.0 <= conf <= 1.0
        assert "ml_shadow_conf01" not in parts

    @pytest.mark.asyncio
    async def test_ml_gate_unavailable_falls_back(self):
        from services.signal_confidence import ConfidenceScorer

        class FailingMLGate:
            def score(self, **kwargs):
                return None, {"ml_status": "model_unavailable"}

        scorer = ConfidenceScorer(ml_scoring_gate=FailingMLGate())
        ff = SimpleNamespace(use_unified_scoring=True)

        with patch.dict(os.environ, {"ML_SCORER_MODE": "enforce"}):
            ctx = SimpleNamespace(delta_z=4.0, spread_bps=5.0, l2_age_ms=100, impact_proxy=0.1)
            conf, parts = await scorer.score(kind="breakout", side="LONG", ctx=ctx, ff=ff)

        # Enforce mode but ML returned None → should fallback to rule-based
        assert conf is not None
        assert 0.0 <= conf <= 1.0
        assert parts.get("ml_shadow_status") == "model_unavailable"
        assert parts.get("scorer_mode") in ("shadow", "ml_enforce_fallback")
