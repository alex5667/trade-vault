from __future__ import annotations

"""
Unit tests for Signal Ensemble: SignalVote, EnsembleDecision, SignalEnsemble.
"""

import json
import os
import sys
from unittest.mock import MagicMock

import pytest

# Add python-worker to path for imports (matches pytest.ini pythonpath=.)
_pw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _pw_dir not in sys.path:
    sys.path.insert(0, _pw_dir)

from services.signal_ensemble import (
    EnsembleDecision,
    SignalEnsemble,
    SignalVote,
    build_microstructure_vote,
    build_orderflow_vote,
    build_regime_vote,
    build_ta_vote,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class FakeRedis:
    """Minimal Redis mock for ensemble tests."""

    def __init__(self, data: dict = None):
        self._data = data or {}
        self._hash_data = {}
        self._stream_data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, **kwargs):
        self._data[key] = value

    def hgetall(self, key):
        return self._hash_data.get(key, {})

    def hset(self, key, field, value):
        if key not in self._hash_data:
            self._hash_data[key] = {}
        self._hash_data[key][field] = value

    def xadd(self, key, fields, **kwargs):
        if key not in self._stream_data:
            self._stream_data[key] = []
        self._stream_data[key].append(fields)
        return f"1-{len(self._stream_data[key])}"

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self._ops = []

    def delete(self, key):
        self._ops.append(("delete", key))

    def hset(self, key, field, value):
        self._ops.append(("hset", key, field, value))

    def execute(self):
        for op in self._ops:
            if op[0] == "delete":
                self.redis._hash_data.pop(op[1], None)
            elif op[0] == "hset":
                if op[1] not in self.redis._hash_data:
                    self.redis._hash_data[op[1]] = {}
                self.redis._hash_data[op[1]][op[2]] = op[3]
        self._ops = []


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture
def ensemble(redis):
    return SignalEnsemble(
        redis_client=redis,
        symbol="BTCUSDT",
        mode="enforce",
        threshold=0.35,
        consensus_ratio=1.5,
    )


@pytest.fixture
def ensemble_shadow(redis):
    return SignalEnsemble(
        redis_client=redis,
        symbol="BTCUSDT",
        mode="shadow",
        threshold=0.35,
        consensus_ratio=1.5,
    )


# ---------------------------------------------------------------------------
# SignalVote tests
# ---------------------------------------------------------------------------

class TestSignalVote:
    def test_valid_directions(self):
        for d in ("long", "short", "neutral"):
            v = SignalVote(source="test", direction=d, confidence=0.5)
            assert v.direction == d

    def test_invalid_direction_raises(self):
        with pytest.raises(ValueError, match="Invalid direction"):
            SignalVote(source="test", direction="up", confidence=0.5)

    def test_confidence_clamped(self):
        v = SignalVote(source="test", direction="long", confidence=1.5)
        assert v.confidence == 1.0

        v2 = SignalVote(source="test", direction="short", confidence=-0.3)
        assert v2.confidence == 0.0

    def test_default_veto_false(self):
        v = SignalVote(source="test", direction="neutral", confidence=0.0)
        assert v.veto is False

    def test_meta_default_empty(self):
        v = SignalVote(source="test", direction="neutral", confidence=0.0)
        assert v.meta == {}


# ---------------------------------------------------------------------------
# EnsembleDecision tests
# ---------------------------------------------------------------------------

class TestEnsembleDecision:
    def test_to_dict(self):
        d = EnsembleDecision(action="long", score=0.65, reason="test")
        result = d.to_dict()
        assert result["action"] == "long"
        assert result["score"] == 0.65
        assert result["reason"] == "test"


# ---------------------------------------------------------------------------
# SignalEnsemble.vote() tests
# ---------------------------------------------------------------------------

class TestEnsembleVote:
    def test_all_neutral_returns_skip(self, ensemble):
        """All neutral votes should produce skip."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="neutral", confidence=0.0),
            "ta_indicators": SignalVote(source="ta_indicators", direction="neutral", confidence=0.0),
            "microstructure": SignalVote(source="microstructure", direction="neutral", confidence=0.0),
            "regime_filter": SignalVote(source="regime_filter", direction="neutral", confidence=0.0),
        }
        decision = ensemble.vote(signals)
        assert decision.action == "skip"
        assert "no_consensus" in decision.reason

    def test_all_long_above_threshold(self, ensemble):
        """All sources voting long with high confidence → long decision."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.8),
            "ta_indicators": SignalVote(source="ta_indicators", direction="long", confidence=0.7),
            "microstructure": SignalVote(source="microstructure", direction="long", confidence=0.6),
            "regime_filter": SignalVote(source="regime_filter", direction="long", confidence=0.5),
        }
        decision = ensemble.vote(signals)
        assert decision.action == "long"
        assert decision.score > 0.35
        assert "long_consensus" in decision.reason

    def test_all_short_above_threshold(self, ensemble):
        """All sources voting short → short decision."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="short", confidence=0.8),
            "ta_indicators": SignalVote(source="ta_indicators", direction="short", confidence=0.7),
            "microstructure": SignalVote(source="microstructure", direction="short", confidence=0.6),
            "regime_filter": SignalVote(source="regime_filter", direction="short", confidence=0.5),
        }
        decision = ensemble.vote(signals)
        assert decision.action == "short"
        assert decision.score > 0.35

    def test_veto_blocks_signal(self, ensemble):
        """Even one veto should block the signal."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
            "ta_indicators": SignalVote(source="ta_indicators", direction="long", confidence=0.8),
            "microstructure": SignalVote(source="microstructure", direction="long", confidence=0.7),
            "regime_filter": SignalVote(source="regime_filter", direction="neutral", confidence=0.0, veto=True),
        }
        decision = ensemble.vote(signals)
        assert decision.action == "skip"
        assert "veto" in decision.reason
        assert "regime_filter" in decision.reason

    def test_multiple_vetos(self, ensemble):
        """Multiple vetos listed in reason."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
            "ta_indicators": SignalVote(source="ta_indicators", direction="neutral", confidence=0.0, veto=True),
            "microstructure": SignalVote(source="microstructure", direction="neutral", confidence=0.0, veto=True),
            "regime_filter": SignalVote(source="regime_filter", direction="long", confidence=0.5),
        }
        decision = ensemble.vote(signals)
        assert decision.action == "skip"
        assert "ta_indicators" in decision.reason
        assert "microstructure" in decision.reason

    def test_mixed_signals_no_consensus(self, ensemble):
        """Mixed long/short signals without clear majority → skip."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.5),
            "ta_indicators": SignalVote(source="ta_indicators", direction="short", confidence=0.5),
            "microstructure": SignalVote(source="microstructure", direction="neutral", confidence=0.0),
            "regime_filter": SignalVote(source="regime_filter", direction="neutral", confidence=0.0),
        }
        decision = ensemble.vote(signals)
        # long_score = 0.5 * 0.25 = 0.125
        # short_score = 0.5 * 0.25 = 0.125
        # Neither exceeds threshold 0.35 → skip
        assert decision.action == "skip"

    def test_consensus_ratio_enforced(self, ensemble):
        """Long score must exceed short score × ratio."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
            "ta_indicators": SignalVote(source="ta_indicators", direction="short", confidence=0.6),
            "microstructure": SignalVote(source="microstructure", direction="long", confidence=0.3),
            "regime_filter": SignalVote(source="regime_filter", direction="neutral", confidence=0.0),
        }
        decision = ensemble.vote(signals)
        # long_score = 0.9*0.25 + 0.3*0.25 = 0.30
        # short_score = 0.6*0.25 = 0.15
        # 0.30 < 0.35 threshold → skip
        assert decision.action == "skip"

    def test_missing_source_treated_as_unavailable(self, ensemble):
        """Missing sources are simply unavailable, not a veto."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
            "ta_indicators": None,  # unavailable
            "microstructure": SignalVote(source="microstructure", direction="long", confidence=0.8),
            "regime_filter": SignalVote(source="regime_filter", direction="long", confidence=0.7),
        }
        decision = ensemble.vote(signals)
        # 3 sources voting long: 0.9*0.25 + 0.8*0.25 + 0.7*0.25 = 0.60 > 0.35
        assert decision.action == "long"
        assert decision.votes.get("ta_indicators", {}).get("status") == "unavailable"

    def test_shadow_mode_never_blocks(self, ensemble_shadow):
        """Shadow mode decisions should always have shadow=True."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.8),
        }
        decision = ensemble_shadow.vote(signals)
        assert decision.shadow is True

    def test_enforce_mode_not_shadow(self, ensemble):
        """Enforce mode decisions should have shadow=False."""
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.8),
        }
        decision = ensemble.vote(signals)
        assert decision.shadow is False


# ---------------------------------------------------------------------------
# Dynamic weights from Redis
# ---------------------------------------------------------------------------

class TestDynamicWeights:
    def test_uses_redis_weights(self, redis):
        """Weights from Redis should override defaults."""
        redis._hash_data["weights:ensemble:BTCUSDT"] = {
            "orderflow": "0.6",
            "ta_indicators": "0.1",
            "microstructure": "0.2",
            "regime_filter": "0.1",
        }
        ens = SignalEnsemble(
            redis_client=redis,
            symbol="BTCUSDT",
            mode="enforce",
            threshold=0.20,
        )
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.8),
            "ta_indicators": SignalVote(source="ta_indicators", direction="neutral", confidence=0.0),
            "microstructure": SignalVote(source="microstructure", direction="long", confidence=0.5),
            "regime_filter": SignalVote(source="regime_filter", direction="neutral", confidence=0.0),
        }
        decision = ens.vote(signals)
        # long_score = 0.8*0.6 + 0.5*0.2 = 0.58 > 0.20 threshold
        assert decision.action == "long"
        assert decision.score > 0.5

    def test_fallback_equal_weights_on_redis_error(self):
        """When Redis is broken, should fall back to equal weights."""
        broken_redis = MagicMock()
        broken_redis.hgetall.side_effect = Exception("connection refused")
        broken_redis.get.return_value = None
        broken_redis.set.return_value = None
        broken_redis.xadd.return_value = None

        ens = SignalEnsemble(
            redis_client=broken_redis,
            symbol="BTCUSDT",
            mode="enforce",
            threshold=0.35,
        )
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
            "ta_indicators": SignalVote(source="ta_indicators", direction="long", confidence=0.8),
            "microstructure": SignalVote(source="microstructure", direction="long", confidence=0.7),
            "regime_filter": SignalVote(source="regime_filter", direction="long", confidence=0.6),
        }
        decision = ens.vote(signals)
        # Should still work with default weights
        assert decision.action == "long"

    def test_per_symbol_threshold_from_redis(self, redis):
        """Per-symbol threshold override via Redis."""
        redis._data["threshold:ensemble:BTCUSDT"] = "0.5"
        ens = SignalEnsemble(
            redis_client=redis,
            symbol="BTCUSDT",
            mode="enforce",
            threshold=0.2,  # constructor default
        )
        signals = {
            "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
            "ta_indicators": SignalVote(source="ta_indicators", direction="long", confidence=0.5),
        }
        decision = ens.vote(signals)
        # With default weights: long_score = 0.9*0.25 + 0.5*0.25 = 0.35 < 0.5 (Redis threshold)
        assert decision.action == "skip"


# ---------------------------------------------------------------------------
# Source builder helpers
# ---------------------------------------------------------------------------

class TestBuildOrderflowVote:
    def test_long_vote(self):
        vote = build_orderflow_vote(0.8, "LONG", {"z_delta": 3.5})
        assert vote.source == "orderflow"
        assert vote.direction == "long"
        assert vote.confidence == 0.8

    def test_short_vote(self):
        vote = build_orderflow_vote(0.7, "SHORT", {})
        assert vote.direction == "short"

    def test_neutral_when_no_side(self):
        vote = build_orderflow_vote(0.5, None, {})
        assert vote.direction == "neutral"

    def test_neutral_when_low_conf(self):
        vote = build_orderflow_vote(0.01, "LONG", {})
        assert vote.direction == "neutral"


class TestBuildTaVote:
    def test_no_ta_keys(self):
        redis = FakeRedis()
        vote = build_ta_vote(redis, "BTCUSDT")
        assert vote.source == "ta_indicators"
        assert vote.direction == "neutral"
        assert vote.meta.get("status") == "no_ta_keys"

    def test_bullish_rsi(self):
        redis = FakeRedis({
            "ta:last:rsi:BTCUSDT": json.dumps({"rsi": 25.0}),
        })
        vote = build_ta_vote(redis, "BTCUSDT")
        assert vote.direction == "long"
        assert vote.confidence > 0

    def test_bearish_rsi(self):
        redis = FakeRedis({
            "ta:last:rsi:BTCUSDT": json.dumps({"rsi": 75.0}),
        })
        vote = build_ta_vote(redis, "BTCUSDT")
        assert vote.direction == "short"


class TestBuildMicrostructureVote:
    def test_strong_long_impulse(self):
        vote = build_microstructure_vote(
            {"z_delta": 3.5, "z_speed": 2.0, "obi_avg": 0.5, "book_churn": 1.0},
            {},
            {"cluster_score": 50},
        )
        assert vote.direction == "long"
        assert vote.confidence > 0.3

    def test_high_churn_veto(self):
        vote = build_microstructure_vote(
            {"z_delta": 1.5, "z_speed": 0.5, "obi_avg": 0.1, "book_churn": 6.0},
            {},
            None,
        )
        assert vote.veto is True

    def test_neutral_weak_impulse(self):
        vote = build_microstructure_vote(
            {"z_delta": 0.3, "z_speed": 0.1, "obi_avg": 0.0, "book_churn": 0.5},
            {},
            None,
        )
        assert vote.direction == "neutral"


class TestBuildRegimeVote:
    def test_trending_with_side_hint(self):
        redis = FakeRedis({
            "regime:state:BTCUSDT": json.dumps({
                "label": "trending",
                "trend_score": 0.8,
                "range_score": 0.1,
            }),
        })
        vote = build_regime_vote(redis, "BTCUSDT", side_hint="LONG")
        assert vote.direction == "long"
        assert vote.confidence > 0.5
        assert vote.veto is False

    def test_range_regime_neutral(self):
        redis = FakeRedis({
            "regime:state:BTCUSDT": json.dumps({
                "label": "range",
                "trend_score": 0.2,
                "range_score": 0.7,
            }),
        })
        vote = build_regime_vote(redis, "BTCUSDT")
        assert vote.direction == "neutral"
        assert vote.veto is False

    def test_unknown_regime_veto(self):
        redis = FakeRedis({
            "regime:state:BTCUSDT": json.dumps({
                "label": "unknown",
                "trend_score": 0.0,
                "range_score": 0.0,
            }),
        })
        vote = build_regime_vote(redis, "BTCUSDT")
        assert vote.veto is True

    def test_no_regime_key_neutral(self):
        redis = FakeRedis()
        vote = build_regime_vote(redis, "BTCUSDT")
        assert vote.direction == "neutral"
        assert vote.veto is False


# ---------------------------------------------------------------------------
# Sharpe computation tests
# ---------------------------------------------------------------------------

class TestSharpeComputation:
    def test_positive_series(self):
        from services.ensemble_weight_calibrator import compute_sharpe_robust
        pnl = [0.01, 0.02, 0.015, 0.012, 0.018, 0.011, 0.013, 0.019, 0.017, 0.014,
               0.016, 0.012, 0.015, 0.013, 0.014, 0.018, 0.011, 0.016, 0.017, 0.013]
        sharpe = compute_sharpe_robust(pnl)
        assert sharpe > 0  # positive PnL series should have positive Sharpe

    def test_negative_series(self):
        from services.ensemble_weight_calibrator import compute_sharpe_robust
        pnl = [-0.01, -0.02, -0.015, -0.012, -0.018, -0.011, -0.013, -0.019, -0.017, -0.014,
               -0.016, -0.012, -0.015, -0.013, -0.014, -0.018, -0.011, -0.016, -0.017, -0.013]
        sharpe = compute_sharpe_robust(pnl)
        assert sharpe < 0  # negative PnL series should have negative Sharpe

    def test_insufficient_data(self):
        from services.ensemble_weight_calibrator import compute_sharpe_robust
        pnl = [0.01, 0.02, 0.03]  # less than MIN_OUTCOMES_FOR_WEIGHT
        sharpe = compute_sharpe_robust(pnl)
        assert sharpe == 0.0

    def test_constant_positive(self):
        from services.ensemble_weight_calibrator import compute_sharpe_robust
        pnl = [0.01] * 25
        sharpe = compute_sharpe_robust(pnl)
        assert sharpe == 10.0  # capped at max

    def test_sharpe_capped_at_ten(self):
        from services.ensemble_weight_calibrator import compute_sharpe_robust
        pnl = [0.01] * 25
        sharpe = compute_sharpe_robust(pnl)
        assert -10.0 <= sharpe <= 10.0


# ---------------------------------------------------------------------------
# Weight calibration result
# ---------------------------------------------------------------------------

class TestWeightCalibrationResult:
    def test_normalization(self):
        from services.ensemble_weight_calibrator import WeightCalibrationResult
        result = WeightCalibrationResult(
            symbol="BTCUSDT",
            weights={"orderflow": 0.5, "ta_indicators": 0.3, "microstructure": 0.15, "regime_filter": 0.05},
            sharpes={"orderflow": 2.0, "ta_indicators": 1.2, "microstructure": 0.6, "regime_filter": 0.2},
            outcome_counts={"orderflow": 50, "ta_indicators": 30, "microstructure": 25, "regime_filter": 20},
            previous_weights={"orderflow": 0.25, "ta_indicators": 0.25, "microstructure": 0.25, "regime_filter": 0.25},
        )
        assert abs(sum(result.weights.values()) - 1.0) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
