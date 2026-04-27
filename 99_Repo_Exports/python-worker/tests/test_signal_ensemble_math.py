# -*- coding: utf-8 -*-
"""
Regression: Signal Ensemble — pure-math weighted voting invariants (merge-blocker).

Tests:
  - Weighted consensus: long_score > threshold AND > short_score * ratio → long
  - Veto from any source → skip
  - All neutral → skip
  - Equal vs dynamic weights produce expected decisions
  - Boundary: exactly at threshold → skip (not >=, must be >)
  - Shadow mode flag propagation

Run:
    cd python-worker && python -m pytest tests/test_signal_ensemble_math.py -v
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock

from services.signal_ensemble import (
    SignalVote,
    EnsembleDecision,
    SignalEnsemble,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeRedisMinimal:
    """Minimal Redis stub for ensemble tests."""

    def __init__(self, weights: dict = None, threshold: float = None):
        self._hash_data = {}
        self._data = {}
        self._stream_data = {}
        if weights:
            self._hash_data = weights
        if threshold is not None:
            self._data = {"threshold": str(threshold)}

    def hgetall(self, key):
        return self._hash_data.get(key, {})

    def get(self, key):
        if "threshold" in key:
            return self._data.get("threshold")
        return self._data.get(key)

    def set(self, key, value, **kwargs):
        self._data[key] = value

    def xadd(self, key, fields, **kwargs):
        return "1-1"


def _vote(source: str, direction: str, confidence: float, veto: bool = False) -> SignalVote:
    return SignalVote(source=source, direction=direction, confidence=confidence, veto=veto)


def _ensemble(
    redis=None,
    symbol="BTCUSDT",
    mode="shadow",
    threshold=0.35,
    consensus_ratio=1.5,
) -> SignalEnsemble:
    r = redis or FakeRedisMinimal()
    return SignalEnsemble(r, symbol, mode=mode, threshold=threshold, consensus_ratio=consensus_ratio)


# ---------------------------------------------------------------------------
# Weighted scoring invariants
# ---------------------------------------------------------------------------

class TestWeightedScoring:
    def test_all_long_above_threshold(self) -> None:
        """All sources vote long with high confidence → long action."""
        e = _ensemble(threshold=0.30)
        signals = {
            "orderflow": _vote("orderflow", "long", 0.8),
            "ta_indicators": _vote("ta_indicators", "long", 0.7),
            "microstructure": _vote("microstructure", "long", 0.6),
            "regime_filter": _vote("regime_filter", "long", 0.5),
        }
        d = e.vote(signals)
        assert d.action == "long"
        assert d.score > 0.30

    def test_all_short_above_threshold(self) -> None:
        e = _ensemble(threshold=0.30)
        signals = {
            "orderflow": _vote("orderflow", "short", 0.8),
            "ta_indicators": _vote("ta_indicators", "short", 0.7),
            "microstructure": _vote("microstructure", "short", 0.6),
            "regime_filter": _vote("regime_filter", "short", 0.5),
        }
        d = e.vote(signals)
        assert d.action == "short"
        assert d.score > 0.30

    def test_all_neutral_produces_skip(self) -> None:
        e = _ensemble()
        signals = {
            "orderflow": _vote("orderflow", "neutral", 0.0),
            "ta_indicators": _vote("ta_indicators", "neutral", 0.0),
            "microstructure": _vote("microstructure", "neutral", 0.0),
            "regime_filter": _vote("regime_filter", "neutral", 0.0),
        }
        d = e.vote(signals)
        assert d.action == "skip"

    def test_conflicting_directions_no_consensus(self) -> None:
        """Equal long and short confidence → skip (no consensus ratio met)."""
        e = _ensemble(threshold=0.20, consensus_ratio=1.5)
        signals = {
            "orderflow": _vote("orderflow", "long", 0.5),
            "ta_indicators": _vote("ta_indicators", "short", 0.5),
            "microstructure": _vote("microstructure", "neutral", 0.0),
            "regime_filter": _vote("regime_filter", "neutral", 0.0),
        }
        d = e.vote(signals)
        # long_score = 0.5*0.25 = 0.125, short_score = 0.5*0.25 = 0.125
        # Neither long > short*1.5 nor short > long*1.5 → skip
        assert d.action == "skip"


# ---------------------------------------------------------------------------
# Veto logic
# ---------------------------------------------------------------------------

class TestVetoLogic:
    def test_single_veto_blocks(self) -> None:
        e = _ensemble()
        signals = {
            "orderflow": _vote("orderflow", "long", 0.9),
            "ta_indicators": _vote("ta_indicators", "long", 0.9),
            "microstructure": _vote("microstructure", "long", 0.9, veto=True),
            "regime_filter": _vote("regime_filter", "long", 0.9),
        }
        d = e.vote(signals)
        assert d.action == "skip"
        assert "veto" in d.reason

    def test_multiple_veto_reports_all(self) -> None:
        e = _ensemble()
        signals = {
            "orderflow": _vote("orderflow", "long", 0.9, veto=True),
            "ta_indicators": _vote("ta_indicators", "long", 0.9),
            "microstructure": _vote("microstructure", "long", 0.9, veto=True),
            "regime_filter": None,
        }
        d = e.vote(signals)
        assert d.action == "skip"
        assert "orderflow" in d.reason
        assert "microstructure" in d.reason


# ---------------------------------------------------------------------------
# Threshold boundary
# ---------------------------------------------------------------------------

class TestThresholdBoundary:
    def test_exactly_at_threshold_produces_skip(self) -> None:
        """Score must be strictly > threshold, not >=."""
        # threshold=0.25, with 1 source voting long at conf=1.0, weight=0.25
        # long_score = 1.0 * 0.25 = 0.25. With threshold=0.25, 0.25 > 0.25 is False → skip
        e = _ensemble(threshold=0.25)
        signals = {
            "orderflow": _vote("orderflow", "long", 1.0),
            "ta_indicators": None,
            "microstructure": None,
            "regime_filter": None,
        }
        d = e.vote(signals)
        assert d.action == "skip", f"Expected skip at exact threshold, got {d.action}"

    def test_just_above_threshold_produces_action(self) -> None:
        # threshold=0.24, same setup → long_score=0.25 > 0.24 = True
        e = _ensemble(threshold=0.24)
        signals = {
            "orderflow": _vote("orderflow", "long", 1.0),
            "ta_indicators": None,
            "microstructure": None,
            "regime_filter": None,
        }
        d = e.vote(signals)
        assert d.action == "long"


# ---------------------------------------------------------------------------
# Shadow mode flag
# ---------------------------------------------------------------------------

class TestShadowMode:
    def test_shadow_mode_flags_decision(self) -> None:
        e = _ensemble(mode="shadow")
        d = e.vote({"orderflow": _vote("orderflow", "long", 0.9)})
        assert d.shadow is True

    def test_enforce_mode_flags_decision(self) -> None:
        e = _ensemble(mode="enforce")
        d = e.vote({"orderflow": _vote("orderflow", "long", 0.9)})
        assert d.shadow is False


# ---------------------------------------------------------------------------
# Dynamic weights fallback
# ---------------------------------------------------------------------------

class TestDynamicWeights:
    def test_equal_weights_when_no_redis_data(self) -> None:
        """When Redis has no weights, each source gets DEFAULT_WEIGHT (0.25)."""
        e = _ensemble()
        w = e._get_dynamic_weights()
        for src in SignalEnsemble.SOURCES:
            assert w[src] == pytest.approx(0.25)

    def test_custom_weights_from_redis(self) -> None:
        """Dynamic weights override default."""
        r = FakeRedisMinimal(weights={
            "weights:ensemble:BTCUSDT": {
                "orderflow": "0.4",
                "ta_indicators": "0.3",
                "microstructure": "0.2",
                "regime_filter": "0.1",
            },
        })
        e = _ensemble(redis=r, symbol="BTCUSDT")
        w = e._get_dynamic_weights()
        assert w["orderflow"] == pytest.approx(0.4)
        assert w["ta_indicators"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Missing source (None) handling
# ---------------------------------------------------------------------------

class TestMissingSources:
    def test_none_source_ignored(self) -> None:
        e = _ensemble(threshold=0.10)
        signals = {
            "orderflow": _vote("orderflow", "long", 0.9),
            "ta_indicators": None,
            "microstructure": None,
            "regime_filter": None,
        }
        d = e.vote(signals)
        # Only orderflow contributes: 0.9 * 0.25 = 0.225 > 0.10 → long
        assert d.action == "long"
        assert "ta_indicators" in d.votes
        assert d.votes["ta_indicators"]["status"] == "unavailable"
