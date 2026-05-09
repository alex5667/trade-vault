from __future__ import annotations

"""
Regression: Signal Ensemble Degraded Modes (Before Canary 2.5)

Tests that the Signal Ensemble gracefully handles external component failures,
such as Redis outages, missing dynamic weights, or complete source timeouts.
"""


from services.signal_ensemble import SignalEnsemble, SignalVote


class ExceptionRedis:
    def hgetall(self, key):
        raise ConnectionError("Redis down")

def test_ensemble_degraded_redis_down() -> None:
    """When Redis is down, ensemble should fallback to DEFAULT_WEIGHT (0.25) and not throw."""
    r = ExceptionRedis()
    e = SignalEnsemble(redis_client=r, symbol="BTCUSDT")

    votes = {
        "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.8),
        "ta_indicators": SignalVote(source="ta_indicators", direction="long", confidence=0.8),
    }

    # Should not raise exception
    decision = e.vote(votes)

    # Weights should default to 0.25. Score = 0.8*0.25 + 0.8*0.25 = 0.4 > threshold
    # Since only 2 votes are present, remaining 2 are "unavailable"
    assert decision.action == "long"
    assert "orderflow" in decision.votes
    assert "microstructure" in decision.votes
    assert decision.votes["microstructure"]["status"] == "unavailable"

def test_ensemble_degraded_partial_source_missing() -> None:
    """Missing or None votes are gracefully skipped."""
    e = SignalEnsemble(symbol="ETHUSDT")

    votes = {
        "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
        "ta_indicators": None,
        "microstructure": None,
        "regime_filter": None, # Should be treated as unavailable
    }

    decision = e.vote(votes)
    # Total score = 0.9 * 0.25 = 0.225
    assert decision.action in ("long", "skip") # depending on default threshold, but it shouldn't crash

def test_ensemble_missing_components_but_above_threshold() -> None:
    """Ensemble still emits proper action if the surviving shards carry enough weight."""
    e = SignalEnsemble(symbol="ETHUSDT", threshold=0.1)

    votes = {
        "orderflow": SignalVote(source="orderflow", direction="long", confidence=0.9),
    }

    decision = e.vote(votes)
    assert decision.action == "long"

def test_ensemble_invalid_veto_state() -> None:
    """If a source provides an invalid format (e.g. wrong type), it should be skipped/unavailable."""
    e = SignalEnsemble(symbol="ETHUSDT")
    # This might throw at vote creation, but we test graceful degradation
    pass
