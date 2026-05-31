from services.orderflow.sentiment_context_gate import evaluate_sentiment_context


def test_extreme_greed_reduces_risk_no_veto():
    dec = evaluate_sentiment_context(
        profile="strict",
        side="BUY",
        sentiment_regime="extreme_greed",
        fear_greed_value=82,
        fear_greed_delta_1d=5,
        fear_greed_delta_7d=20,
        base_risk_multiplier=0.70,
        tighten_cap_bps=2.0,
    )

    assert dec.veto is False
    assert dec.risk_multiplier == 0.70
    assert dec.tighten_add_bps > 0
    assert "sentiment_extreme_greed" in dec.flags

def test_neutral_has_no_effect():
    dec = evaluate_sentiment_context(
        profile="strict",
        side="BUY",
        sentiment_regime="neutral",
        fear_greed_value=50,
        fear_greed_delta_1d=1,
        fear_greed_delta_7d=2,
        base_risk_multiplier=1.0,
        tighten_cap_bps=2.0,
    )

    assert dec.veto is False
    assert dec.risk_multiplier == 1.0
    assert dec.tighten_add_bps == 0.0

def test_monitor_profile_no_tighten():
    dec = evaluate_sentiment_context(
        profile="monitor",
        side="SELL",
        sentiment_regime="extreme_fear",
        fear_greed_value=15,
        fear_greed_delta_1d=-15,
        fear_greed_delta_7d=-30,
        base_risk_multiplier=0.70,
        tighten_cap_bps=2.0,
    )

    assert dec.veto is False
    assert dec.risk_multiplier == 0.70
    assert dec.tighten_add_bps == 0.0
    assert "sentiment_extreme_fear" in dec.flags
    assert "sentiment_fast_fear_shift" in dec.flags
