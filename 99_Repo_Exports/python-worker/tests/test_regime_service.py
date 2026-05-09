# test_regime_service.py
"""
Tests for MarketRegimeService.
"""

import pytest

from handlers.regime_service import MarketRegimeService, RegimeConfig, RegimeFeatures, regime_label_to_enum


def test_regime_label_to_enum():
    """Test regime label to enum conversion."""
    assert regime_label_to_enum("trend") == "TREND"
    assert regime_label_to_enum("range") == "RANGE"
    assert regime_label_to_enum("mixed") == "MIXED"
    assert regime_label_to_enum("") == "UNKNOWN"
    assert regime_label_to_enum("unknown") == "UNKNOWN"
    assert regime_label_to_enum("trending") == "TREND"
    assert regime_label_to_enum("ranging") == "RANGE"
    assert regime_label_to_enum("mean_reversion") == "RANGE"


def test_update_regime_trend():
    """Test regime classification for trend conditions."""
    svc = MarketRegimeService(RegimeConfig(score_hi=0.2, score_lo=-0.2))
    f = RegimeFeatures(
        atr_q=0.9, delta_ema=+3.0, hold_side_score=+0.8, vwap_cross_rate=0.0
    )
    svc.update_regime(f)
    st = svc.get_current_regime()
    assert st.regime in ("trend", "trending_bull", "trending_bear", "mixed")
    assert st.score > 0.0
    assert st.confidence > 0.0


def test_update_regime_range():
    """Test regime classification for range conditions."""
    svc = MarketRegimeService(RegimeConfig(score_hi=0.2, score_lo=-0.2))
    f = RegimeFeatures(
        atr_q=0.2, delta_ema=0.0, hold_side_score=0.0, vwap_cross_rate=0.5
    )
    svc.update_regime(f)
    st = svc.get_current_regime()
    assert st.regime in ("range", "mixed")
    assert st.score <= 0.2
    assert st.confidence >= 0.0


def test_update_regime_mixed():
    """Test regime classification for mixed conditions."""
    svc = MarketRegimeService(RegimeConfig(score_hi=0.3, score_lo=-0.3))
    f = RegimeFeatures(
        atr_q=0.5, delta_ema=0.1, hold_side_score=0.0, vwap_cross_rate=0.1
    )
    svc.update_regime(f)
    st = svc.get_current_regime()
    assert st.regime == "mixed"
    assert -0.3 < st.score < 0.3


def test_regime_config_defaults():
    """Test default regime configuration."""
    cfg = RegimeConfig()
    assert cfg.score_hi == 0.35
    assert cfg.score_lo == -0.35
    assert cfg.atr_q_hi == 0.70
    assert cfg.atr_q_lo == 0.35
    assert cfg.w_atr == 0.35
    assert cfg.w_adx == 0.20
    assert cfg.w_delta == 0.25
    assert cfg.w_hold == 0.25
    assert cfg.w_ping == 0.15


def test_regime_features_defaults():
    """Test default regime features."""
    f = RegimeFeatures()
    assert f.atr_q == 0.5
    assert f.delta_ema == 0.0
    assert f.hold_side_score == 0.0
    assert f.vwap_cross_rate == 0.0
    assert f.vwap == 0.0
    assert f.open_day == 0.0
    assert isinstance(f.volume_profile, dict)


def test_regime_state_defaults():
    """Test default regime state."""
    svc = MarketRegimeService()
    st = svc.get_current_regime()
    assert st.regime == "unknown"
    assert st.confidence == 0.0
    assert st.score == 0.0
    assert st.last_update == 0.0  # not updated yet

    # After update, last_update should be set
    f = RegimeFeatures()
    svc.update_regime(f)
    st = svc.get_current_regime()
    assert st.last_update > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
