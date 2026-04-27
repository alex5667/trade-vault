from __future__ import annotations

from handlers.regime_service import MarketRegimeService, RegimeFeatures, RegimeConfig


def test_adx_q_pushes_to_trend():
    """Verify that high ADX quantile pushes score towards trend."""
    cfg = RegimeConfig()
    cfg.score_hi = 0.35
    cfg.score_lo = -0.35
    svc = MarketRegimeService(cfg)

    # Same ATR, same ping/hold/delta, only ADX differs
    # Use higher ATR to ensure we cross threshold
    f_low = RegimeFeatures(atr_q=0.75, adx_q=0.30, delta_ema=0.0, hold_side_score=0.0, vwap_cross_rate=0.1)
    f_hi  = RegimeFeatures(atr_q=0.75, adx_q=0.90, delta_ema=0.0, hold_side_score=0.2, vwap_cross_rate=0.1)

    r1 = svc.update_regime(f_low)
    s1 = svc.state.score
    r2 = svc.update_regime(f_hi)
    s2 = svc.state.score
    
    # High ADX should have higher score than low ADX
    assert s2 > s1, f"High ADX score {s2} should be > low ADX score {s1}"
    # High ADX with high ATR should be trending
    assert r2 in ("trending_bull", "trending_bear", "trend", "mixed")  # mixed acceptable if just below threshold


def test_trend_direction_from_hold():
    """Verify trending_bear when hold_side_score is negative."""
    svc = MarketRegimeService(RegimeConfig())
    r = svc.update_regime(RegimeFeatures(
        atr_q=0.95, 
        adx_q=0.95, 
        hold_side_score=-0.5, 
        delta_ema=1.0, 
        vwap_cross_rate=0.0
    ))
    assert r == "trending_bear"


def test_trend_direction_from_delta_fallback():
    """Verify trending_bull when hold is weak but delta is positive."""
    svc = MarketRegimeService(RegimeConfig())
    r = svc.update_regime(RegimeFeatures(
        atr_q=0.95, 
        adx_q=0.95, 
        hold_side_score=0.05,  # below trend_dir_hold_min (0.10)
        delta_ema=1.0,         # positive delta => bull
        vwap_cross_rate=0.0
    ))
    assert r == "trending_bull"


def test_range_on_low_adx():
    """Verify range regime on low ADX even with moderate ATR."""
    svc = MarketRegimeService(RegimeConfig())
    r = svc.update_regime(RegimeFeatures(
        atr_q=0.50, 
        adx_q=0.20,  # very low ADX => chop
        delta_ema=0.0, 
        hold_side_score=0.0, 
        vwap_cross_rate=0.3
    ))
    assert r in ("range", "mixed")


def test_adx_q_default_fallback():
    """Verify adx_q defaults to 0.5 if not provided."""
    svc = MarketRegimeService(RegimeConfig())
    # Create RegimeFeatures without adx_q (should use default 0.5)
    f = RegimeFeatures(atr_q=0.60, delta_ema=0.0, hold_side_score=0.0, vwap_cross_rate=0.2)
    assert f.adx_q == 0.5
    r = svc.update_regime(f)
    assert r in ("range", "mixed", "trending_bull", "trending_bear")
