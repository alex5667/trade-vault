from core.liquidity_regime_service import LiquidityRegimeService


def test_liq_regime_stressed_on_multiple_factors():
    # To get stressed regime (< 0.35), we need poor spread, depth, and rate
    svc = LiquidityRegimeService(symbol="BTCUSDT", cfg={"dist_bp_threshold": 5.0, "liq_spread_crit_mult": 8.0})
    snap = svc.update(
        ts_ms=1,
        spread_bps=80.0,            # 0.0 score (crit)
        depth_min_5_usd=50_000.0,   # < 80k (crit for majors) -> 0.0 score
        book_rate_hz=0.05,          # < 0.1 (crit) -> 0.0 score
    )
    assert snap.score <= 0.35
    assert snap.regime == "stressed"


def test_liq_regime_thin_when_score_low():
    svc = LiquidityRegimeService(symbol="BTCUSDT", cfg={"liq_thin_score": 0.60, "liq_stressed_score": 0.35, "dist_bp_threshold": 5.0})
    snap = svc.update(
        ts_ms=1,
        spread_bps=20.0,
        depth_min_5_usd=70_000.0,
        book_rate_hz=10.0,
    )
    assert snap.regime in ("thin", "stressed")
