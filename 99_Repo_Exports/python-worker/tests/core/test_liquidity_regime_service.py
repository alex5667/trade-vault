from core.liquidity_regime_service import LiquidityRegimeService


def test_liquidity_regime_service_basic():
    cfg = {
        "dist_bp_threshold": 12.0,
        "book_rate_min_hz": 50.0,
        "book_rate_warn_hz": 30.0,
        "dn_tier1_usd": 400_000.0,
        "liq_thin_score": 0.60,
        "liq_stressed_score": 0.35,
    }
    s = LiquidityRegimeService(symbol="BTCUSDT", cfg=cfg)

    # Good liquidity -> normal
    ev = s.update(ts_ms=1_000, spread_bps=10.0, depth_min_5_usd=900_000.0, book_rate_hz=60.0)
    assert ev.regime == "normal"
    assert 0.0 <= ev.score <= 1.0

    # Thin
    ev = s.update(ts_ms=2_000, spread_bps=25.0, depth_min_5_usd=120_000.0, book_rate_hz=18.0)
    assert ev.regime in ("thin", "stressed")

    # Stressed
    ev = s.update(ts_ms=3_000, spread_bps=60.0, depth_min_5_usd=20_000.0, book_rate_hz=5.0)
    assert ev.regime == "stressed"
