def test_extract_empirical_triplet_prefers_entry_regime():
    from services.stats_aggregator import extract_empirical_triplet

    trade_closed = {
        "entry_regime": "RANGE",
        "regime": "TREND",
        "entry_price": 100.0,
        "qty": 1.0,
        "notional_usd": 100.0,
        "mfe_pnl": 0.3,  # 30 bps
        "mae_pnl": 0.1,  # 10 bps
        "tp1_hit": 1,
        "tp1_hit_ts_ms": 2000,
        "entry_ts_ms": 1000,
    }
    emp = extract_empirical_triplet(trade_closed)
    assert emp["regime"] == "range"
    assert emp["mfe_bps"] is not None and emp["mfe_bps"] > 0
    assert emp["mae_bps"] is not None and emp["mae_bps"] > 0
    assert emp["ttd_tp1_ms"] == 1000
