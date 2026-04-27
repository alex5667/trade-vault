from services.stats_aggregator import extract_empirical_triplet


def test_extract_empirical_triplet_uses_bps_if_present():
    tc = {
        "regime": "trend",
        "mfe_bps": 55.0,
        "mae_bps": 22.0,
        "tp1_hit": 1,
        "entry_ts_ms": 1000,
        "tp1_hit_ts_ms": 7000,
    }
    out = extract_empirical_triplet(tc)
    assert out["regime"] == "trend"
    assert abs(out["mfe_bps"] - 55.0) < 1e-9
    assert abs(out["mae_bps"] - 22.0) < 1e-9
    assert out["ttd_tp1_ms"] == 6000


def test_extract_empirical_triplet_converts_pnl_to_bps():
    # Notional = qty * entry_price = 2 * 100 = 200
    # mfe_pnl=1.0 => 1/200*10000 = 50 bps
    # mae_pnl=0.4 => 0.4/200*10000 = 20 bps
    tc = {
        "regime_name": "range",
        "entry_price": 100.0,
        "qty": 2.0,
        "mfe_pnl": 1.0,
        "mae_pnl": 0.4,
        "tp1_hit": 0,
    }
    out = extract_empirical_triplet(tc)
    assert out["regime"] == "range"
    assert abs(out["mfe_bps"] - 50.0) < 1e-6
    assert abs(out["mae_bps"] - 20.0) < 1e-6
    assert out["ttd_tp1_ms"] == 0
