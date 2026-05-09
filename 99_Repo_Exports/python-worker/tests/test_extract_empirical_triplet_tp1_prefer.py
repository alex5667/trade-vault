from services.stats_aggregator import extract_empirical_triplet


def test_extract_empirical_triplet_prefer_tp1_snapshot():
    # Arrange
    trade = {
        "regime": "trend",

        # Global MFE/MAE (end of trade)
        "mfe_pnl": 500.0,
        "mae_pnl": -200.0,

        # TP1 snapshot (earlier) - PREFERRED if tp1_hit=1
        "mfe_pnl_at_tp1": 100.0,
        "mae_pnl_before_tp1": -50.0,

        "tp1_hit": 1,

        # Helper fields for bps calc
        "entry_price": 10000,
        "qty": 1.0,  # notional = 10000
    }

    # Act
    res = extract_empirical_triplet(trade)

    # Assert
    # 100 / 10000 = 1% = 100 bps
    # 50 / 10000 = 0.5% = 50 bps
    # Should use the *at_tp1* values, not the global ones (500, -200)
    assert abs(res["mfe_bps"] - 100.0) < 0.1
    assert abs(res["mae_bps"] - 50.0) < 0.1

def test_extract_empirical_triplet_fallback_global():
    # Arrange
    trade = {
        "regime": "trend",
        "mfe_pnl": 200.0,
        "mae_pnl": -100.0,
        # No snapshot fields
        "tp1_hit": 0,
        "entry_price": 1000,
        "qty": 10.0, # notional 10000
    }

    # Act
    res = extract_empirical_triplet(trade)

    # Assert
    # 200/10000 = 200 bps
    # 100/10000 = 100 bps
    assert abs(res["mfe_bps"] - 200.0) < 0.1
    assert abs(res["mae_bps"] - 100.0) < 0.1
