from __future__ import annotations


def test_derive_fgh_rows_relative_velocity_replenishment():
    from ml_analysis.common.derived_fgh import derive_fgh_rows

    rows = [
        {
            "symbol": "BTCUSDT"
            "ts_ms": 1_000
            "indicators": {"ofi_ml_norm": 0.25, "lob_micro_shift_bps": 1.5}
        }
        {
            "symbol": "ETHUSDT"
            "ts_ms": 1_000
            "indicators": {
                "ofi_ml_norm": 0.10
                "lob_micro_shift_bps": 2.0
                "hawkes_taker_buy_lam": 9.0
                "hawkes_limit_add_ask_lam": 6.0
                "hawkes_taker_sell_lam": 7.0
                "hawkes_limit_add_bid_lam": 10.0
            }
        }
        {
            "symbol": "ETHUSDT"
            "ts_ms": 2_000
            "indicators": {"ofi_ml_wsum": 12.0, "lob_micro_shift_bps": 2.2}
        }
        {
            "symbol": "ETHUSDT"
            "ts_ms": 3_000
            "indicators": {"ofi_ml_wsum": 18.0, "lob_micro_shift_bps": 2.5}
        }
    ]

    rep = derive_fgh_rows(rows, leader_symbol="BTCUSDT", leader_max_lag_ms=10_000)
    assert rep.get("ok") is True

    eth0 = rows[1]["indicators"]
    # F) relative vs BTC
    assert abs(eth0.get("rel_ofi_ml_norm_btc") - (0.10 - 0.25)) < 1e-9
    assert abs(eth0.get("rel_lob_micro_shift_bps_btc") - (2.0 - 1.5)) < 1e-9

    # G) replenishment imbalance exists
    assert "ask_replenish_imb" in eth0
    assert "bid_replenish_imb" in eth0
    assert "lob_replenishment_pressure" in eth0

    # H) velocities exist on subsequent rows
    eth1 = rows[2]["indicators"]
    eth2 = rows[3]["indicators"]
    assert "ofi_ml_wsum_vel" in eth2  # needs prev
    assert "micro_shift_bps_vel" in eth2
    assert "ofi_ml_wsum_vel_z_ema" in eth2
    assert "micro_shift_bps_vel_z_ema" in eth2
