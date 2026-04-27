from core.slippage_model import expected_slippage_bps


def test_slippage_ok_low_spread():
    est = expected_slippage_bps(
        spread_bps=3.0,
        churn_score=0.1,
        book_rate_z=0.0,
        pressure_sps=0.05,
        atr_bps=12.0,
        cfg={"slippage_max_bps": 18.0, "slippage_shadow_only_bps": 12.0},
    )
    assert est.ok is True
    assert est.expected_bps >= 0


def test_slippage_veto_high_spread():
    est = expected_slippage_bps(
        spread_bps=40.0,
        churn_score=3.0,
        book_rate_z=-4.0,
        pressure_sps=1.0,
        atr_bps=2.0,
        cfg={"slippage_max_bps": 18.0, "slippage_shadow_only_bps": 12.0},
    )
    assert est.ok is False
    assert est.reason == "SLIPPAGE_TOO_HIGH"
