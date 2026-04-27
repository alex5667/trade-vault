from core.fees_aware_policy import fees_aware_min_atr_bps


def test_fees_aware_basic():
    th, reason = fees_aware_min_atr_bps(
        fees_bps_rt=8.0,
        tp_bps_buffer=6.0,
        tp1_share=0.5,
        rocket_mult=1.0,
    )
    # (8+6)/(0.5*1)=28
    assert abs(th - 28.0) < 1e-9
    assert reason == "ok"


def test_fees_aware_fail_open_on_zero_denom():
    th, reason = fees_aware_min_atr_bps(
        fees_bps_rt=8.0,
        tp_bps_buffer=6.0,
        tp1_share=0.0,
        rocket_mult=1.0,
    )
    assert th == 0.0
    assert reason == "bad_denominator"
