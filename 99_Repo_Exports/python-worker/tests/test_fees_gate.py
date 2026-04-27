import pytest
from core.fees_aware_policy import fees_aware_min_atr_bps

def test_fees_aware_min_atr_bps():
    th, meta = fees_aware_min_atr_bps(
        fees_bps_rt=8.0,
        tp_bps_buffer=6.0,
        tp1_share=0.5,
        rocket_mult=0.78,
    )
    assert meta["ok"] == 1
    assert th == pytest.approx(35.9, abs=0.1)

def test_fees_aware_dynamic_share():
    th, meta = fees_aware_min_atr_bps(
        fees_bps_rt=8.0,
        tp_bps_buffer=6.0,
        tp1_share=0.2,
        rocket_mult=0.78,
    )
    assert meta["ok"] == 1
    assert th == pytest.approx(89.74, abs=0.1)

def test_fees_aware_bad_denom():
    th, meta = fees_aware_min_atr_bps(
        fees_bps_rt=8.0,
        tp_bps_buffer=6.0,
        tp1_share=0.0,
        rocket_mult=0.78,
    )
    assert meta["ok"] == 0
    assert th == 0.0
