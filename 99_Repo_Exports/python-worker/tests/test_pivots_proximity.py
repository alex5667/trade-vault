import math
import pytest
from signals.pivots import is_near_level_atr


def _pivots():
    return {"P": 100.0, "R1": 110.0, "S1": 90.0}


def test_backward_compat_atr_only_behavior():
    piv = _pivots()
    # price=100.4, closest to P=100.0 => distance=0.4
    # atr=1.0, threshold=0.5 => 0.4 <= 0.5 => True
    res = is_near_level_atr(100.4, piv, atr=1.0, threshold=0.5)
    assert res is True

    # atr-only fail
    res2 = is_near_level_atr(100.6, piv, atr=1.0, threshold=0.5)
    assert res2 is False


def test_dist_bp_or_mode_passes_when_bps_is_ok():
    piv = _pivots()
    # Make ATR check very strict to force reliance on bps
    # distance=0.6, threshold*atr=0.1 => near_atr=False
    # bps ~ 0.6/100.6*10000 ~ 59.6 bps -> pass if thr>=60
    # Actually price=100.6, level=100.0. distance=0.6.
    # dist_bps(100.6, 100.0) = 0.6 / 100.6 * 10000 = 59.64
    
    passed, det = is_near_level_atr(
        100.6,
        piv,
        atr=1.0,
        threshold=0.1,
        dist_bp_threshold=60.0,
        mode="or",
        return_details=True,
    )
    assert passed is True
    assert det["near_atr"] is False
    assert det["near_bps"] is True
    assert det["dist_bps"] > 0


def test_dist_bp_and_mode_requires_both():
    piv = _pivots()
    # ATR strict => near_atr False, bps True => AND must fail
    passed, det = is_near_level_atr(
        100.6,
        piv,
        atr=1.0,
        threshold=0.1,
        dist_bp_threshold=80.0,
        mode="and",
        return_details=True,
    )
    assert passed is False
    assert det["near_bps"] is True
    assert det["near_atr"] is False
