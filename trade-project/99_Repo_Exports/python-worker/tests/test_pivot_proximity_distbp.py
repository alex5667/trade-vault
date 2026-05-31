from signals.pivots import PivotProximityCfg, check_pivot_proximity, is_near_level_atr


def _pivots():
    return {"P": 100.0, "R1": 102.0, "S1": 98.0}


def test_backward_compat_atr_only_true():
    piv = _pivots()
    assert is_near_level_atr(100.4, piv, atr=1.0, threshold=0.5) is True


def test_backward_compat_atr_only_false():
    piv = _pivots()
    assert is_near_level_atr(100.6, piv, atr=1.0, threshold=0.5) is False


def test_or_mode_passes_by_bps():
    piv = _pivots()
    cfg = PivotProximityCfg(dist_atr_threshold=0.1, dist_bp_threshold=60.0, dist_mode="or")
    passed, det = check_pivot_proximity(100.6, piv, atr=1.0, cfg=cfg, return_details=True)
    assert passed is True
    assert det["near_atr"] is False
    assert det["near_bps"] is True
    assert det["closest_key"] in ("P", "R1", "S1")


def test_and_mode_requires_both():
    piv = _pivots()
    cfg = PivotProximityCfg(dist_atr_threshold=0.1, dist_bp_threshold=80.0, dist_mode="and")
    passed, det = check_pivot_proximity(100.6, piv, atr=1.0, cfg=cfg, return_details=True)
    assert passed is False
    assert det["near_bps"] is True
    assert det["near_atr"] is False


def test_bps_can_work_without_atr_if_enabled():
    piv = _pivots()
    cfg = PivotProximityCfg(dist_atr_threshold=0.5, dist_bp_threshold=80.0, dist_mode="or")
    passed, det = check_pivot_proximity(100.1, piv, atr=None, cfg=cfg, return_details=True)
    assert passed is True
    assert det["near_bps"] is True
