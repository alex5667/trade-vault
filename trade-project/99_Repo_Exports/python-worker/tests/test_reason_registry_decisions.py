def test_ok_has_stable_u16():
    from signal_scoring.reason_registry import reason_code_to_u16
    v = reason_code_to_u16("OK", strict=True)
    assert isinstance(v, int)
    assert v == 1


def test_soft_quality_has_u16():
    from signal_scoring.reason_registry import reason_code_to_u16
    v = reason_code_to_u16("SOFT_QUALITY", strict=True)
    assert isinstance(v, int)
    assert 0 < v <= 65535


def test_soft_l3_missing_has_u16():
    from signal_scoring.reason_registry import reason_code_to_u16
    v = reason_code_to_u16("SOFT_L3_MISSING", strict=True)
    assert isinstance(v, int)
    assert v == 11
