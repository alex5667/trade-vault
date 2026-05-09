from signal_scoring.reason_registry import reason_code_to_u16, reason_codes_to_u16s


def test_soft_codes_have_stable_u16():
    u1 = reason_code_to_u16("SOFT_L3_MISSING")
    u2 = reason_code_to_u16("SOFT_HTF_MISSING")
    assert u1 >= 1000 and u1 <= 65535
    assert u2 >= 1000 and u2 <= 65535
    xs = reason_codes_to_u16s(["SOFT_L3_MISSING", "SOFT_HTF_MISSING"])
    assert xs == [u1, u2]
