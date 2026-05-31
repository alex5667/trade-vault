from __future__ import annotations

from common.reason_codes import REASON_U16_BY_CODE, ReasonCode, code_to_u16, ensure_reason_fields


def test_reason_u16_is_stable_and_non_negative():
    # basic ABI sanity
    for k, v in REASON_U16_BY_CODE.items():
        assert isinstance(k, str)
        assert isinstance(v, int)
        assert 0 <= v <= 65535


def test_code_to_u16_known_codes():
    assert code_to_u16(ReasonCode.OK.value) == 0
    assert code_to_u16("veto_l2_stale") == REASON_U16_BY_CODE["VETO_L2_STALE"]


def test_ensure_reason_fields_non_strict_fallback():
    r, rc, u16 = ensure_reason_fields(reason="some legacy", reason_code="", strict=False)
    assert rc in {"OK", "VETO_INTERNAL_ERROR"}
    assert isinstance(u16, int)
