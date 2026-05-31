import pytest

from signal_scoring.reason_codes import ReasonCode, is_valid_reason_code, legacy_reason_to_code


@pytest.mark.parametrize(
    "reason,expected",
    [
        ("ok", ReasonCode.OK),
        ("conf_below_min_veto", ReasonCode.VETO_CONF_BELOW_MIN),
        ("spread_wide", ReasonCode.VETO_SPREAD_WIDE),
        ("bo_l2_missing", ReasonCode.VETO_L2_MISSING),
        ("bo_l2_stale", ReasonCode.VETO_L2_STALE),
        ("bo_l2_bad", ReasonCode.VETO_L2_BAD),
        ("bo_l2_veto", ReasonCode.VETO_L2_BAD),
        ("range_breakout_veto", ReasonCode.VETO_REGIME_RANGE_BREAKOUT),
        ("wall_near_veto", ReasonCode.VETO_WALL_NEAR),
        ("l3_spoof_risk", ReasonCode.VETO_L3_SPOOF_RISK),
    ],
)
def test_legacy_reason_to_code(reason, expected):
    assert legacy_reason_to_code(reason) == expected


def test_unknown_reason_returns_unknown():
    assert legacy_reason_to_code("some_new_reason_we_forgot") == ReasonCode.VETO_UNKNOWN


def test_none_reason_returns_unknown():
    assert legacy_reason_to_code(None) == ReasonCode.VETO_UNKNOWN


def test_empty_reason_returns_unknown():
    assert legacy_reason_to_code("") == ReasonCode.VETO_UNKNOWN


@pytest.mark.parametrize(
    "code,expected",
    [
        ("OK", True),
        ("VETO_UNKNOWN", True),
        ("VETO_CONF_BELOW_MIN", True),
        ("INVALID_CODE", False),
        ("", False),
        (None, False),
    ],
)
def test_is_valid_reason_code(code, expected):
    assert is_valid_reason_code(code) == expected
