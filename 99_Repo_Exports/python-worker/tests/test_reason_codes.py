import pytest

from signal_scoring.reason_codes import ReasonCode, legacy_reason_to_code
from handlers.confirmations.engine import Validation


@pytest.mark.parametrize(
    "legacy,expected",
    [
        ("conf_below_min_veto", ReasonCode.VETO_CONF_BELOW_MIN),
        ("spread_wide", ReasonCode.VETO_SPREAD_WIDE),
        ("bo_l2_missing", ReasonCode.VETO_L2_MISSING),
        ("bo_l2_stale", ReasonCode.VETO_L2_STALE),
        ("bo_l2_bad", ReasonCode.VETO_L2_BAD),
        ("range_breakout_veto", ReasonCode.VETO_REGIME_RANGE_BREAKOUT),
        ("wall_near_veto", ReasonCode.VETO_WALL_NEAR),
        ("l3_spoof_risk", ReasonCode.VETO_L3_SPOOF_RISK),
    ],
)
def test_legacy_reason_to_code(legacy: str, expected: ReasonCode) -> None:
    assert legacy_reason_to_code(legacy) == expected


def test_validation_autofills_reason_code_from_legacy_reason() -> None:
    v = Validation(
        veto=True,
        conf_factor01=0.0,
        flags=[],
        reason="bo_l2_stale",
        parts={},
        reason_code="",  # simulate legacy caller forgetting
    )
    assert v.reason_code == ReasonCode.VETO_L2_STALE.value


def test_validation_defaults_to_ok_for_non_veto_with_invalid_code() -> None:
    v = Validation(veto=False, conf_factor01=0.8, flags=[], reason="some_reason", parts={}, reason_code="garbage")
    assert v.reason_code == ReasonCode.OK.value
