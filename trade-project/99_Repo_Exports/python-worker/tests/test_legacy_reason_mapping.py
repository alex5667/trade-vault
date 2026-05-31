from signal_scoring.reason_codes import ReasonCode, legacy_reason_to_code


def test_legacy_reason_mapping_golden() -> None:
    # Эти строки встречаются в validate()/confirm() как "reason".
    assert legacy_reason_to_code("bo_l2_missing") == ReasonCode.VETO_L2_MISSING.value
    assert legacy_reason_to_code("bo_l2_stale") == ReasonCode.VETO_L2_STALE.value
    assert legacy_reason_to_code("conf_below_min_veto") == ReasonCode.VETO_CONF_BELOW_MIN.value
