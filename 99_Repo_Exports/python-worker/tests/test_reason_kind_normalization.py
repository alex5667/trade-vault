from handlers.confirmations.engine import Validation
from signal_scoring.reason_codes import ReasonCode
from signal_scoring.reason_policy import patch_validation_reason_for_kind


def test_reason_kind_mismatch_is_normalized_to_unknown() -> None:
    v = Validation(
        veto=True,
        conf_factor01=0.0,
        flags=[],
        reason="regime_range_breakout",
        parts={},
        reason_code=ReasonCode.VETO_REGIME_RANGE_BREAKOUT.value,
        reason_u16=0,
    )
    v2 = patch_validation_reason_for_kind(validation=v, kind="absorption")
    assert v2.veto is True
    assert v2.reason_code == ReasonCode.VETO_UNKNOWN.value
    assert "reason_kind_mismatch" in (v2.parts or {})
    assert (v2.parts["reason_kind_mismatch"] or {}).get("kind") == "absorption"
