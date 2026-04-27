from core.unified_signal_formatter import UnifiedSignalFormatter
from signal_scoring.reason_codes import ReasonCode


def test_formatter_fallback_from_legacy_reason() -> None:
    fmt = UnifiedSignalFormatter()
    out = fmt.format({"kind": "breakout", "reason": "bo_l2_missing"})
    assert out.get("reason_code") == ReasonCode.VETO_L2_MISSING.value
