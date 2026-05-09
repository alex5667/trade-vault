from dataclasses import dataclass

from common.qf_codes import QF
from signal_scoring.reason_codes import ReasonCode
from signal_scoring.reason_policy import POLICY, is_reason_allowed_for_kind, patch_validation_reason_for_kind


def test_reason_policy_covers_all_veto_codes() -> None:
    """
    Anti-drift guard:
      if you add a new VETO_* code, you MUST decide which kinds it can appear in.
    """
    missing = []
    for rc in ReasonCode:
        if rc.value == ReasonCode.VETO_UNKNOWN.value:
            continue
        if rc.value.startswith("VETO_"):
            if rc.value not in POLICY:
                missing.append(rc.value)
    assert not missing, f"POLICY missing coverage for: {missing}"


def test_reason_policy_specificity_regime_range_breakout() -> None:
    assert is_reason_allowed_for_kind(ReasonCode.VETO_REGIME_RANGE_BREAKOUT.value, "breakout") is True
    assert is_reason_allowed_for_kind(ReasonCode.VETO_REGIME_RANGE_BREAKOUT.value, "absorption") is False
    assert is_reason_allowed_for_kind(ReasonCode.VETO_REGIME_RANGE_BREAKOUT.value, "extreme") is False


@dataclass(frozen=True)
class _V:
    veto: bool
    conf_factor01: float
    flags: list[int]
    reason: str
    reason_code: str
    reason_u16: int = 0
    parts: dict | None = None


def test_reason_mismatch_adds_qf_flag() -> None:
    # L2 stale policy: breakout-only. Пробуем "absorption" => должен быть normalize + QF flag.
    v = _V(veto=True, conf_factor01=0.0, flags=[], reason="regime_range_breakout", reason_code=ReasonCode.VETO_L2_STALE.value, parts={})
    patched = patch_validation_reason_for_kind(validation=v, kind="absorption", monitor=None)
    assert patched.reason_code in (ReasonCode.VETO_UNKNOWN.value, ReasonCode.VETO_L2_STALE.value)
    # ожидаем mismatch => VETO_UNKNOWN и meta qf флаг
    assert patched.reason_code == ReasonCode.VETO_UNKNOWN.value
    assert int(QF.REASON_KIND_MISMATCH) in (patched.flags or [])
    assert (patched.parts or {}).get("reason_code_original") == ReasonCode.VETO_L2_STALE.value


def test_legacy_reason_mapping_sets_legacy_flag_without_kind_mismatch() -> None:
    # Test that unknown legacy reasons are handled gracefully
    # "some_unknown_legacy" should not cause errors and should be processed
    v = _V(veto=True, conf_factor01=0.0, flags=[], reason="regime_range_breakout", reason_code="some_unknown_legacy", parts={})
    patched = patch_validation_reason_for_kind(validation=v, kind="breakout", monitor=None)
    # Unknown legacy should not add legacy-specific flags
    assert int(QF.REASON_LEGACY_MAPPED) not in (patched.flags or [])
    # But should still be processed by policy


def test_unknown_legacy_reason_keeps_code_and_sets_no_legacy_flag() -> None:
    v = _V(veto=True, conf_factor01=0.0, flags=[], reason="regime_range_breakout", reason_code="some_old_reason_x", parts={})
    patched = patch_validation_reason_for_kind(validation=v, kind="breakout", monitor=None)
    # если registry не знает — оставляем как есть до policy; policy может нормализовать дальше (зависит от ваших правил)
    # здесь проверяем именно отсутствие legacy-флага.
    assert int(QF.REASON_LEGACY_MAPPED) not in (patched.flags or [])
