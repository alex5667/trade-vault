from signal_scoring.reason_codes import ReasonCode
from signal_scoring.reason_policy import POLICY
from signal_scoring.reason_registry import LEGACY_REASON_ALIASES


def test_legacy_aliases_map_to_known_reason_codes() -> None:
    known = {rc.value for rc in ReasonCode}
    bad = []
    for k, v in LEGACY_REASON_ALIASES.items():
        if v not in known:
            bad.append((k, v))
    assert not bad, f"LEGACY_REASON_ALIASES contains unknown targets: {bad}"


def test_policy_has_no_unknown_keys() -> None:
    known = {rc.value for rc in ReasonCode}
    extra = sorted([k for k in POLICY.keys() if k not in known])
    assert not extra, f"POLICY contains unknown reason codes (typos?): {extra}"


def test_policy_covers_all_veto_codes() -> None:
    missing = []
    for rc in ReasonCode:
        if rc.value == ReasonCode.VETO_UNKNOWN.value:
            continue
        if rc.value.startswith("VETO_") and rc.value not in POLICY:
            missing.append(rc.value)
    assert not missing, f"POLICY missing coverage for: {missing}"


def test_l2_veto_is_breakout_only_and_error_severity() -> None:
    # Жёсткое правило: L2 fail-closed применим ТОЛЬКО к breakout
    for rc in ReasonCode:
        if rc.value.startswith("VETO_L2_"):
            p = POLICY.get(rc.value)
            assert p is not None
            assert p.allowed_kinds == {"breakout"}, f"{rc.value}: allowed_kinds must be breakout-only"
            assert p.mismatch_severity == "error", f"{rc.value}: mismatch_severity must be error"


def test_regime_range_breakout_is_breakout_only_and_error() -> None:
    rc = ReasonCode.VETO_REGIME_RANGE_BREAKOUT.value
    p = POLICY.get(rc)
    assert p is not None
    assert p.allowed_kinds == {"breakout"}
    assert p.mismatch_severity == "error"


def test_legacy_aliases_map_to_known_reason_codes() -> None:
    known = {rc.value for rc in ReasonCode}
    bad = []
    for k, v in LEGACY_REASON_ALIASES.items():
        if v not in known:
            bad.append((k, v))
    assert not bad, f"LEGACY_REASON_ALIASES contains unknown targets: {bad}"
