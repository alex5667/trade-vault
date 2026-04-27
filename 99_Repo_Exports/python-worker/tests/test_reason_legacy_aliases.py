from signal_scoring.reason_codes import ReasonCode
from signal_scoring.reason_registry import iter_golden_aliases, legacy_reason_to_code


def test_every_veto_code_has_at_least_one_alias() -> None:
    """
    Guarantees "future-proofing":
      each ReasonCode.VETO_* must be addressable by at least one alias.
    We enforce this via auto-alias (normalized code string).
    """
    alias_pairs = iter_golden_aliases()
    by_code = {}
    for a, code in alias_pairs:
        by_code.setdefault(code, set()).add(a)

    veto_codes = [rc for rc in ReasonCode if rc.value.startswith("VETO_") and rc.value != ReasonCode.VETO_UNKNOWN.value]
    for rc in veto_codes:
        assert rc.value in by_code, f"no aliases for veto code: {rc.value}"
        assert len(by_code[rc.value]) >= 1, f"empty alias set for veto code: {rc.value}"


def test_known_legacy_aliases_resolve() -> None:
    # A few high-signal legacy strings we care about
    assert legacy_reason_to_code("bo_l2_missing") == ReasonCode.VETO_L2_MISSING.value
    assert legacy_reason_to_code("bo_l2_stale") == ReasonCode.VETO_L2_STALE.value
    assert legacy_reason_to_code("conf_below_min_veto") == ReasonCode.VETO_CONF_BELOW_MIN.value
    assert legacy_reason_to_code("spread_wide") == ReasonCode.VETO_SPREAD_WIDE.value


def test_alias_normalization_is_robust() -> None:
    # Case / separators / spaces should not matter
    assert legacy_reason_to_code("BO-L2-STALE") == ReasonCode.VETO_L2_STALE.value
    assert legacy_reason_to_code("  bo l2 stale  ") == ReasonCode.VETO_L2_STALE.value
    assert legacy_reason_to_code("Spread Wide") == ReasonCode.VETO_SPREAD_WIDE.value
