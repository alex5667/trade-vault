"""Plan 1 — reason_code enum invariants.

Downstream metrics and SQL views key on these strings; if anyone adds a
free-text reason or renames an enum member, those queries break silently.
The contract here is a regression barrier.
"""
from __future__ import annotations

from services.confidence_meta_gate.reason_codes import MetaGateReason, is_valid_reason


def test_all_reason_values_are_lower_snake_case() -> None:
    for r in MetaGateReason:
        assert r.value == r.value.lower(), r.name
        assert " " not in r.value, r.name
        # Members may contain underscores but must not contain dashes.
        assert "-" not in r.value, r.name


def test_no_duplicate_values() -> None:
    values = [r.value for r in MetaGateReason]
    assert len(values) == len(set(values))


def test_is_valid_reason_accepts_known() -> None:
    assert is_valid_reason(MetaGateReason.META_ALLOW.value)
    assert is_valid_reason(MetaGateReason.P_WIN_BELOW_FLOOR.value)


def test_is_valid_reason_rejects_unknown() -> None:
    assert not is_valid_reason("free_text_reason")
    assert not is_valid_reason("META_ALLOW")  # case-sensitive guard
    assert not is_valid_reason("")


def test_critical_reason_codes_present() -> None:
    """Locked contract — these codes are referenced from alerts and the
    audit table query plan. If any are removed/renamed, update those too."""
    required = {
        "mode_shadow", "mode_canary_selected", "mode_canary_not_selected",
        "mode_enforce", "mode_kill_switch", "legacy_fallback",
        "model_not_loaded", "model_stale", "calibration_ece_high",
        "schema_mismatch",
        "p_win_below_floor", "expected_r_below_floor",
        "expected_edge_below_floor",
        "meta_allow", "meta_allow_tightened", "meta_deny_soft",
        "probability_ok", "edge_ok",
        "dq_degraded", "exec_cost_high",
    }
    have = {r.value for r in MetaGateReason}
    missing = required - have
    assert not missing, f"missing required reason codes: {missing}"
