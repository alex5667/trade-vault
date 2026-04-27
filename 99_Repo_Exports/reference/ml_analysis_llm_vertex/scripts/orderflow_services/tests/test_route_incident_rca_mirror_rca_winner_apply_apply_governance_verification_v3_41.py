from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_verification_loop_v3_41 import (
    compute_exposure_stats,
    evaluate_verification,
)


def _verify_policy():
    return {
        "enabled": 1,
        "kill_switch": 0,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "min_exposures": 5,
        "min_primary_match_rate": 0.80,
        "max_unexpected_primary_rate": 0.20,
        "max_shadow_rate_single_arm": 0.05,
        "require_policy_match": 1,
        "rollback_cooldown_sec": 21600,
    }


def test_keep_applied_when_policy_and_exposures_match():
    apply_event = {
        "decision": "APPLY_PRIMARY_ARM_SHADOW",
        "mode_before": "SHADOW",
        "primary_arm_before": "deterministic",
        "mode_after": "SHADOW",
        "primary_arm_after": "vertex_candidate",
        "ts_ms": "1000",
    }
    current_policy = {
        "mode": "SHADOW",
        "primary_arm": "vertex_candidate",
        "shadow_arms_json": '["deterministic","local_fallback_candidate"]',
        "last_mode_switch_ts_ms": 0,
    }
    exposures = [
        {"arm": "vertex_candidate", "is_primary": "1", "ts_ms": "2000"},
        {"arm": "vertex_candidate", "is_primary": "1", "ts_ms": "2001"},
        {"arm": "vertex_candidate", "is_primary": "1", "ts_ms": "2002"},
        {"arm": "vertex_candidate", "is_primary": "1", "ts_ms": "2003"},
        {"arm": "vertex_candidate", "is_primary": "1", "ts_ms": "2004"},
        {"arm": "deterministic", "is_primary": "0", "ts_ms": "2005"},
    ]
    stats = compute_exposure_stats(exposures, "vertex_candidate")
    out = evaluate_verification(
        apply_event=apply_event,
        current_policy=current_policy,
        exposure_stats=stats,
        verify_policy=_verify_policy(),
        now_ts_ms=100000,
    )
    assert out["decision"] == "KEEP_APPLIED"
    assert out["reason_code"] == "POST_APPLY_VERIFIED"


def test_rollback_when_policy_mismatch():
    apply_event = {
        "decision": "APPLY_PRIMARY_ARM_SHADOW",
        "mode_before": "SHADOW",
        "primary_arm_before": "deterministic",
        "mode_after": "SHADOW",
        "primary_arm_after": "vertex_candidate",
        "ts_ms": "1000",
    }
    current_policy = {
        "mode": "SHADOW",
        "primary_arm": "deterministic",
        "shadow_arms_json": '["vertex_candidate","local_fallback_candidate"]',
        "last_mode_switch_ts_ms": 0,
    }
    stats = {
        "total": 5,
        "primary_total": 5,
        "target_primary_n": 5,
        "unexpected_primary_n": 0,
        "shadow_n": 0,
        "primary_match_rate": 1.0,
        "unexpected_primary_rate": 0.0,
        "shadow_rate": 0.0,
    }
    out = evaluate_verification(
        apply_event=apply_event,
        current_policy=current_policy,
        exposure_stats=stats,
        verify_policy=_verify_policy(),
        now_ts_ms=100000,
    )
    assert out["decision"] == "ROLLBACK_PREVIOUS_POLICY"
    assert out["reason_code"] == "POLICY_MISMATCH_AFTER_APPLY"


def test_rollback_when_primary_match_rate_too_low():
    apply_event = {
        "decision": "APPLY_SINGLE_ARM",
        "mode_before": "SHADOW",
        "primary_arm_before": "deterministic",
        "mode_after": "SINGLE_ARM",
        "primary_arm_after": "local_fallback_candidate",
        "ts_ms": "1000",
    }
    current_policy = {
        "mode": "SINGLE_ARM",
        "primary_arm": "local_fallback_candidate",
        "shadow_arms_json": "[]",
        "last_mode_switch_ts_ms": 0,
    }
    stats = {
        "total": 6,
        "primary_total": 6,
        "target_primary_n": 3,
        "unexpected_primary_n": 3,
        "shadow_n": 0,
        "primary_match_rate": 0.5,
        "unexpected_primary_rate": 0.5,
        "shadow_rate": 0.0,
    }
    out = evaluate_verification(
        apply_event=apply_event,
        current_policy=current_policy,
        exposure_stats=stats,
        verify_policy=_verify_policy(),
        now_ts_ms=100000,
    )
    assert out["decision"] == "ROLLBACK_PREVIOUS_POLICY"
    assert out["reason_code"] == "PRIMARY_MATCH_RATE_TOO_LOW"
