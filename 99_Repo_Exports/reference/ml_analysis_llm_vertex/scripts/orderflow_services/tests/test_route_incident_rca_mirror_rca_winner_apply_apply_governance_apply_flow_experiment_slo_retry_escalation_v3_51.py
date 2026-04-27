from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_rollup_v3_51 import (
    build_rollup,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_escalation_controller_v3_51 import (
    evaluate_action,
)


def test_build_rollup_computes_basic_rates():
    verification_rows = [
        {"decision": "VERIFIED", "ts_ms": "9999999999999"},
        {"decision": "ROLLBACK_PREVIOUS_PROFILE", "ts_ms": "9999999999998"},
    ]
    rollback_rows = [
        {"applied": "1", "source_verification_ts_ms": "9999999999998", "ts_ms": "10000000000998"},
    ]
    retry_rows = [{"ts_ms": "9999999999997"}]
    escalation_rows = [{"ts_ms": "9999999999996"}]
    rollup = build_rollup(verification_rows, rollback_rows, retry_rows, escalation_rows)
    assert rollup["verification_n"] == 2
    assert rollup["verified_n"] == 1
    assert rollup["rollback_planned_n"] == 1
    assert rollup["rollback_applied_n"] == 1
    assert rollup["retry_n"] == 1
    assert rollup["escalation_n"] == 1


def test_retry_for_allowed_reason_before_max_attempts():
    verification_row = {
        "decision": "ROLLBACK_PREVIOUS_PROFILE",
        "reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY",
        "target_profile": "local_profile",
        "target_incumbent_arm": "local_candidate",
        "target_weights_json": '{"vertex_primary_weight":25,"vertex_compact_weight":25,"local_candidate_weight":50}',
    }
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "allow_commit": 0,
        "max_attempts": 2,
        "allowed_retry_reasons": {"TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
        "warning_reasons": {"TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
    }
    out = evaluate_action(verification_row, attempts=1, policy=policy)
    assert out["decision"] == "RETRY_REAPPLY_TARGET_PROFILE"


def test_escalate_for_non_retryable_reason():
    verification_row = {
        "decision": "ROLLBACK_PREVIOUS_PROFILE",
        "reason_code": "WEIGHTS_MISMATCH_AFTER_APPLY",
        "target_profile": "local_profile",
        "target_incumbent_arm": "local_candidate",
        "target_weights_json": '{"vertex_primary_weight":25,"vertex_compact_weight":25,"local_candidate_weight":50}',
    }
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "allow_commit": 0,
        "max_attempts": 2,
        "allowed_retry_reasons": {"TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
        "warning_reasons": {"TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
    }
    out = evaluate_action(verification_row, attempts=0, policy=policy)
    assert out["decision"] == "ESCALATE"
    assert out["severity"] == "critical"
