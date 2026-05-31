from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollup_v3_57 import build_rollup
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_escalation_controller_v3_57 import (
    evaluate_action,
    state_attempts_key,
    state_not_before_key,
)


def test_build_rollup_counts_verified_rollback_retry_and_escalation():
    verification_rows = [
        {"decision": "VERIFIED"},
        {"decision": "VERIFIED"},
        {"decision": "ROLLBACK_TO_PREVIOUS_MODE"},
    ]
    rollback_rows = [
        {
            "decision": "ROLLBACK_TO_PREVIOUS_MODE",
            "applied": 1,
            "source_verification_ts_ms": 1000,
            "ts_ms": 4000,
        },
        {
            "decision": "ROLLBACK_TO_PREVIOUS_MODE",
            "applied": 0,
            "source_verification_ts_ms": 1000,
            "ts_ms": 7000,
        },
    ]
    retry_rows = [{"decision": "RETRY_ROLLBACK_TO_PREVIOUS_MODE"}]
    escalation_rows = [{"decision": "ESCALATE"}]

    out = build_rollup(
        verification_rows=verification_rows,
        rollback_rows=rollback_rows,
        retry_rows=retry_rows,
        escalation_rows=escalation_rows,
    )

    import pytest
    assert out["verification_n"] == 3
    assert out["verified_n"] == 2
    assert out["rollback_planned_n"] == 2
    assert out["rollback_applied_n"] == 1
    assert out["retry_n"] == 1
    assert out["escalation_n"] == 1
    assert out["verify_keep_rate"] == pytest.approx(2 / 3, rel=1e-5)
    assert out["rollback_plan_rate"] == pytest.approx(2 / 3, rel=1e-5)
    assert out["rollback_applied_rate"] == 0.5
    assert out["retry_rate"] == 0.5
    assert out["escalation_rate"] == 0.5


def test_build_rollup_mttr_p95_uses_only_applied_rollbacks():
    rollback_rows = [
        {"decision": "ROLLBACK_TO_PREVIOUS_MODE", "applied": 1, "source_verification_ts_ms": 1000, "ts_ms": 5000},
        {"decision": "ROLLBACK_TO_PREVIOUS_MODE", "applied": 0, "source_verification_ts_ms": 1000, "ts_ms": 9000},
    ]
    out = build_rollup(
        verification_rows=[],
        rollback_rows=rollback_rows,
        retry_rows=[],
        escalation_rows=[],
    )
    assert out["rollback_mttr_p95_sec"] == 4.0


def test_retry_action_for_retryable_reason_when_attempts_below_limit():
    row = {
        "decision": "ROLLBACK_TO_PREVIOUS_MODE",
        "applied": 0,
        "reason_code": "BRIDGE_MODE_MISMATCH_AFTER_APPLY",
    }
    out = evaluate_action(row=row, attempts=0, max_attempts=2)
    assert out["decision"] == "RETRY_ROLLBACK_TO_PREVIOUS_MODE"
    assert out["reason_code"] == "BRIDGE_MODE_MISMATCH_AFTER_APPLY"


def test_retry_action_escalates_when_attempts_exhausted():
    row = {
        "decision": "ROLLBACK_TO_PREVIOUS_MODE",
        "applied": 0,
        "reason_code": "VERTEX_ONLY_UNDERPERFORMS_AFTER_APPLY",
    }
    out = evaluate_action(row=row, attempts=2, max_attempts=2)
    assert out["decision"] == "ESCALATE"
    assert out["reason_code"] == "VERTEX_ONLY_UNDERPERFORMS_AFTER_APPLY"


def test_retry_action_holds_when_already_rolled_back():
    row = {
        "decision": "ROLLBACK_TO_PREVIOUS_MODE",
        "applied": 1,
        "reason_code": "BRIDGE_MODE_MISMATCH_AFTER_APPLY",
    }
    out = evaluate_action(row=row, attempts=0, max_attempts=2)
    assert out["decision"] == "HOLD"
    assert out["reason_code"] == "ALREADY_ROLLED_BACK"


def test_retry_state_keys_are_deterministic():
    row = {
        "rollback_mode": "AUTO",
        "failed_target_mode": "VERTEX_ONLY",
        "reason_code": "BRIDGE_MODE_MISMATCH_AFTER_APPLY",
    }
    assert state_attempts_key(row) == "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_attempts:AUTO:VERTEX_ONLY:BRIDGE_MODE_MISMATCH_AFTER_APPLY"
    assert state_not_before_key(row) == "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_not_before_ms:AUTO:VERTEX_ONLY:BRIDGE_MODE_MISMATCH_AFTER_APPLY"
