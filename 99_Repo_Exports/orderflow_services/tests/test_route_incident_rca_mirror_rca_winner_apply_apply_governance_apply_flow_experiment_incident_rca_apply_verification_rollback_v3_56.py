from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_post_apply_verifier_v3_56 import (
    evaluate_post_apply,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_controller_v3_56 import (
    evaluate_rollback,
)


def _policy():
    return {
        "verify_delay_sec": 0,
        "min_provider_samples": 3,
        "min_provider_usefulness": 0.60,
        "min_provider_accepted": 0.60,
        "require_mode_match": 1,
    }


def test_verifier_holds_when_not_applied():
    journal_row = {
        "applied": "0",
        "current_bridge_mode": "AUTO",
        "target_bridge_mode": "VERTEX_ONLY",
        "ts_ms": "1",
    }
    out = evaluate_post_apply(
        journal_row=journal_row,
        current_mode="AUTO",
        latest_vertex_rollup={"n": 5, "avg_usefulness": 0.70, "accepted_rate": 0.70},
        latest_local_rollup={"n": 5, "avg_usefulness": 0.65, "accepted_rate": 0.65},
        policy=_policy(),
    )
    assert out["decision"] == "HOLD"
    assert out["reason_code"] == "NOT_APPLIED"


def test_verifier_rolls_back_on_mode_mismatch():
    journal_row = {
        "applied": "1",
        "current_bridge_mode": "AUTO",
        "target_bridge_mode": "LOCAL_ONLY",
        "ts_ms": "1",
    }
    out = evaluate_post_apply(
        journal_row=journal_row,
        current_mode="VERTEX_ONLY",
        latest_vertex_rollup={"n": 5, "avg_usefulness": 0.70, "accepted_rate": 0.70},
        latest_local_rollup={"n": 5, "avg_usefulness": 0.70, "accepted_rate": 0.70},
        policy=_policy(),
    )
    assert out["decision"] == "ROLLBACK_PREVIOUS_MODE"
    assert out["reason_code"] == "BRIDGE_MODE_MISMATCH_AFTER_APPLY"


def test_verifier_rolls_back_when_vertex_only_underperforms():
    journal_row = {
        "applied": "1",
        "current_bridge_mode": "AUTO",
        "target_bridge_mode": "VERTEX_ONLY",
        "ts_ms": "1",
    }
    out = evaluate_post_apply(
        journal_row=journal_row,
        current_mode="VERTEX_ONLY",
        latest_vertex_rollup={"n": 4, "avg_usefulness": 0.40, "accepted_rate": 0.70},
        latest_local_rollup={"n": 4, "avg_usefulness": 0.75, "accepted_rate": 0.80},
        policy=_policy(),
    )
    assert out["decision"] == "ROLLBACK_PREVIOUS_MODE"
    assert out["reason_code"] == "VERTEX_ONLY_UNDERPERFORMS_AFTER_APPLY"


def test_rollback_controller_accepts_actionable_reason():
    verification_row = {
        "decision": "ROLLBACK_PREVIOUS_MODE",
        "reason_code": "LOCAL_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY",
        "rollback_mode": "AUTO",
        "target_mode": "LOCAL_ONLY",
    }
    rollback_ready = {"previous_mode": "AUTO", "target_mode": "LOCAL_ONLY"}
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "allow_commit": 0,
        "allowed_reasons": {
            "BRIDGE_MODE_MISMATCH_AFTER_APPLY",
            "VERTEX_ONLY_UNDERPERFORMS_AFTER_APPLY",
            "VERTEX_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY",
            "LOCAL_ONLY_UNDERPERFORMS_AFTER_APPLY",
            "LOCAL_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY",
        },
    }
    out = evaluate_rollback(verification_row, rollback_ready, policy)
    assert out["decision"] == "ROLLBACK_TO_PREVIOUS_MODE"
    assert out["rollback_mode"] == "AUTO"
