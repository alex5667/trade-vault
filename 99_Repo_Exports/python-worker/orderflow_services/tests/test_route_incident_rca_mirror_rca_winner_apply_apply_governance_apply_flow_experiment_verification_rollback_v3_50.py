from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_post_apply_verifier_v3_50 import (
    evaluate_post_apply,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_controller_v3_50 import (
    evaluate_rollback,
)


def _policy():
    return {
        "verify_delay_sec": 0,
        "min_post_apply_exposures": 3,
        "min_target_share_floor": 0.25,
        "share_tolerance": 0.20,
        "require_incumbent_match": 1,
        "max_weight_delta_sum": 0,
    }


def test_verifier_holds_when_not_applied():
    journal_row = {
        "applied": "0",
        "winner_arm": "local_candidate",
        "target_profile": "local_profile",
        "current_profile": "vertex_primary_profile",
        "target_weights_json": '{"vertex_primary_weight":25,"vertex_compact_weight":25,"local_candidate_weight":50}',
        "current_weights_json": '{"vertex_primary_weight":50,"vertex_compact_weight":30,"local_candidate_weight":20}',
        "ts_ms": "1",
    }
    out = evaluate_post_apply(
        journal_row=journal_row,
        current_weights={"vertex_primary_weight": 50, "vertex_compact_weight": 30, "local_candidate_weight": 20},
        current_incumbent_arm="vertex_primary",
        exposure_rows=[],
        policy=_policy(),
    )
    assert out["decision"] == "HOLD"
    assert out["reason_code"] == "NOT_APPLIED"


def test_verifier_rolls_back_on_weights_mismatch():
    journal_row = {
        "applied": "1",
        "winner_arm": "local_candidate",
        "target_profile": "local_profile",
        "current_profile": "vertex_primary_profile",
        "target_weights_json": '{"vertex_primary_weight":25,"vertex_compact_weight":25,"local_candidate_weight":50}',
        "current_weights_json": '{"vertex_primary_weight":50,"vertex_compact_weight":30,"local_candidate_weight":20}',
        "ts_ms": "1",
    }
    out = evaluate_post_apply(
        journal_row=journal_row,
        current_weights={"vertex_primary_weight": 30, "vertex_compact_weight": 40, "local_candidate_weight": 30},
        current_incumbent_arm="local_candidate",
        exposure_rows=[],
        policy=_policy(),
    )
    assert out["decision"] == "ROLLBACK_PREVIOUS_PROFILE"
    assert out["reason_code"] == "WEIGHTS_MISMATCH_AFTER_APPLY"


def test_verifier_rolls_back_when_target_share_too_low():
    journal_row = {
        "applied": "1",
        "winner_arm": "local_candidate",
        "target_profile": "local_profile",
        "current_profile": "vertex_primary_profile",
        "target_weights_json": '{"vertex_primary_weight":25,"vertex_compact_weight":25,"local_candidate_weight":50}',
        "current_weights_json": '{"vertex_primary_weight":50,"vertex_compact_weight":30,"local_candidate_weight":20}',
        "ts_ms": "1",
    }
    exposure_rows = [
        {"arm": "vertex_primary", "ts_ms": "2"},
        {"arm": "vertex_primary", "ts_ms": "3"},
        {"arm": "vertex_compact_candidate", "ts_ms": "4"},
    ]
    out = evaluate_post_apply(
        journal_row=journal_row,
        current_weights={"vertex_primary_weight": 25, "vertex_compact_weight": 25, "local_candidate_weight": 50},
        current_incumbent_arm="local_candidate",
        exposure_rows=exposure_rows,
        policy=_policy(),
    )
    assert out["decision"] == "ROLLBACK_PREVIOUS_PROFILE"
    assert out["reason_code"] == "TARGET_SHARE_TOO_LOW_AFTER_APPLY"


def test_rollback_evaluates_actionable_reason():
    verification_row = {
        "decision": "ROLLBACK_PREVIOUS_PROFILE",
        "reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY",
        "rollback_profile": "vertex_primary_profile",
        "rollback_incumbent_arm": "vertex_primary",
        "rollback_weights_json": '{"vertex_primary_weight":50,"vertex_compact_weight":30,"local_candidate_weight":20}',
    }
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "allow_commit": 0,
        "allowed_reasons": {
            "WEIGHTS_MISMATCH_AFTER_APPLY",
            "INCUMBENT_MISMATCH_AFTER_APPLY",
            "TARGET_SHARE_TOO_LOW_AFTER_APPLY",
        },
    }
    out = evaluate_rollback(verification_row, policy)
    assert out["decision"] == "ROLLBACK_TO_PREVIOUS_PROFILE"
