from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_result_consumer_v3_48 import (
    build_result_payload,
    evaluate_request,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_selector_v3_48 import (
    build_scorecards,
    select_winner,
)


def _bundle():
    return {
        "bundle_id": "winner-apply-apply-governance-apply-flow-bundle:1",
        "trigger_type": "verification",
        "trigger_severity": "critical",
        "summary": {
            "apply_decisions_n": 2,
            "verification_events_n": 3,
            "rollback_events_n": 1,
            "retry_events_n": 1,
            "escalation_events_n": 1,
            "verification_reason_codes": [
                "PRIMARY_MATCH_RATE_TOO_LOW",
                "POLICY_MISMATCH_AFTER_APPLY",
            ],
            "rollback_reason_codes": ["ROLLBACK_MTTR_P95_HIGH"],
            "retry_reason_codes": ["MAX_ATTEMPTS_REACHED"],
            "escalation_severities": ["critical"],
        },
        "evidence": {
            "trigger": {
                "decision": "ROLLBACK_PREVIOUS_POLICY",
                "reason_code": "PRIMARY_MATCH_RATE_TOO_LOW",
            }
        },
    }


def _policy():
    return {
        "enabled": 1,
        "kill_switch": 0,
        "handler_mode": "DETERMINISTIC",
        "allow_severities": {"warning", "critical"},
        "max_bundle_bytes": 196608,
    }


def test_consumer_accepts_valid_experiment_bundle():
    out = evaluate_request(_bundle(), _policy())
    assert out["decision"] == "BUILD_RESULT"
    assert out["reason_code"] == "OK"


def test_result_payload_compact_candidate_is_shorter():
    base = build_result_payload(_bundle(), "vertex_primary", "VERTEX")
    compact = build_result_payload(_bundle(), "vertex_compact_candidate", "VERTEX")
    assert len(compact["dominant_findings"]) <= len(base["dominant_findings"])
    assert compact["quality_flags"]["experiment_arm"] == "vertex_compact_candidate"


def test_winner_selector_promotes_local_candidate_when_better():
    exposures = [
        {"request_id": "r1", "arm": "vertex_primary", "ts_ms": "9999999999999"},
        {"request_id": "r2", "arm": "vertex_primary", "ts_ms": "9999999999999"},
        {"request_id": "r3", "arm": "vertex_primary", "ts_ms": "9999999999999"},
        {"request_id": "r4", "arm": "vertex_primary", "ts_ms": "9999999999999"},
        {"request_id": "r5", "arm": "vertex_primary", "ts_ms": "9999999999999"},
        {"request_id": "l1", "arm": "local_candidate", "ts_ms": "9999999999999"},
        {"request_id": "l2", "arm": "local_candidate", "ts_ms": "9999999999999"},
        {"request_id": "l3", "arm": "local_candidate", "ts_ms": "9999999999999"},
        {"request_id": "l4", "arm": "local_candidate", "ts_ms": "9999999999999"},
        {"request_id": "l5", "arm": "local_candidate", "ts_ms": "9999999999999"},
    ]
    results = [
        {"request_id": "r1", "ts_ms": "9999999999999"},
        {"request_id": "r2", "ts_ms": "9999999999999"},
        {"request_id": "r3", "ts_ms": "9999999999999"},
        {"request_id": "r4", "ts_ms": "9999999999999"},
        {"request_id": "r5", "ts_ms": "9999999999999"},
        {"request_id": "l1", "ts_ms": "9999999999999"},
        {"request_id": "l2", "ts_ms": "9999999999999"},
        {"request_id": "l3", "ts_ms": "9999999999999"},
        {"request_id": "l4", "ts_ms": "9999999999999"},
        {"request_id": "l5", "ts_ms": "9999999999999"},
    ]
    feedback = [
        {"request_id": "r1", "quality_score": "0.55", "usefulness_score": "0.56", "accepted": "1", "ts_ms": "9999999999999"},
        {"request_id": "r2", "quality_score": "0.56", "usefulness_score": "0.57", "accepted": "0", "ts_ms": "9999999999999"},
        {"request_id": "r3", "quality_score": "0.57", "usefulness_score": "0.58", "accepted": "1", "ts_ms": "9999999999999"},
        {"request_id": "l1", "quality_score": "0.80", "usefulness_score": "0.82", "accepted": "1", "ts_ms": "9999999999999"},
        {"request_id": "l2", "quality_score": "0.81", "usefulness_score": "0.83", "accepted": "1", "ts_ms": "9999999999999"},
        {"request_id": "l3", "quality_score": "0.82", "usefulness_score": "0.84", "accepted": "1", "ts_ms": "9999999999999"},
    ]
    policy = {
        "min_exposures": 5,
        "min_feedback": 3,
        "min_result_coverage": 0.50,
        "min_feedback_coverage": 0.30,
        "min_quality": 0.55,
        "min_usefulness": 0.60,
        "min_accepted_rate": 0.60,
        "min_score_margin": 0.05,
        "incumbent_arm": "vertex_primary",
    }
    scorecards = build_scorecards(exposures, results, feedback, policy)
    out = select_winner(scorecards, policy)
    assert out["decision"] == "PROMOTE_LOCAL_CANDIDATE"
    assert out["winner_arm"] == "local_candidate"
