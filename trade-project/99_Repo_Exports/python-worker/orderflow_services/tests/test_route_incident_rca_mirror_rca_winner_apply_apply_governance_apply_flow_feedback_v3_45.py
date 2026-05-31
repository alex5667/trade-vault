from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_feedback_governor_v3_45 import (
    evaluate_governance,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_consumer_v3_45 import (
    build_result_payload,
    evaluate_request,
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


def test_consumer_accepts_valid_apply_flow_bundle():
    out = evaluate_request(_bundle(), _policy())
    assert out["decision"] == "BUILD_RESULT"
    assert out["reason_code"] == "OK"


def test_result_payload_contains_actions():
    payload = build_result_payload(_bundle(), "VERTEX")
    assert payload["confidence"] > 0.6
    assert len(payload["next_actions"]) >= 1


def test_governance_prefers_local_when_quality_low():
    rollup = {
        "n": 12,
        "avg_quality": 0.40,
        "avg_usefulness": 0.50,
        "accepted_rate": 0.50,
        "low_quality_rate": 0.50,
    }
    policy = {
        "min_samples": 10,
        "min_avg_quality": 0.55,
        "min_avg_usefulness": 0.60,
        "min_accepted_rate": 0.60,
        "max_low_quality_rate": 0.35,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
    }
    out = evaluate_governance(rollup, policy)
    assert out["decision"] == "PREFER_LOCAL_ONLY"
    assert out["target_bridge_mode"] == "LOCAL_ONLY"
