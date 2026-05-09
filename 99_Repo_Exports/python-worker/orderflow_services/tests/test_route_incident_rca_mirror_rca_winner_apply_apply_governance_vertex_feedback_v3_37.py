from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_feedback_governor_v3_37 import (
    evaluate_governance,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_consumer_v3_37 import (
    build_result_payload,
    evaluate_request,
)


def _bundle():
    return {
        "bundle_id": "winner-apply-apply-governance-bundle:1",
        "trigger_type": "rollback",
        "trigger_severity": "critical",
        "summary": {
            "verification_reason_codes": ["PRIMARY_MATCH_RATE_TOO_LOW"],
            "retry_reason_codes": ["MAX_ATTEMPTS_REACHED"],
            "rollback_reason_codes": ["PRIMARY_MATCH_RATE_TOO_LOW"],
            "escalation_severities": ["critical"],
        },
        "evidence": {
            "slo_recent": [
                {"reason_codes_json": '["ROLLBACK_MTTR_P95_HIGH","VERIFY_KEEP_RATE_LOW","APPLY_RATE_LOW"]'}
            ]
        },
    }


def _policy():
    return {
        "enabled": 1,
        "kill_switch": 0,
        "handler_mode": "DETERMINISTIC",
        "allow_severities": {"warning", "critical"},
        "max_bundle_bytes": 131072,
    }


def test_consumer_accepts_valid_bundle():
    out = evaluate_request(_bundle(), _policy())
    assert out["decision"] == "BUILD_RESULT"
    assert out["reason_code"] == "OK"


def test_result_payload_contains_actions():
    payload = build_result_payload(_bundle())
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
