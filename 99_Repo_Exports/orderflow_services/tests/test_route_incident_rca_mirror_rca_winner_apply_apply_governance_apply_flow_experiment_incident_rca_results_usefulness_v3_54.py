from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_result_consumer_v3_54 import (
    build_result_payload,
    evaluate_request,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_governor_v3_54 import (
    evaluate_usefulness,
)


def _bundle():
    return {
        "bundle_id": "apply-flow-experiment-bundle:1",
        "trigger_type": "rollback",
        "trigger_reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY",
        "trigger_severity": "critical",
        "summary": {
            "verification_events_n": 3,
            "rollback_events_n": 1,
            "retry_events_n": 1,
            "escalation_events_n": 1,
            "verify_keep_rate": 0.40,
            "rollback_mttr_p95_sec": 1200,
            "escalation_rate": 0.30,
        },
        "evidence": {
            "latest_verification": {"reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
            "latest_rollback": {"reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
            "latest_retry": {"reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
            "latest_escalation": {"reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY"},
            "latest_slo_rollup": {"verification_n": 4},
        },
    }


def _policy():
    return {
        "enabled": 1,
        "kill_switch": 0,
        "handler_mode": "DETERMINISTIC",
        "allow_severities": {"warning", "critical"},
        "max_bundle_bytes": 262144,
    }


def test_incident_result_consumer_accepts_valid_bundle():
    out = evaluate_request(_bundle(), _policy())
    assert out["decision"] == "BUILD_RESULT"
    assert out["reason_code"] == "OK"


def test_incident_result_payload_contains_dominant_findings():
    payload = build_result_payload(_bundle(), "VERTEX")
    assert payload["quality_flags"]["provider_mode"] == "VERTEX"
    assert len(payload["dominant_findings"]) >= 1


def test_usefulness_prefers_local_when_local_is_better():
    vertex = {"provider_mode": "VERTEX", "n": 6, "avg_quality": 0.55, "avg_usefulness": 0.56, "accepted_rate": 0.55}
    local = {"provider_mode": "LOCAL", "n": 6, "avg_quality": 0.75, "avg_usefulness": 0.76, "accepted_rate": 0.75}
    policy = {
        "min_vertex_samples": 5,
        "min_local_samples": 5,
        "min_usefulness": 0.60,
        "min_accepted_rate": 0.60,
        "min_quality": 0.55,
        "min_delta_usefulness": 0.05,
        "min_delta_accepted": 0.05,
        "cooldown_sec": 21600,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
    }
    out = evaluate_usefulness(vertex, local, "AUTO", policy, cooldown_active=False)
    assert out["decision"] == "PREFER_LOCAL_ONLY"
    assert out["target_bridge_mode"] == "LOCAL_ONLY"
