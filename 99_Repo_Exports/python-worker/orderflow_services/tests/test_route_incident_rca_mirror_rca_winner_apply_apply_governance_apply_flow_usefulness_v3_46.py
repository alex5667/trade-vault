from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_governor_v3_46 import (
    evaluate_usefulness,
    join_feedback_with_results,
)


def test_join_feedback_with_results_by_request_id():
    feedback = [
        {
            "request_id": "r1",
            "bundle_id": "b1",
            "quality_score": "0.8",
            "usefulness_score": "0.9",
            "accepted": "1",
            "reason_code": "helpful",
            "ts_ms": "9999999999999",
        }
    ]
    results = [
        {
            "request_id": "r1",
            "provider_mode": "VERTEX",
            "ts_ms": "9999999999999",
        }
    ]
    joined = join_feedback_with_results(feedback, results)
    assert len(joined) == 1
    assert joined[0]["provider_mode"] == "VERTEX"


def test_suppress_to_local_only_when_vertex_worse_than_local():
    vertex = {"provider_mode": "VERTEX", "n": 12, "avg_quality": 0.60, "avg_usefulness": 0.50, "accepted_rate": 0.50, "low_usefulness_rate": 0.50}
    local = {"provider_mode": "LOCAL", "n": 8, "avg_quality": 0.80, "avg_usefulness": 0.80, "accepted_rate": 0.80, "low_usefulness_rate": 0.0}
    policy = {
        "min_vertex_samples_to_suppress": 10,
        "min_local_samples_to_suppress": 5,
        "min_local_samples_to_promote": 15,
        "vertex_suppress_min_usefulness": 0.58,
        "vertex_suppress_min_accepted": 0.55,
        "vertex_suppress_max_low_usefulness_rate": 0.45,
        "local_good_min_usefulness": 0.65,
        "local_good_min_accepted": 0.65,
        "local_promote_min_usefulness": 0.78,
        "local_promote_min_accepted": 0.75,
        "local_promote_min_quality": 0.70,
        "min_delta_usefulness": 0.05,
        "min_delta_accepted": 0.05,
        "cooldown_sec": 21600,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
    }
    out = evaluate_usefulness(vertex, local, "AUTO", policy, cooldown_active=False)
    assert out["decision"] == "SUPPRESS_TO_LOCAL_ONLY"
    assert out["target_bridge_mode"] == "LOCAL_ONLY"


def test_promote_to_auto_when_local_is_stable():
    vertex = {"provider_mode": "VERTEX", "n": 0, "avg_quality": 0.0, "avg_usefulness": 0.0, "accepted_rate": 0.0, "low_usefulness_rate": 0.0}
    local = {"provider_mode": "LOCAL", "n": 20, "avg_quality": 0.85, "avg_usefulness": 0.84, "accepted_rate": 0.82, "low_usefulness_rate": 0.0}
    policy = {
        "min_vertex_samples_to_suppress": 10,
        "min_local_samples_to_suppress": 5,
        "min_local_samples_to_promote": 15,
        "vertex_suppress_min_usefulness": 0.58,
        "vertex_suppress_min_accepted": 0.55,
        "vertex_suppress_max_low_usefulness_rate": 0.45,
        "local_good_min_usefulness": 0.65,
        "local_good_min_accepted": 0.65,
        "local_promote_min_usefulness": 0.78,
        "local_promote_min_accepted": 0.75,
        "local_promote_min_quality": 0.70,
        "min_delta_usefulness": 0.05,
        "min_delta_accepted": 0.05,
        "cooldown_sec": 21600,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
    }
    out = evaluate_usefulness(vertex, local, "LOCAL_ONLY", policy, cooldown_active=False)
    assert out["decision"] == "PROMOTE_TO_AUTO"
    assert out["target_bridge_mode"] == "AUTO"
