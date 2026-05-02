from orderflow_services.operator_routing_incident_rca_results_persister_v2_10 import (
    compute_output_hash,
)
from orderflow_services.operator_routing_incident_rca_quality_scorer_v2_10 import (
    evaluate_quality,
)
from orderflow_services.operator_routing_incident_rca_feedback_loop_v2_10 import (
    score_usefulness,
)


def test_compute_output_hash_is_stable():
    payload1 = {
        "route_change_id": "rc-10",
        "provider": "vertex",
        "model_name": "gemini-2.5",
        "result_json": '{"status": "ok"}',
    }
    payload2 = dict(payload1)
    payload2["extra"] = "ignored"
    
    h1 = compute_output_hash(payload1)
    h2 = compute_output_hash(payload2)
    assert h1 == h2
    assert len(h1) == 16


def test_evaluate_quality_scoring():
    empty = {"result_json": "{}"}
    score_empty = evaluate_quality(empty)
    assert score_empty["quality_score"] == 0.0
    assert len(score_empty["quality_reasons"]) == 3
    
    perfect = {
        "result_json": """,
        {
            "summary": "Full analysis",
            "findings": [{"evidence": ["log1"]}],
            "recommendations": [{"action": "HOLD"}]
        }
        """
    }
    score_perfect = evaluate_quality(perfect)
    assert score_perfect["quality_score"] == 1.0
    assert len(score_perfect["quality_reasons"]) == 0


def test_score_usefulness_mapping():
    assert score_usefulness("VERY_USEFUL") == 1.0
    assert score_usefulness("useful") == 0.75
    assert score_usefulness("Mixed") == 0.50
    assert score_usefulness("UNKNOWN") == 0.0
