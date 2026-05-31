import pytest

from orderflow_services.route_incident_rca_mirror_rca_winner_apply_vertex_feedback_governor_v3_21 import (
    calculate_rollups,
    decide_governance,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_vertex_rca_consumer_v3_21 import (
    generate_deterministic_result,
)


@pytest.mark.asyncio
async def test_generate_deterministic_result():
    res = await generate_deterministic_result("req_123", '{"trigger":{"description":"ROLLBACK_MTTR_P95_HIGH", "severity":"critical"}}')
    assert res["request_id"] == "req_123"
    assert res["severity"] == "critical"
    assert res["rca_payload"]["dominant_findings"][0] == "system_lag_or_persistence_issue"

@pytest.mark.asyncio
async def test_generate_deterministic_result_keep_rate():
    res = await generate_deterministic_result("req_124", '{"trigger":{"description":"verify_keep_rate drop", "severity":"warning"}}')
    assert res["rca_payload"]["dominant_findings"][0] == "model_hallucination_or_mismatch"

def test_calculate_rollups_empty():
    res = calculate_rollups([])
    assert res["n"] == 0
    assert res["avg_q"] == 0.0

def test_calculate_rollups_values():
    feedbacks = [
        {"quality_score": "0.9", "usefulness_score": "0.8", "accepted": "1"},
        {"quality_score": "0.1", "usefulness_score": "0.2", "accepted": "0"}
    ]
    res = calculate_rollups(feedbacks)
    assert res["n"] == 2
    assert res["avg_q"] == 0.5
    assert res["avg_u"] == 0.5
    assert res["acc_r"] == 0.5
    assert res["low_q"] == 0.5 # One out of two was under 0.5 quality

def test_decide_governance_hold():
    res = calculate_rollups([])
    assert decide_governance(res, min_n=10, min_q=0.5, min_u=0.5, min_a=0.5, max_lq=0.5) == "HOLD"

def test_decide_governance_prefer_local():
    # Only 0.5 avg_q vs 0.6 minimum
    feedbacks = [
        {"quality_score": "0.9", "usefulness_score": "0.8", "accepted": "1"},
        {"quality_score": "0.1", "usefulness_score": "0.2", "accepted": "0"}
    ]
    res = calculate_rollups(feedbacks)
    # Set n higher so it doesn't HOLD
    res["n"] = 10
    assert decide_governance(res, min_n=10, min_q=0.6, min_u=0.5, min_a=0.5, max_lq=1.0) == "PREFER_LOCAL_ONLY"

def test_decide_governance_keep_auto():
    feedbacks = [
        {"quality_score": "0.9", "usefulness_score": "0.8", "accepted": "1"},
        {"quality_score": "0.9", "usefulness_score": "0.8", "accepted": "1"}
    ]
    res = calculate_rollups(feedbacks)
    res["n"] = 10
    assert decide_governance(res, min_n=10, min_q=0.6, min_u=0.6, min_a=0.6, max_lq=0.35) == "KEEP_AUTO"
