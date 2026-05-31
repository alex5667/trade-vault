import json

from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_vertex_feedback_governor_v3_29 import (
    calculate_rollups,
    evaluate_governance,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_consumer_v3_29 import (
    build_deterministic_result,
)


def test_build_deterministic_result_apply():
    bundle_json = json.dumps({"trigger": {"type": "apply", "severity": "warning"}})
    res = build_deterministic_result(bundle_json)
    assert "APPLY_CONTROLLER_TRIGGERED" in res["dominant_findings"]
    assert "normal_transition" in res["hypotheses"]

def test_build_deterministic_result_rollback():
    bundle_json = json.dumps({"trigger": {"type": "rollback", "severity": "critical"}})
    res = build_deterministic_result(bundle_json)
    assert "ROLLBACK_TRIGGERED" in res["dominant_findings"]
    assert "policy_mismatch" in res["hypotheses"]

def test_calculate_rollups():
    feedbacks = [
        {"quality_score": "0.9", "usefulness_score": "0.8", "accepted": "1"},
        {"quality_score": "0.3", "usefulness_score": "0.4", "accepted": "0"}
    ]

    q, u, acc, low_q = calculate_rollups(feedbacks)
    assert q == 0.6
    assert u == 0.6
    assert acc == 0.5
    assert low_q == 0.5  # 1 out of 2 is below 0.4

def test_evaluate_governance_hold():
    dec = evaluate_governance(samples=5, avg_q=0.9, avg_u=0.9, acc_r=1.0, low_q_r=0.0)
    assert dec == "HOLD"

def test_evaluate_governance_keep_auto():
    dec = evaluate_governance(samples=15, avg_q=0.9, avg_u=0.9, acc_r=1.0, low_q_r=0.0)
    assert dec == "KEEP_AUTO"

def test_evaluate_governance_prefer_local():
    dec = evaluate_governance(samples=15, avg_q=0.5, avg_u=0.9, acc_r=1.0, low_q_r=0.0)
    assert dec == "PREFER_LOCAL_ONLY"
