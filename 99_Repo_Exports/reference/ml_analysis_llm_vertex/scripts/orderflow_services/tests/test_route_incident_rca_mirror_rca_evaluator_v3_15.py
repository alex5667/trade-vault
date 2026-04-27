import json
from orderflow_services.route_incident_rca_mirror_rca_evaluator_v3_15 import parse_arm_from_request_id, ArmMetrics, select_winner

def test_parse_arm_from_request_id():
    assert parse_arm_from_request_id("bundle_id:vertex_candidate") == "vertex_candidate"
    assert parse_arm_from_request_id("something_without_colon") == "deterministic"

def test_select_winner_promote_vertex():
    scorecards = {
        "deterministic": {
            "eligible": True,
            "score": 0.50
        },
        "vertex_candidate": {
            "eligible": True,
            "score": 0.60
        }
    }
    
    winner = select_winner(scorecards, "deterministic", 0.05)
    assert winner == "vertex_candidate"

def test_select_winner_keep_deterministic_due_to_margin():
    scorecards = {
        "deterministic": {
            "eligible": True,
            "score": 0.50
        },
        "vertex_candidate": {
            "eligible": True,
            "score": 0.52
        }
    }
    
    winner = select_winner(scorecards, "deterministic", 0.05)
    assert winner == "deterministic"

def test_select_winner_keep_deterministic_due_to_ineligible():
    scorecards = {
        "deterministic": {
            "eligible": True,
            "score": 0.50
        },
        "vertex_candidate": {
            "eligible": False,
            "score": 0.80
        }
    }
    
    winner = select_winner(scorecards, "deterministic", 0.05)
    assert winner == "deterministic"

def test_scorecard_computation():
    metrics = ArmMetrics("vertex_candidate")
    
    # Needs to meet global thresholds to be eligible
    for i in range(15):
        metrics.add_exposure()
    for i in range(10):
        metrics.add_result()
    for i in range(8):
        metrics.add_feedback(0.8, 0.9, 1.0)
        
    sc = metrics.compute_scorecard()
    assert sc["arm"] == "vertex_candidate"
    assert sc["exposure_n"] == 15
    assert sc["result_n"] == 10
    assert sc["feedback_n"] == 8
    import math
    assert math.isclose(sc["avg_quality"], 0.8)
    assert math.isclose(sc["avg_usefulness"], 0.9)
    assert math.isclose(sc["accepted_rate"], 1.0)
    
    assert sc["eligible"] is True
