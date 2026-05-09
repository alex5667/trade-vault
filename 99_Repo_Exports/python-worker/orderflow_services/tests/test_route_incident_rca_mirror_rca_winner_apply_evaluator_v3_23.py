from orderflow_services.route_incident_rca_mirror_rca_winner_apply_evaluator_v3_23 import (
    build_scorecards,
    evaluate_winner,
    parse_arm_from_request_id,
)


def test_parse_arm():
    assert parse_arm_from_request_id("req_123_vertex_candidate") == "vertex_candidate"
    assert parse_arm_from_request_id("req_xyz_local_fallback_candidate") == "local_fallback_candidate"
    assert parse_arm_from_request_id("req_123_deterministic") == "deterministic"
    assert parse_arm_from_request_id("req_unknown") == "deterministic" # default incumbent

def test_build_scorecards():
    exposures = [
        {"arm": "deterministic"}, {"arm": "deterministic"},
        {"arm": "vertex_candidate"}, {"arm": "vertex_candidate"}
    ]
    results = [
        {"request_id": "req_1_deterministic"},
        {"request_id": "req_1_vertex_candidate"}
    ]
    feedbacks = [
        # Deterministic: 1 feedback
        {"request_id": "req_1_deterministic", "quality_score": "0.9", "usefulness_score": "0.9", "accepted": "1"},
        # Vertex: 1 feedback
        {"request_id": "req_1_vertex_candidate", "quality_score": "0.95", "usefulness_score": "0.95", "accepted": "1"}
    ]

    scorecards = build_scorecards(exposures, results, feedbacks)

    det = scorecards["deterministic"]
    assert det["exposure_n"] == 2
    assert det["result_n"] == 1
    assert det["feedback_n"] == 1
    assert det["result_coverage"] == 0.5
    assert det["feedback_coverage"] == 0.5

    # 0.9 * 0.3 + 0.9 * 0.4 + 1.0 * 0.3 = 0.27 + 0.36 + 0.3 = 0.93
    # coverage mult = sqrt(0.5 * 0.5) = 0.5
    # score = 0.93 * 0.5 = 0.465
    assert abs(det["score"] - 0.465) < 0.001
    assert det["eligible"] == 0 # Min exposures not met by defaults

def test_evaluate_winner_ineligible():
    sc = {
        "deterministic": {"score": 0.5, "eligible": 0},
        "vertex_candidate": {"score": 0.9, "eligible": 0}
    }
    rec, arm = evaluate_winner(sc)
    assert rec == "KEEP_DETERMINISTIC"
    assert arm == "deterministic"

def test_evaluate_winner_promote():
    sc = {
        "deterministic": {"score": 0.5, "eligible": 1},
        "vertex_candidate": {"score": 0.9, "eligible": 1} # 0.9 - 0.5 >= 0.05
    }
    rec, arm = evaluate_winner(sc)
    assert rec == "PROMOTE_VERTEX_CANDIDATE"
    assert arm == "vertex_candidate"
