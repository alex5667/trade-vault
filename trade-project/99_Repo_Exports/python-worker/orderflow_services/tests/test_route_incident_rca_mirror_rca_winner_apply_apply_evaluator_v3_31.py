from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_evaluator_v3_31 import (
    arm_from_request_id,
    recommend,
    scorecards_from_rows,
)


def _policy():
    return {
        "min_exposures": 10,
        "min_feedback": 5,
        "min_result_coverage": 0.30,
        "min_feedback_coverage": 0.20,
        "min_quality": 0.55,
        "min_usefulness": 0.60,
        "min_accepted_rate": 0.60,
        "min_score_margin": 0.05,
        "incumbent_arm": "deterministic",
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
    }


def test_arm_from_request_id_parses_suffix():
    assert arm_from_request_id("bundle-1:vertex_candidate") == "vertex_candidate"
    assert arm_from_request_id("bundle-1:local_fallback_candidate") == "local_fallback_candidate"
    assert arm_from_request_id("bundle-1") == ""


def test_scorecards_and_recommend_promote_vertex():
    exposures = []
    for _ in range(20):
        exposures.append({"arm": "deterministic", "ts_ms": "9999999999999"})
        exposures.append({"arm": "vertex_candidate", "ts_ms": "9999999999999"})

    results = []
    for _ in range(10):
        results.append({"request_id": "bundle-a:deterministic", "ts_ms": "9999999999999"})
    for _ in range(12):
        results.append({"request_id": "bundle-b:vertex_candidate", "ts_ms": "9999999999999"})

    feedback = []
    for _ in range(6):
        feedback.append({
            "request_id": "bundle-a:deterministic",
            "quality_score": 0.58,
            "usefulness_score": 0.60,
            "accepted": 1,
            "ts_ms": "9999999999999",
        })
    for _ in range(8):
        feedback.append({
            "request_id": "bundle-b:vertex_candidate",
            "quality_score": 0.85,
            "usefulness_score": 0.90,
            "accepted": 1,
            "ts_ms": "9999999999999",
        })

    cards = scorecards_from_rows(exposures, results, feedback, _policy())
    out = recommend(cards, _policy())
    assert out["decision"] == "PROMOTE_VERTEX_CANDIDATE"
    assert out["winner_arm"] == "vertex_candidate"


def test_recommend_keep_when_no_candidate_eligible():
    exposures = [{"arm": "deterministic", "ts_ms": "9999999999999"} for _ in range(3)]
    results = []
    feedback = []
    cards = scorecards_from_rows(exposures, results, feedback, _policy())
    out = recommend(cards, _policy())
    assert out["decision"] == "KEEP_DETERMINISTIC"
