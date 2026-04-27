from orderflow_services.llm_recommendation_feedback_loop_v1 import normalize_feedback, update_summary


def test_normalize_feedback_defaults_and_lowercases():
    fb = normalize_feedback({"recommendation_id": "r1", "verdict": "Accepted", "action": "freeze_candidate"})
    assert fb.recommendation_id == "r1"
    assert fb.verdict == "accepted"
    assert fb.action == "freeze_candidate"


def test_update_summary_counts_total_and_verdict():
    fb = normalize_feedback({"recommendation_id": "r1", "verdict": "rejected", "action": "freeze_candidate"})
    out = update_summary({}, fb)
    assert out["rejected"] == 1
    assert out["total"] == 1
