from orderflow_services.operator_routing_incident_rca_usefulness_governor_v2_11 import (
    evaluate_score,
    MIN_SAMPLE,
    SUPPRESS_SCORE_MAX,
    PROMOTE_SCORE_MIN,
)


def test_evaluate_score_logic():
    # Not enough samples
    action, score = evaluate_score(MIN_SAMPLE - 1, 1.0, 1.0)
    assert action == "HOLD"
    assert score == 0.0

    # Suppress due to low score
    action, score = evaluate_score(MIN_SAMPLE + 1, SUPPRESS_SCORE_MAX, SUPPRESS_SCORE_MAX)
    assert action == "SUPPRESS"
    assert score <= SUPPRESS_SCORE_MAX

    # Promote due to high score
    action, score = evaluate_score(MIN_SAMPLE + 1, PROMOTE_SCORE_MIN + 0.1, PROMOTE_SCORE_MIN + 0.1)
    assert action == "PROMOTE"
    assert score > PROMOTE_SCORE_MIN

    # Hold (mixed score)
    mid = (SUPPRESS_SCORE_MAX + PROMOTE_SCORE_MIN) / 2
    action, score = evaluate_score(MIN_SAMPLE + 1, mid, mid)
    assert action == "HOLD"
