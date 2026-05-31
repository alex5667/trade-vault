from orderflow_services.operator_rca_experiment_router_v2_4 import (
    ArmSpec,
    build_experiment_assignment,
    choose_arm,
    parse_experiment_arms,
)
from orderflow_services.operator_rca_experiment_winner_selector_v2_4 import (
    aggregate_arm_stats,
    choose_winner,
)


def test_parse_and_choose_arm_is_deterministic():
    arms = parse_experiment_arms(
        '[{"name":"control","weight":0.7,"provider":"vertex","model_name":"a","prompt_version":"p1","policy_version":"pv1"},'
        '{"name":"challenger","weight":0.3,"provider":"vertex","model_name":"b","prompt_version":"p2","policy_version":"pv1"}]'
    )
    a1 = choose_arm("exp1", "req-123", arms)
    a2 = choose_arm("exp1", "req-123", arms)
    assert a1 is not None
    assert a1 == a2


def test_build_experiment_assignment_sets_arm_fields():
    arm = ArmSpec("challenger", 1.0, "vertex", "gemini-2.5-flash", "ml_triage_v2", "policy_v1")
    assigned, exposure = build_experiment_assignment({"request_id": "r1", "ts_ms": 1000}, "exp1", arm)
    assert assigned["experiment_arm"] == "challenger"
    assert assigned["model_name"] == "gemini-2.5-flash"
    assert exposure["experiment_id"] == "exp1"


def test_winner_selection_prefers_high_usefulness():
    now_ms = 1_700_000_000_000
    exposures = [
        {"request_id": "r1", "experiment_id": "exp1", "arm": "control", "provider": "vertex", "model_name": "lite", "prompt_version": "p1", "ts_ms": now_ms},
        {"request_id": "r2", "experiment_id": "exp1", "arm": "control", "provider": "vertex", "model_name": "lite", "prompt_version": "p1", "ts_ms": now_ms},
        {"request_id": "r3", "experiment_id": "exp1", "arm": "challenger", "provider": "vertex", "model_name": "flash", "prompt_version": "p2", "ts_ms": now_ms},
        {"request_id": "r4", "experiment_id": "exp1", "arm": "challenger", "provider": "vertex", "model_name": "flash", "prompt_version": "p2", "ts_ms": now_ms},
    ]
    quality = [
        {"request_id": "r1", "quality_score": 0.8, "ts_ms": now_ms},
        {"request_id": "r2", "quality_score": 0.8, "ts_ms": now_ms},
        {"request_id": "r3", "quality_score": 0.75, "ts_ms": now_ms},
        {"request_id": "r4", "quality_score": 0.75, "ts_ms": now_ms},
    ]
    feedback = [
        {"request_id": "r1", "decision": "MIXED", "ts_ms": now_ms},
        {"request_id": "r2", "decision": "MIXED", "ts_ms": now_ms},
        {"request_id": "r3", "decision": "VERY_USEFUL", "ts_ms": now_ms},
        {"request_id": "r4", "decision": "VERY_USEFUL", "ts_ms": now_ms},
    ]
    stats = aggregate_arm_stats(exposures, quality, feedback, now_ms, 1440)
    decisions = choose_winner(stats, min_sample=2)
    assert len(decisions) == 1
    assert decisions[0]["winning_arm"] == "challenger"
    assert decisions[0]["decision"] == "PROMOTE"


def test_winner_selection_holds_when_not_enough_sample():
    now_ms = 1_700_000_000_000
    exposures = [{"request_id": "r1", "experiment_id": "exp1", "arm": "control", "provider": "vertex", "model_name": "lite", "prompt_version": "p1", "ts_ms": now_ms}]
    stats = aggregate_arm_stats(exposures, [], [], now_ms, 1440)
    decisions = choose_winner(stats, min_sample=2)
    assert decisions[0]["decision"] == "HOLD"
    assert decisions[0]["reason"] == "INSUFFICIENT_SAMPLE"
