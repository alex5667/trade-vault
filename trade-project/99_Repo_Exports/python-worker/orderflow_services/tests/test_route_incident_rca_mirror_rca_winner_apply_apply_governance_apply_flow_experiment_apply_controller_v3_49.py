from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_v3_49 import (
    evaluate_apply,
    extract_score_margin,
    infer_profile_name,
    policy_from_hash,
)


def _controller_policy():
    return policy_from_hash(
        {
            "enabled": "1",
            "kill_switch": "0",
            "advisory_only": "1",
            "executor_mode": "DRY_RUN",
            "allow_commit": "0",
            "cooldown_sec": "21600",
            "min_score_margin": "0.05",
            "allow_winner_arms_json": '["vertex_compact_candidate","local_candidate"]',
        }
    )


def _exp_policy():
    return {
        "mode": "SHADOW",
        "vertex_primary_weight": 50,
        "vertex_compact_weight": 30,
        "local_candidate_weight": 20,
        "last_weight_rebalance_ts_ms": 0,
    }


def _winner_policy():
    return {"incumbent_arm": "vertex_primary"}


def test_extract_score_margin():
    scorecards_json = '{"vertex_primary":{"score":0.60},"local_candidate":{"score":0.72}}'
    margin = extract_score_margin(scorecards_json, "vertex_primary", "local_candidate")
    assert margin == 0.12


def test_apply_local_profile_when_margin_is_good():
    row = {
        "decision": "PROMOTE_LOCAL_CANDIDATE",
        "winner_arm": "local_candidate",
        "scorecards_json": '{"vertex_primary":{"score":0.60},"local_candidate":{"score":0.72}}',
    }
    out = evaluate_apply(row, _exp_policy(), _winner_policy(), _controller_policy())
    assert out["decision"] == "APPLY_LOCAL_PROFILE"
    assert out["target_profile"] == "local_profile"
    assert out["target_incumbent_arm"] == "local_candidate"


def test_hold_when_margin_too_small():
    row = {
        "decision": "PROMOTE_VERTEX_COMPACT_CANDIDATE",
        "winner_arm": "vertex_compact_candidate",
        "scorecards_json": '{"vertex_primary":{"score":0.60},"vertex_compact_candidate":{"score":0.62}}',
    }
    out = evaluate_apply(row, _exp_policy(), _winner_policy(), _controller_policy())
    assert out["decision"] == "HOLD"
    assert out["reason_code"] == "SCORE_MARGIN_TOO_SMALL"


def test_infer_profile_name_matches_defaults():
    policy = _controller_policy()
    name = infer_profile_name(
        {"vertex_primary_weight": 30, "vertex_compact_weight": 50, "local_candidate_weight": 20},
        policy["profiles"],
    )
    assert name == "vertex_compact_profile"
