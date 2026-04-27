from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_apply_controller_v3_32 import (
    evaluate_apply,
)


def _controller_policy(strategy: str = "SHADOW_PRIMARY"):
    return {
        "enabled": 1,
        "kill_switch": 0,
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "apply_strategy": strategy,
        "cooldown_sec": 21600,
        "min_winner_score": 0.0,
        "allow_arms": ["vertex_candidate", "local_fallback_candidate"],
    }


def _experiment_policy():
    return {
        "mode": "SHADOW",
        "primary_arm": "deterministic",
        "shadow_arms": ["vertex_candidate", "local_fallback_candidate"],
        "last_mode_switch_ts_ms": 0,
    }


def test_shadow_primary_apply_for_vertex_candidate():
    recommendation = {
        "decision": "PROMOTE_VERTEX_CANDIDATE",
        "winner_arm": "vertex_candidate",
        "winner_score": "0.72",
    }
    out = evaluate_apply(
        recommendation=recommendation,
        controller_policy=_controller_policy("SHADOW_PRIMARY"),
        experiment_policy=_experiment_policy(),
        now_ts_ms=100000,
    )
    assert out["decision"] == "APPLY_PRIMARY_ARM_SHADOW"
    assert out["target_mode"] == "SHADOW"
    assert out["target_primary_arm"] == "vertex_candidate"


def test_single_arm_apply_for_local_candidate():
    recommendation = {
        "decision": "PROMOTE_LOCAL_FALLBACK_CANDIDATE",
        "winner_arm": "local_fallback_candidate",
        "winner_score": "0.81",
    }
    out = evaluate_apply(
        recommendation=recommendation,
        controller_policy=_controller_policy("SINGLE_ARM"),
        experiment_policy=_experiment_policy(),
        now_ts_ms=100000,
    )
    assert out["decision"] == "APPLY_SINGLE_ARM"
    assert out["target_mode"] == "SINGLE_ARM"
    assert out["target_primary_arm"] == "local_fallback_candidate"


def test_keep_when_recommendation_is_not_promotion():
    recommendation = {
        "decision": "KEEP_DETERMINISTIC",
        "winner_arm": "deterministic",
        "winner_score": "0.55",
    }
    out = evaluate_apply(
        recommendation=recommendation,
        controller_policy=_controller_policy("SHADOW_PRIMARY"),
        experiment_policy=_experiment_policy(),
        now_ts_ms=100000,
    )
    assert out["decision"] == "HOLD"
    assert out["reason_code"] == "RECOMMENDATION_NOT_PROMOTION"
