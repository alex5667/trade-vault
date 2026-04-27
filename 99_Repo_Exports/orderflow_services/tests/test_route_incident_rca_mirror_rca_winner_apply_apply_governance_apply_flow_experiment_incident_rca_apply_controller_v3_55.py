from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_apply_controller_v3_55 import (
    evaluate_apply,
    policy_from_hash,
    target_mode_from_usefulness_decision,
)


def _policy():
    return policy_from_hash(
        {
            "enabled": "1",
            "kill_switch": "0",
            "advisory_only": "1",
            "executor_mode": "DRY_RUN",
            "allow_commit": "0",
            "cooldown_sec": "21600",
            "allowed_target_modes_json": '["AUTO","VERTEX_ONLY","LOCAL_ONLY"]',
        }
    )


def test_target_mode_mapping():
    assert target_mode_from_usefulness_decision("PREFER_VERTEX_ONLY", "AUTO") == "VERTEX_ONLY"
    assert target_mode_from_usefulness_decision("RETURN_TO_AUTO", "LOCAL_ONLY") == "AUTO"


def test_apply_vertex_only_when_usefulness_prefers_vertex():
    row = {
        "decision": "PREFER_VERTEX_ONLY",
        "reason_code": "VERTEX_BETTER_THAN_LOCAL",
    }
    out = evaluate_apply(row, "AUTO", _policy(), cooldown_active=False)
    assert out["decision"] == "APPLY_VERTEX_ONLY"
    assert out["target_bridge_mode"] == "VERTEX_ONLY"


def test_apply_auto_when_return_to_auto():
    row = {
        "decision": "RETURN_TO_AUTO",
        "reason_code": "LOCAL_ONLY_UNDERPERFORMS",
    }
    out = evaluate_apply(row, "LOCAL_ONLY", _policy(), cooldown_active=False)
    assert out["decision"] == "APPLY_AUTO"
    assert out["target_bridge_mode"] == "AUTO"


def test_hold_when_cooldown_is_active():
    row = {
        "decision": "PREFER_LOCAL_ONLY",
        "reason_code": "LOCAL_BETTER_THAN_VERTEX",
    }
    out = evaluate_apply(row, "AUTO", _policy(), cooldown_active=True)
    assert out["decision"] == "HOLD"
    assert out["reason_code"] == "COOLDOWN_ACTIVE"
