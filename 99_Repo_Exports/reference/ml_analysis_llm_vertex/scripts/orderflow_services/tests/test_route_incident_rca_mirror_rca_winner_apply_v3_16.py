import json
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_controller_v3_16 import build_new_config

def test_build_new_config_shadow_primary():
    current_cfg = {
        "mode": "SHADOW",
        "primary_arm": "deterministic",
        "shadow_arms": '["vertex_candidate", "local_fallback_candidate"]'
    }
    winner = "vertex_candidate"
    strategy = "SHADOW_PRIMARY"
    allow_arms = ["vertex_candidate", "local_fallback_candidate"]
    
    can_apply, new_cfg, reason = build_new_config(current_cfg, winner, strategy, allow_arms)
    assert can_apply is True
    assert new_cfg["mode"] == "SHADOW"
    assert new_cfg["primary_arm"] == "vertex_candidate"
    
    shadows = json.loads(new_cfg["shadow_arms"])
    assert "local_fallback_candidate" in shadows
    assert "deterministic" in shadows
    assert "vertex_candidate" not in shadows
    
def test_build_new_config_single_arm():
    current_cfg = {
        "mode": "SHADOW",
        "primary_arm": "deterministic",
        "shadow_arms": '["local_fallback_candidate"]'
    }
    winner = "local_fallback_candidate"
    strategy = "SINGLE_ARM"
    allow_arms = ["vertex_candidate", "local_fallback_candidate"]
    
    can_apply, new_cfg, reason = build_new_config(current_cfg, winner, strategy, allow_arms)
    assert can_apply is True
    assert new_cfg["mode"] == "SINGLE_ARM"
    assert new_cfg["primary_arm"] == "local_fallback_candidate"
    assert new_cfg["shadow_arms"] == "[]"

def test_build_new_config_not_allowed():
    current_cfg = {"mode": "SHADOW", "primary_arm": "deterministic"}
    allow_arms = ["vertex_candidate"]
    
    can_apply, new_cfg, reason = build_new_config(current_cfg, "unauthorized_arm", "SINGLE_ARM", allow_arms)
    assert can_apply is False
    assert "not allowed" in reason

def test_build_new_config_already_primary():
    current_cfg = {"mode": "SHADOW", "primary_arm": "vertex_candidate"}
    allow_arms = ["vertex_candidate"]
    
    can_apply, new_cfg, reason = build_new_config(current_cfg, "vertex_candidate", "SINGLE_ARM", allow_arms)
    assert can_apply is False
    assert "already primary" in reason
