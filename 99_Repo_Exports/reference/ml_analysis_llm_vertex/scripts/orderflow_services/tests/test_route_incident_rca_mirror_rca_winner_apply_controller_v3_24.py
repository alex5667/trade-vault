import json
import pytest
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_controller_v3_24 import calculate_new_harness_state

def test_shadow_primary_transition():
    harness = {
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE": "SHADOW",
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM": "deterministic",
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON": '["vertex_candidate"]'
    }
    
    # We want to promote vertex_candidate using SHADOW_PRIMARY
    ok, n_harness, msg = calculate_new_harness_state("vertex_candidate", "SHADOW_PRIMARY", harness, ["vertex_candidate"])
    
    assert ok
    assert n_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE"] == "SHADOW"
    assert n_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM"] == "vertex_candidate"
    
    # Deterministic should now be in shadows
    shadows = json.loads(n_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"])
    assert "deterministic" in shadows
    assert "vertex_candidate" not in shadows

def test_single_arm_transition():
    harness = {
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE": "SHADOW",
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM": "deterministic",
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON": '["local_fallback_candidate"]'
    }
    
    # We want to promote local_fallback_candidate using SINGLE_ARM
    ok, n_harness, msg = calculate_new_harness_state("local_fallback_candidate", "SINGLE_ARM", harness, ["local_fallback_candidate"])
    
    assert ok
    assert n_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE"] == "SINGLE_ARM"
    assert n_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM"] == "local_fallback_candidate"
    assert n_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"] == "[]"

def test_not_in_bounded_arms():
    harness = {}
    ok, _, _ = calculate_new_harness_state("hacker_arm", "SINGLE_ARM", harness, ["vertex_candidate"])
    assert not ok

def test_already_in_state():
    harness = {
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE": "SHADOW",
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM": "vertex_candidate"
    }
    ok, _, _ = calculate_new_harness_state("vertex_candidate", "SHADOW_PRIMARY", harness, ["vertex_candidate"])
    assert not ok
