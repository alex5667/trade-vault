import json

from orderflow_services.route_incident_rca_mirror_rca_winner_apply_verification_loop_v3_25 import (
    build_rollback_state,
    calculate_rates,
    evaluate_verification,
)


def test_calculate_rates():
    exposures = [
        {"mode": "SHADOW", "arm": "vertex_candidate"},
        {"mode": "SHADOW", "arm": "vertex_candidate"},
        {"mode": "SHADOW", "arm": "deterministic"} # Not primary if target is vertex
    ]
    t, pmr, upr, sr = calculate_rates(exposures, "SHADOW", "vertex_candidate")
    assert t == 3
    assert abs(pmr - 0.666) < 0.01 # 2/3
    assert abs(upr - 0.333) < 0.01 # 1 is deterministic (which doesn't contain 'shadow' in name, but is not target)
    assert abs(sr - 0.333) < 0.01

def test_evaluate_verification_keep():
    dec, rco = evaluate_verification(10, "SHADOW", 0.9, 0.05, 0.05)
    assert dec == "KEEP_APPLIED"

def test_evaluate_verification_low_match():
    dec, rco = evaluate_verification(10, "SHADOW", 0.79, 0.05, 0.05)
    assert dec == "ROLLBACK_PREVIOUS_POLICY"
    assert rco == "LOW_PRIMARY_MATCH_RATE"

def test_evaluate_verification_hold():
    dec, rco = evaluate_verification(4, "SHADOW", 1.0, 0.0, 0.0)
    assert dec == "HOLD"
    assert rco == "INSUFFICIENT_DATA"

def test_build_rollback_state():
    harness = {
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE": "SINGLE_ARM",
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM": "vertex_candidate"
    }
    rb = build_rollback_state(harness, "SHADOW", "deterministic")
    assert rb["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE"] == "SHADOW"
    assert rb["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM"] == "deterministic"
    shadows = json.loads(rb["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"])
    assert "vertex_candidate" in shadows
    assert "local_fallback_candidate" in shadows
    assert "deterministic" not in shadows
