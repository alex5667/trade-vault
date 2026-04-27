from orderflow_services.route_incident_rca_mirror_rca_winner_apply_verification_loop_v3_17 import verify_exposures

def test_verify_exposures_keep_applied():
    exposures = [
        {"role": "primary", "arm": "vertex_candidate"},
        {"role": "primary", "arm": "vertex_candidate"},
        {"role": "primary", "arm": "vertex_candidate"},
        {"role": "primary", "arm": "vertex_candidate"},
        {"role": "shadow", "arm": "deterministic"},
    ]
    target_mode = "SHADOW"
    target_primary = "vertex_candidate"
    
    # Needs at least 5 exposures (based on default MIN_EXPOSURES logic handled centrally)
    decision, reason, rates = verify_exposures(exposures, target_mode, target_primary)
    assert decision == "KEEP_APPLIED"
    assert rates["primary_match_rate"] == 0.8 # 4 out of 5
    assert rates["unexpected_primary_rate"] == 0.0

def test_verify_exposures_rollback_low_primary_match():
    # Only 3 total exposures where 4 primary was expected, heavily skewed shadows
    exposures = [
        {"role": "primary", "arm": "vertex_candidate"},
        {"role": "shadow", "arm": "deterministic"},
        {"role": "shadow", "arm": "deterministic"},
        {"role": "shadow", "arm": "deterministic"},
        {"role": "shadow", "arm": "deterministic"},
        {"role": "shadow", "arm": "deterministic"},
    ]
    target_mode = "SHADOW"
    target_primary = "vertex_candidate"
    
    # 1 primary out of 6 total = 16% < 80% default threshold
    decision, reason, rates = verify_exposures(exposures, target_mode, target_primary)
    assert decision == "ROLLBACK_PREVIOUS_POLICY"
    assert "LOW_PRIMARY_MATCH_RATE" in reason
    
def test_verify_exposures_rollback_unexpected_primary():
    exposures = [
        {"role": "primary", "arm": "deterministic"},
        {"role": "primary", "arm": "deterministic"},
        {"role": "primary", "arm": "vertex_candidate"}, # Match
        {"role": "shadow", "arm": "local_fallback_candidate"},
        {"role": "shadow", "arm": "local_fallback_candidate"},
    ]
    target_mode = "SHADOW"
    target_primary = "vertex_candidate"
    
    # 2 unexpected primary out of 5 = 40% > 20% default threshold
    decision, reason, rates = verify_exposures(exposures, target_mode, target_primary)
    assert decision == "ROLLBACK_PREVIOUS_POLICY"
    assert "HIGH_UNEXPECTED_PRIMARY_RATE" in reason

def test_verify_exposures_hold_insufficient():
    # Only 3 exposures vs MIN_EXPOSURES=5 requirement in function
    exposures = [
        {"role": "primary", "arm": "vertex_candidate"},
    ]
    
    decision, reason, rates = verify_exposures(exposures, "SHADOW", "vertex_candidate")
    assert decision == "HOLD"
    assert "insufficient_exposures" in reason
