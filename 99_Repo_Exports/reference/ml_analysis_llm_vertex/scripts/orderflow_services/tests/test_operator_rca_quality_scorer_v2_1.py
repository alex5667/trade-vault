from orderflow_services.operator_rca_quality_scorer_v2_1 import score_output

def test_score_output_good_payload():
    payload = {
        "summary": "Drift on top feature after promote.",
        "findings": [{"kind": "drift", "evidence": ["psi", "ece"]}, {"kind": "drift", "evidence": ["psi", "ece"]}],
        "recommendations": [{"action": "require_shadow_retrain", "risk": "low"}, {"action": "require_shadow_retrain", "risk": "low"}],
    }
    score, reasons, parts = score_output(payload)
    assert score >= 60.0
    assert parts["summary"] > 0
    assert "QUALITY_BELOW_THRESHOLD" not in reasons

def test_score_output_bad_payload():
    payload = {"summary": "", "findings": [], "recommendations": []}
    score, reasons, _parts = score_output(payload)
    assert score < 60.0
    assert "MISSING_SUMMARY" in reasons
    assert "NO_FINDINGS" in reasons
