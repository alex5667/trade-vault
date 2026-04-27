from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundle_builder_v3_43 import (
    normalize_trigger,
    should_trigger_bundle,
)


def test_normalize_apply_decision_trigger():
    row = {
        "decision": "APPLY_PRIMARY_ARM_SHADOW",
        "reason_code": "PROMOTE_WINNER_TO_SHADOW_PRIMARY",
        "ts_ms": "9999999999999",
    }
    out = normalize_trigger("apply_decisions", row)
    assert out["trigger_type"] == "apply_decision"
    assert out["severity"] == "warning"


def test_normalize_verification_trigger():
    row = {
        "decision": "ROLLBACK_PREVIOUS_POLICY",
        "reason_code": "PRIMARY_MATCH_RATE_TOO_LOW",
        "ts_ms": "9999999999999",
    }
    out = normalize_trigger("verification", row)
    assert out["trigger_type"] == "verification"
    assert out["severity"] == "critical"


def test_normalize_retry_trigger():
    row = {
        "decision": "EXHAUSTED",
        "reason_code": "MAX_ATTEMPTS_REACHED",
        "ts_ms": "9999999999999",
    }
    out = normalize_trigger("retry", row)
    assert out["trigger_type"] == "retry"
    assert out["severity"] == "critical"


def test_should_trigger_bundle_for_critical_escalation():
    trigger = {
        "trigger_type": "escalation",
        "transition_type": "NONE",
        "severity": "critical",
        "reason_code": "RETRY_EXHAUSTED",
        "ts_ms": 9999999999999,
        "row": {},
    }
    assert should_trigger_bundle(trigger) is True
