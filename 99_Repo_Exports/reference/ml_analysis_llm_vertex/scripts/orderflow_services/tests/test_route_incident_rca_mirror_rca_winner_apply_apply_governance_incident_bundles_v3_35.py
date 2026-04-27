from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundle_builder_v3_35 import (
    normalize_trigger,
    should_trigger_bundle,
)


def test_normalize_trigger_for_apply_transition():
    row = {
        "decision": "APPLY_PRIMARY_ARM_SHADOW",
        "reason_code": "PROMOTE_WINNER_TO_SHADOW_PRIMARY",
        "ts_ms": "9999999999999",
    }
    out = normalize_trigger("apply_journal", row)
    assert out["trigger_type"] == "apply_transition"
    assert out["severity"] == "warning"


def test_normalize_trigger_for_rollback():
    row = {
        "reason_code": "PRIMARY_MATCH_RATE_TOO_LOW",
        "ts_ms": "9999999999999",
    }
    out = normalize_trigger("rollback_journal", row)
    assert out["trigger_type"] == "rollback"
    assert out["severity"] == "critical"


def test_normalize_trigger_for_escalation():
    row = {
        "severity": "warning",
        "summary_json": '{"severity":"warning","reason_codes":["VERIFY_KEEP_RATE_LOW"]}',
        "ts_ms": "9999999999999",
    }
    out = normalize_trigger("escalations", row)
    assert out["trigger_type"] == "escalation"
    assert out["severity"] == "warning"


def test_should_trigger_bundle_for_critical_escalation():
    trigger = {
        "trigger_type": "escalation",
        "transition_type": "NONE",
        "severity": "critical",
        "ts_ms": 9999999999999,
        "row": {},
    }
    assert should_trigger_bundle(trigger) is True
