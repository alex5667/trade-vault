from orderflow_services.route_incident_rca_mirror_incident_bundle_builder_v3_11 import normalize_trigger


def test_normalize_transition_trigger():
    event = {"transition_type": "AUDIT_TO_MIRROR"}
    event_id = "12345-0"
    trigger = normalize_trigger(event, event_id)
    assert trigger["trigger_source"] == "journal"
    assert trigger["trigger_type"] == "AUDIT_TO_MIRROR"
    assert trigger["severity"] == "info"
    assert trigger["raw"] == event


def test_normalize_transition_rollback():
    event = {"transition_type": "MIRROR_TO_AUDIT"}
    event_id = "12345-1"
    trigger = normalize_trigger(event, event_id)
    assert trigger["trigger_source"] == "journal"
    assert trigger["trigger_type"] == "MIRROR_TO_AUDIT"
    assert trigger["severity"] == "warning"
    assert trigger["raw"] == event


def test_normalize_escalation_trigger():
    event = {"severity": "critical", "summary_json": "{}"}
    event_id = "555-0"
    trigger = normalize_trigger(event, event_id)
    assert trigger["trigger_source"] == "escalation"
    assert trigger["trigger_type"] == "ESCALATION_CRITICAL"
    assert trigger["severity"] == "critical"
    assert trigger["raw"] == event


def test_normalize_empty_trigger():
    event = {"other_field": "value"}
    trigger = normalize_trigger(event, "0-0")
    assert not trigger
