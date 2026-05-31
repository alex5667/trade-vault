import pytest
import json
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_incident_bundle_builder_v3_27 import determine_trigger, trim_evidence

def test_determine_trigger_apply():
    journal = [{"strategy": "APPLY_SINGLE_ARM", "ts_ms": "2000"}]
    rbs = []
    esc = []
    # patch specific env override internally if need be, but we mock the config by giving expected str
    should_b, trig_type, sev, meta = determine_trigger(journal, rbs, esc, 1000)
    assert should_b
    assert trig_type == "apply"
    assert sev == "warning"

def test_determine_trigger_rollback():
    journal = []
    rbs = [{"ts_ms": "3000"}]
    esc = []
    should_b, trig_type, sev, meta = determine_trigger(journal, rbs, esc, 1000)
    assert should_b
    assert trig_type == "rollback"
    assert sev == "critical"

def test_determine_trigger_escalation():
    journal = []
    rbs = []
    esc = [{"severity": "critical", "ts_ms": "4000"}]
    should_b, trig_type, sev, meta = determine_trigger(journal, rbs, esc, 1000)
    assert should_b
    assert trig_type == "escalation"
    assert sev == "critical"
    
def test_determine_trigger_priority():
    journal = [{"strategy": "APPLY_SINGLE_ARM", "ts_ms": "2000"}]
    rbs = []
    esc = [{"severity": "critical", "ts_ms": "4000"}]
    # It should take the latest active, which is 4000 
    should_b, trig_type, sev, meta = determine_trigger(journal, rbs, esc, 1000)
    assert should_b
    assert trig_type == "escalation"

def test_trim_evidence():
    ev = [{"id": 1}, {"id": 2}, {"id": 3}]
    trimmed = trim_evidence(ev, 2)
    assert len(trimmed) == 2
    assert trimmed[0]["id"] == 1
    assert trimmed[1]["id"] == 2
