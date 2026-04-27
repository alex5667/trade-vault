import pytest
import json
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_bundle_rca_bridge_v3_28 import route_decision, prepare_vertex_payload, prepare_local_payload

def test_route_decision_auto_vertex_healthy():
    decision, reason = route_decision("AUTO", vertex_healthy=True, req_degraded=True, bundle_bytes=1000, severity="critical")
    assert decision == "ROUTE_VERTEX"
    assert reason == "vertex_healthy"

def test_route_decision_auto_vertex_degraded():
    decision, reason = route_decision("AUTO", vertex_healthy=False, req_degraded=True, bundle_bytes=1000, severity="warning")
    assert decision == "ROUTE_LOCAL"
    assert reason == "vertex_degraded"
    
def test_route_decision_too_large():
    decision, reason = route_decision("AUTO", vertex_healthy=True, req_degraded=True, bundle_bytes=99999999, severity="warning")
    assert decision == "REJECT"
    assert reason == "bundle_too_large"

def test_route_decision_low_severity():
    decision, reason = route_decision("AUTO", vertex_healthy=True, req_degraded=True, bundle_bytes=1000, severity="info")
    assert decision == "REJECT"
    assert reason == "severity_too_low"

def test_vertex_payload_shape():
    p = prepare_vertex_payload("app-123", "{}")
    assert "apply_id" in p
    assert p["task_family"] == "route_incident_rca_mirror_rca_winner_apply_apply_rca"

def test_local_payload_shape():
    p = prepare_local_payload("app-123", "{}")
    assert "ticket_id" in p
    assert "vw_app_rca_app-123" in p["ticket_id"]
    assert p["task_type"] == "vertex_unavailable_fallback"
