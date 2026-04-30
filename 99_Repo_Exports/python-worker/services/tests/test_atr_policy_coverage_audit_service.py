#!/usr/bin/env python3
import pytest
from unittest.mock import MagicMock, patch
from services.atr_policy_coverage_audit_service import ATRPolicyCoverageAuditService

@pytest.fixture
def mock_db():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur

@pytest.fixture
def service():
    return ATRPolicyCoverageAuditService()

def test_missing_rule(service, mock_db):
    conn, cur = mock_db
    
    surface = {
        "surface_id": "hard_dq_gate"
        "domain": "runtime"
        "surface_json": {}
        "owner": "test"
    }
    
    # Empty policies
    policies = []
    enforcements = []
    
    results = service.evaluate_surface_coverage(surface, policies, enforcements)
    # Check RULE_COVERAGE dimension
    rule_cov = next((r for r in results if r["dimension"] == "RULE_COVERAGE"), None)
    assert rule_cov is not None
    assert rule_cov["status"] == "missing"
    assert rule_cov["reason_code"] == "NO_RULE"

def test_rule_exists_but_no_enforcement(service, mock_db):
    conn, cur = mock_db
    surface = {
        "surface_id": "quarantine_gate"
        "domain": "governance"
        "surface_json": {}
        "owner": "owner_x"
    }
    
    policies = [{"policy_json": {"target": "quarantine_gate"}}]
    enforcements = []
    
    results = service.evaluate_surface_coverage(surface, policies, enforcements)
    rule_cov = next((r for r in results if r["dimension"] == "RULE_COVERAGE"), None)
    enf_cov = next((r for r in results if r["dimension"] == "ENFORCEMENT_COVERAGE"), None)
    
    assert rule_cov["status"] == "covered"
    assert rule_cov["reason_code"] == "OK"
    
    assert enf_cov["status"] == "missing"
    assert enf_cov["reason_code"] == "NO_ENFORCEMENT"
    # For quarantine_gate, NO_ENFORCEMENT is critical
    assert enf_cov["severity"] == "critical"

def test_no_cert_for_protective_invariant(service, mock_db):
    conn, cur = mock_db
    surface = {
        "surface_id": "sl_ratchet_invariant"
        "domain": "protective"
        "surface_json": {}
        "owner": "owner_y"
    }
    
    policies = [{"policy_json": {"target": "sl_ratchet_invariant"}}]
    # Has enforcement, but no cert
    enforcements = [{"map_json": {"target": "sl_ratchet_invariant"}, "default_action": "WARN"}]
    
    results = service.evaluate_surface_coverage(surface, policies, enforcements)
    cert_cov = next((r for r in results if r["dimension"] == "CERT_COVERAGE"), None)
    action_cov = next((r for r in results if r["dimension"] == "ROLLBACK_OR_FREEZE_COVERAGE"), None)
    
    assert cert_cov["status"] == "missing"
    assert cert_cov["reason_code"] == "NO_CERT"
    assert cert_cov["severity"] == "critical" # sl_ratchet_invariant with NO_CERT must be critical
    
    assert action_cov["status"] == "missing"
    assert action_cov["reason_code"] == "NO_ACTION_PATH"
    assert action_cov["severity"] == "critical"

def test_severity_mapping(service, mock_db):
    # missing order-path action => critical
    assert service._determine_severity("order_queue_dispatch", "NO_ACTION_PATH") == "critical"
    
    # missing alert on graph_consistency => critical
    assert service._determine_severity("graph_consistency", "NO_ALERT") == "critical"
    
    # generic missing alert => warn
    assert service._determine_severity("generic_surface", "NO_ALERT") == "warn"

@patch('services.atr_policy_coverage_audit_service.get_conn')
def test_audit_outcome(mock_get_conn, service, mock_db):
    conn, cur = mock_db
    mock_get_conn.return_value.__enter__.return_value = conn
    
    # Seed inventory
    service.load_surface_inventory = MagicMock(return_value=[
        {"surface_id": "hard_dq_gate", "domain": "runtime", "surface_json": {}, "owner": "test"}
        {"surface_id": "order_queue_dispatch", "domain": "runtime", "surface_json": {}, "owner": "test"}
    ])
    
    service.load_policy_registry = MagicMock(return_value=[])
    service.load_enforcement_map = MagicMock(return_value=[])
    
    # This will result in critical gaps (order_queue_dispatch has NO_RULE -> critical)
    audit_res = service.compute_coverage_audit("system", "all")
    
    assert audit_res["status"] == "failed"
    assert audit_res["summary"]["critical_gaps"] > 0
    # Expected gap was opened
    assert cur.execute.call_count > 0 

def test_open_gap_closure_item(service, mock_db):
    conn, cur = mock_db
    cur.fetchone.return_value = None # No existing gap
    
    service.open_gap_closure_item(conn, "sl_ratchet_invariant", "NO_CERT", "critical", "protective_owner")
    
    # Verify insert query was called
    last_call = cur.execute.call_args[0]
    query = last_call[0]
    args = last_call[1]
    
    assert "INSERT INTO atr_policy_gap_closure_matrix" in query
    assert args[1] == "sl_ratchet_invariant"
    assert args[2] == "NO_CERT"
    assert args[3] == "critical"
    assert args[4] == "protective_owner"
    assert args[5] == "open"
