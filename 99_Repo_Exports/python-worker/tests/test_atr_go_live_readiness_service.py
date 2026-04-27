import pytest
from unittest.mock import MagicMock
import json

from services.atr_go_live_readiness_service import ATRGoLiveReadinessService, REQUIRED_DOMAINS, REQUIRED_ROLES

@pytest.fixture
def mock_db_conn():
    conn = MagicMock()
    cursor = MagicMock()
    
    # Configure cursor for simple standard execute/fetchall
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = []
    
    return conn

@pytest.fixture
def service():
    return ATRGoLiveReadinessService(enable=True, enforce=True)

# --- Unit Tests: Verdict Aggregation & Domain Logic ---
def test_all_pass_verdict_go_live(service, mock_db_conn):
    # Setup mock to return green domains and approved roles
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    # First query fetches checks, Second fetches signoffs
    cursor.fetchall.side_effect = [
        [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS],
        [{"signer_role": r, "status": "approved"} for r in REQUIRED_ROLES]
    ]
    
    verdict = service.compute_final_go_live_verdict(mock_db_conn, "test_pkg_1")
    assert verdict == "GO_LIVE"

def test_missing_critical_owner_status_hold(service, mock_db_conn):
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    # Make protective_owner missing (neither approved nor rejected), but others approved
    roles_present = [r for r in REQUIRED_ROLES if r != "protective_owner"]
    
    cursor.fetchall.side_effect = [
        [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS],
        [{"signer_role": r, "status": "approved"} for r in roles_present]
    ]
    
    verdict = service.compute_final_go_live_verdict(mock_db_conn, "test_pkg_2")
    assert verdict == "HOLD"

def test_reject_by_protective_owner_no_go(service, mock_db_conn):
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    signoffs = [{"signer_role": r, "status": "approved"} for r in REQUIRED_ROLES]
    
    # Protective owner rejects
    for s in signoffs:
        if s["signer_role"] == "protective_owner":
            s["status"] = "rejected"
            
    cursor.fetchall.side_effect = [
        [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS],
        signoffs
    ]
    
    verdict = service.compute_final_go_live_verdict(mock_db_conn, "test_pkg_3")
    assert verdict in ["NO_GO", "ROLLBACK_ONLY"]

def test_protective_drift_and_reject_rollback_only(service, mock_db_conn):
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    # One critical fail (protective drift conceptually)
    checks = [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS]
    for c in checks:
        if c["domain"] == "protective_lifecycle":
            c["status"] = "failed"
            c["severity"] = "critical"
            
    signoffs = [{"signer_role": r, "status": "approved"} for r in REQUIRED_ROLES]
    for s in signoffs:
        if s["signer_role"] == "protective_owner":
            s["status"] = "rejected"
            
    cursor.fetchall.side_effect = [checks, signoffs]
    
    verdict = service.compute_final_go_live_verdict(mock_db_conn, "test_pkg_4")
    assert verdict == "ROLLBACK_ONLY"

def test_bounded_issue_go_live_with_constraints(service, mock_db_conn):
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    checks = [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS]
    for c in checks:
        if c["domain"] == "execution":
            c["status"] = "warning"
            c["severity"] = "warn"
            
    signoffs = [{"signer_role": r, "status": "approved"} for r in REQUIRED_ROLES]
    
    cursor.fetchall.side_effect = [checks, signoffs]
    
    constraints = {"symbols_allowed": ["BTCUSDT", "ETHUSDT"]}
    verdict = service.compute_final_go_live_verdict(mock_db_conn, "test_pkg_5", constraints_block=constraints)
    assert verdict == "GO_LIVE_WITH_CONSTRAINTS"

def test_go_live_with_constraints_requires_constraints_block(service, mock_db_conn):
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    checks = [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS]
    for c in checks:
        if c["domain"] == "execution":
            c["status"] = "warning"
            c["severity"] = "warn"
            
    signoffs = [{"signer_role": r, "status": "approved"} for r in REQUIRED_ROLES]
    
    cursor.fetchall.side_effect = [checks, signoffs]
    
    # Without constraints block, warnings lead to HOLD
    verdict = service.compute_final_go_live_verdict(mock_db_conn, "test_pkg_6", constraints_block=None)
    assert verdict == "HOLD"

# --- Domain Evaluation Integration (Logic Mock) ---
def test_failed_protective_domain_no_go_live(service, mock_db_conn):
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    pkg_id = service.build_go_live_package(mock_db_conn, "global", "1.0.0")
    
    # Inject protective drift
    evidence = {
        "execution_yellow": False,
        "dr_restore_fail": False,
        "charter_compliance_fail": False,
        "protective_drift": True
    }
    
    # This evaluates and inserts to DB, we simulate the state that it would produce
    service.evaluate_readiness_domains(mock_db_conn, pkg_id, evidence)
    
    # Intercept fetchall to read from simulated state
    checks = [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS]
    for c in checks:
        if c["domain"] == "protective_lifecycle":
            c["status"] = "failed"
            c["severity"] = "critical"
            
    signoffs = [{"signer_role": r, "status": "approved"} for r in REQUIRED_ROLES]
            
    cursor.fetchall.side_effect = [checks, signoffs]
    
    verdict = service.compute_final_go_live_verdict(mock_db_conn, pkg_id)
    assert verdict == "NO_GO"

def test_seed_healthy_package_workflow(service, mock_db_conn):
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    
    pkg_id = service.build_go_live_package(mock_db_conn, "global", "1.0.0")
    evidence = service.collect_required_evidence(mock_db_conn)
    service.evaluate_readiness_domains(mock_db_conn, pkg_id, evidence)
    
    roles = {r: {"status": "approved", "signer": f"{r}_user"} for r in REQUIRED_ROLES}
    service.request_signoffs(mock_db_conn, pkg_id, roles)
    
    checks = [{"domain": d, "status": "passed", "severity": "info"} for d in REQUIRED_DOMAINS]
    signoffs = [{"signer_role": r, "status": "approved"} for r in REQUIRED_ROLES]
    cursor.fetchall.side_effect = [checks, signoffs]
    
    verdict = service.compute_final_go_live_verdict(mock_db_conn, pkg_id)
    assert verdict == "GO_LIVE"
