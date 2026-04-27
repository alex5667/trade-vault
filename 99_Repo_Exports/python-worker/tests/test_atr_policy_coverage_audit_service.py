import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import json

from services.atr_policy_coverage_audit_service import ATRPolicyCoverageAuditService, DIMENSIONS


@pytest.fixture
def mock_db_conn():
    conn = MagicMock()
    conn._is_test_mock = True
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    
    # Mock some expected returns for the tests
    # but for simple unit tests we can just return empty lists or pre-defined dicts usually
    cursor.fetchall.return_value = []
    return conn


@pytest.fixture
def service(mock_db_conn):
    return ATRPolicyCoverageAuditService(db_conn=mock_db_conn)


def test_missing_rule_mapping(service):
    # Test missing rule => NO_RULE internally mapped
    surface_id = "test_surface"
    # Provide no evaluation data -> should default to missing -> NO_XXX
    results = service.evaluate_surface_coverage(
        {"surface_id": surface_id, "owner": "test_owner"}, 
        {}
    )
    
    # Check RULE_COVERAGE dimension mapping
    rule_result = next((r for r in results if r["dimension"] == "RULE_COVERAGE"), None)
    assert rule_result is not None
    assert rule_result["status"] == "missing"
    assert rule_result["severity"] == "critical"  # general rule for NO_RULE
    assert "AUTO_NO_RULE" in rule_result["reason_code"]


def test_rule_exists_no_target_layer(service):
    # Test rule exists but no target layer => NO_ENFORCEMENT
    surface_id = "hard_dq_gate"
    results = service.evaluate_surface_coverage(
        {"surface_id": surface_id, "owner": "test_owner"}, 
        {
            "RULE_COVERAGE": {"status": "covered"},
            "ENFORCEMENT_COVERAGE": {"status": "missing", "gap_type": "NO_ENFORCEMENT"}
        }
    )
    
    enf_result = next((r for r in results if r["dimension"] == "ENFORCEMENT_COVERAGE"), None)
    assert enf_result["status"] == "missing"
    assert enf_result["severity"] == "critical"


def test_severity_mapping_order_queue_dispatch(service):
    # missing order-path action => critical
    surface_id = "order_queue_dispatch"
    results = service.evaluate_surface_coverage(
        {"surface_id": surface_id, "owner": "test_owner"}, 
        {
            "ROLLBACK_OR_FREEZE_COVERAGE": {"status": "missing", "gap_type": "NO_ACTION_PATH"}
        }
    )
    
    act_result = next((r for r in results if r["dimension"] == "ROLLBACK_OR_FREEZE_COVERAGE"), None)
    assert act_result["severity"] == "critical"


def test_severity_mapping_telegram_alert(service):
    # missing Telegram alert only => warn
    surface_id = "random_surface"
    results = service.evaluate_surface_coverage(
        {"surface_id": surface_id, "owner": "test_owner"}, 
        {
            "ALERT_COVERAGE": {"status": "missing", "gap_type": "NO_ALERT"}
        }
    )
    
    alert_result = next((r for r in results if r["dimension"] == "ALERT_COVERAGE"), None)
    assert alert_result["severity"] == "warn"


def test_audit_outcome_failed(service):
    # any critical gap => failed
    
    # Mock build_gap_matrix to return critical
    with patch.object(service, 'load_surface_inventory', return_value=[{"surface_id": "quarantine_gate", "owner": "owner"}]):
        with patch.object(service, 'build_gap_matrix', return_value=[{"severity": "critical"}]):
            res = service.compute_coverage_audit("domain", "protective", {
                "quarantine_gate": {
                    "RULE_COVERAGE": {"status": "missing", "gap_type": "NO_RULE"}
                }
            })
            assert res["overall_status"] == "failed"


def test_audit_outcome_warning(service):
    # only warnings => passed/warning
    with patch.object(service, 'load_surface_inventory', return_value=[{"surface_id": "test_surf", "owner": "owner"}]):
        with patch.object(service, 'build_gap_matrix', return_value=[{"severity": "warn"}]):
            # Pass all other dimensions as "covered" so we don't accidentally create critical gaps
            eval_data = {dim: {"status": "covered"} for dim in DIMENSIONS}
            eval_data["ALERT_COVERAGE"] = {"status": "missing", "gap_type": "NO_ALERT"} # mapped to warn
            res = service.compute_coverage_audit("domain", "governance", {"test_surf": eval_data})
            # It should have warning status overall because it triggers error_count=0, warn_count=1
            assert res["overall_status"] == "warning"


def test_forbidden_waiver_rejected(service, mock_db_conn):
    # forbidden waiver rejected
    # Setup the mock db to return a critical gap on a forbidden surface
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        {"surface_id": "sl_ratchet_invariant", "gap_type": "NO_CERT", "severity": "critical", "remediation_status": "open"}
    ]
    
    res = service.waive_gap_closure_item("row123", "Too hard to fix")
    assert res is False


def test_normal_waiver_accepted(service, mock_db_conn):
    # normal waiver accepted
    cursor = mock_db_conn.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        {"surface_id": "minor_feature_gate", "gap_type": "NO_ALERT", "severity": "warn", "remediation_status": "open"}
    ]
    
    res = service.waive_gap_closure_item("row123", "Waiving for now")
    assert res is True
