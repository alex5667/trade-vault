from unittest.mock import MagicMock, patch

from services.atr_release_window_service import (
    build_pre_release_checklist,
    classify_change,
    evaluate_release_blockers,
    find_eligible_window,
    get_required_signoffs,
)


def test_classify_change():
    assert classify_change("update", ["execution", "mt5"]) == "CRITICAL_EXECUTION_TOUCHING"
    assert classify_change("update", ["policy", "trailing", "closeout"]) == "PROTECTIVE_PATH_TOUCHING"
    assert classify_change("update", ["gating", "allow"]) == "CRITICAL_RUNTIME_GATING"
    assert classify_change("update", ["graph", "freeze"]) == "HIGH_GOVERNANCE"
    assert classify_change("update", ["policy", "something"]) == "MEDIUM_POLICY"
    assert classify_change("update", ["policy"]) == "MEDIUM_POLICY"
    assert classify_change("update", ["metrics", "boards"]) == "LOW_RISK_OBSERVABILITY"
    assert classify_change("update", ["configs"]) == "LOW_RISK_CONFIG"

def test_find_eligible_window():
    assert find_eligible_window("LOW_RISK_CONFIG") == "standard"
    assert find_eligible_window("HIGH_GOVERNANCE") == "governance"
    assert find_eligible_window("CRITICAL_RUNTIME_GATING") == "runtime_critical"
    assert find_eligible_window("CRITICAL_EXECUTION_TOUCHING") == "execution_critical"
    assert find_eligible_window("PROTECTIVE_PATH_TOUCHING") == "protective_isolated"

def test_get_required_signoffs():
    assert get_required_signoffs("LOW_RISK_CONFIG") == ["owner"]
    assert get_required_signoffs("HIGH_GOVERNANCE") == ["owner", "control_plane_owner", "oncall"]
    assert get_required_signoffs("CRITICAL_EXECUTION_TOUCHING") == ["owner", "execution_owner", "oncall"]

@patch("services.atr_release_window_service.get_db_connection")
def test_build_pre_release_checklist(mock_get_db):
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn

    res = build_pre_release_checklist("test_chg", "HIGH_GOVERNANCE", "test_scope")
    assert res["status"] == "ready"
    assert res["change_id"] == "test_chg"
    assert res["change_class"] == "HIGH_GOVERNANCE"

@patch("services.atr_model_config_drift_service.get_db_connection")
@patch("services.atr_disaster_recovery_service.ATRDisasterRecoveryService.is_release_blocked_by_dr")
@patch("services.atr_release_quarantine_service.ATRReleaseQuarantineService.is_release_blocked_by_quarantine")
@patch("services.atr_replay_certification_service.ATRReplayCertificationService.get_cert_status_for_change")
@patch("services.atr_replay_certification_service.ATRReplayCertificationService.select_required_datasets")
@patch("services.atr_release_window_service.get_db_connection")
def test_evaluate_release_blockers_clean(mock_get_db, mock_select_ds, mock_cert, mock_quar, mock_dr, mock_drift_db):
    mock_cert.return_value = "passed"
    mock_quar.return_value = False
    mock_dr.return_value = False
    mock_select_ds.return_value = []
    _setup_drift_db_mock(mock_drift_db)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    checks = {
        "control_plane": {"open_critical_drifts": 0},
        "protective": {"open_protective_critical_drift": 0},
        "rollback_ready": {"rollback_bundle_prepared": True}
    }

    mock_cur.fetchone.return_value = {
        "checks_json": checks,
        "change_class": "CRITICAL_RUNTIME_GATING",
        "change_id": "test_chg",
        "target_scope": "test_scope",
    }

    blockers = evaluate_release_blockers("test_chk")
    assert len(blockers) == 0


@patch("services.atr_model_config_drift_service.get_db_connection")
@patch("services.atr_disaster_recovery_service.ATRDisasterRecoveryService.is_release_blocked_by_dr")
@patch("services.atr_release_quarantine_service.ATRReleaseQuarantineService.is_release_blocked_by_quarantine")
@patch("services.atr_replay_certification_service.ATRReplayCertificationService.get_cert_status_for_change")
@patch("services.atr_replay_certification_service.ATRReplayCertificationService.select_required_datasets")
@patch("services.atr_release_window_service.get_db_connection")
def test_evaluate_release_blockers_dirty(mock_get_db, mock_select_ds, mock_cert, mock_quar, mock_dr, mock_drift_db):
    mock_cert.return_value = "passed"
    mock_quar.return_value = False
    mock_dr.return_value = False
    mock_select_ds.return_value = []
    _setup_drift_db_mock(mock_drift_db)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    checks = {
        "control_plane": {"open_critical_drifts": 1},  # Blocker
        "protective": {"open_protective_critical_drift": 0},
        "rollback_ready": {"rollback_bundle_prepared": False}  # Blocker
    }

    mock_cur.fetchone.return_value = {
        "checks_json": checks,
        "change_class": "CRITICAL_RUNTIME_GATING",
        "change_id": "test_chg",
        "target_scope": "test_scope",
    }

    blockers = evaluate_release_blockers("test_chk")
    assert len(blockers) == 2
    assert "control plane critical drift open" in blockers
    assert "rollback bundle not prepared for critical change" in blockers


def _setup_drift_db_mock(mock_drift_db):
    """Configure ATRModelConfigDriftService DB mock to return empty results."""
    mock_drift_conn = MagicMock()
    mock_drift_cur = MagicMock()
    mock_drift_db.return_value.__enter__.return_value = mock_drift_conn
    mock_drift_conn.cursor.return_value.__enter__.return_value = mock_drift_cur
    mock_drift_cur.fetchall.return_value = []
    mock_drift_cur.fetchone.return_value = None
