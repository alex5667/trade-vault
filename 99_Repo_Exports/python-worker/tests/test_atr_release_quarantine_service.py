import pytest
from datetime import datetime, timedelta, timezone
import json

from services.atr_release_quarantine_service import ATRReleaseQuarantineService, QUARANTINE_STATES

import services.atr_release_quarantine_service as q_service
from unittest.mock import patch, MagicMock

@patch('services.atr_release_quarantine_service.get_db_connection')
def test_open_quarantine_protective_drift(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    qid = ATRReleaseQuarantineService.open_quarantine(
        incident_id="inc_001",
        quarantine_class="PROTECTIVE_PATH_QUARANTINE",
        scope_kind="symbol",
        scope_value="BTCUSDT",
        severity="critical",
        reason_code="protective_invariant_violation"
    )

    assert qid is not None
    assert qid.startswith("q_")
    assert mock_cur.execute.called
    
@patch('services.atr_release_quarantine_service.get_db_connection')
def test_open_quarantine_execution_venue(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    qid = ATRReleaseQuarantineService.open_quarantine(
        incident_id="inc_002",
        quarantine_class="EXECUTION_VENUE_QUARANTINE",
        scope_kind="venue",
        scope_value="mt5",
        severity="critical",
        reason_code="venue_degraded"
    )

    assert qid is not None
    assert mock_cur.execute.called

@patch('services.atr_release_quarantine_service.get_db_connection')
@patch('services.atr_release_quarantine_service.ATRReleaseQuarantineService.advance_quarantine_state')
def test_evaluate_quarantine_exit_dwell_satisfied(mock_advance, mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    # 1st call for main q info, 2nd call for checks
    mock_cur.fetchone.side_effect = [
        {
            "status": "QUARANTINED",
            "not_before_release_at": datetime.now(timezone.utc) - timedelta(hours=1)
        },
        None # no failed checks
    ]
    
    result = ATRReleaseQuarantineService.evaluate_quarantine_exit("q_123")
    
    assert result is True
    mock_advance.assert_called_with("q_123", "READY_FOR_REVIEW")

@patch('services.atr_release_quarantine_service.get_db_connection')
@patch('services.atr_release_quarantine_service.ATRReleaseQuarantineService.advance_quarantine_state')
def test_evaluate_quarantine_exit_dwell_not_satisfied(mock_advance, mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    mock_cur.fetchone.side_effect = [
        {
            "status": "QUARANTINED",
            "not_before_release_at": datetime.now(timezone.utc) + timedelta(hours=1)
        },
        None 
    ]
    
    result = ATRReleaseQuarantineService.evaluate_quarantine_exit("q_123")
    
    assert result is False
    assert not mock_advance.called

@patch('services.atr_release_quarantine_service.get_db_connection')
def test_grant_waiver_denied_protective(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    mock_cur.fetchone.return_value = {"quarantine_class": "PROTECTIVE_PATH_QUARANTINE", "severity": "critical"}
    
    result = ATRReleaseQuarantineService.grant_quarantine_waiver("q_123", "admin", "urgent", 3600)
    assert result is False

@patch('services.atr_release_quarantine_service.get_db_connection')
def test_grant_waiver_denied_execution_critical(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    mock_cur.fetchone.return_value = {"quarantine_class": "EXECUTION_VENUE_QUARANTINE", "severity": "critical"}
    
    result = ATRReleaseQuarantineService.grant_quarantine_waiver("q_123", "admin", "urgent", 3600)
    assert result is False

@patch('services.atr_release_quarantine_service.get_db_connection')
@patch('services.atr_release_quarantine_service.ATRReleaseQuarantineService.advance_quarantine_state')
def test_grant_waiver_allowed_control_plane(mock_advance, mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    mock_cur.fetchone.return_value = {"quarantine_class": "CONTROL_PLANE_QUARANTINE", "severity": "warn"}
    
    result = ATRReleaseQuarantineService.grant_quarantine_waiver("q_123", "admin", "observability_update", 3600)
    assert result is True
    mock_advance.assert_called_with("q_123", "WAIVED")

@patch('services.atr_release_quarantine_service.get_db_connection')
def test_release_blocked_by_active_quarantine(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    mock_cur.fetchall.return_value = [
        {"quarantine_class": "SIGNAL_GATE_QUARANTINE", "scope_value": "BTCUSDT", "status": "QUARANTINED"}
    ]
    
    result = ATRReleaseQuarantineService.is_release_blocked_by_quarantine("BTCUSDT | breakout | v17")
    
    assert result is not None
    assert result['quarantine_class'] == "SIGNAL_GATE_QUARANTINE"

@patch('services.atr_release_quarantine_service.get_db_connection')
def test_release_not_blocked_if_no_quarantine(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    mock_cur.fetchall.return_value = []
    
    result = ATRReleaseQuarantineService.is_release_blocked_by_quarantine("ETHUSDT | breakout | v17")
    
    assert result is None
