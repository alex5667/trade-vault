from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from services.atr_program_closure_service import ATRProgramClosureService


@pytest.fixture
def service():
    return ATRProgramClosureService()

@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur

def test_check_stabilization_window_passed(service, mock_conn):
    conn, cur = mock_conn

    # 1. Mock Go-Live sign-off date
    signed_at = datetime.now(UTC) - timedelta(days=20)
    cur.fetchone.side_effect = [
        {'signed_at': signed_at}, # golive fetch
    ]

    # 2. Mock 3 GREEN scorecards after sign-off
    cur.fetchall.return_value = [
        {'week_start': (signed_at + timedelta(days=1)).date(), 'overall_status': 'GO'},
        {'week_start': (signed_at + timedelta(days=8)).date(), 'overall_status': 'GO'},
        {'week_start': (signed_at + timedelta(days=15)).date(), 'overall_status': 'GO'},
    ]

    result = service.check_stabilization_window(conn, required_days=14)
    assert result["passed"] is True
    assert result["days_stable"] == 21
    assert result["reason"] == "WINDOW_PASSED"

def test_check_stabilization_window_broken(service, mock_conn):
    conn, cur = mock_conn

    signed_at = datetime.now(UTC) - timedelta(days=20)
    cur.fetchone.return_value = {'signed_at': signed_at}

    # Second week is RED
    cur.fetchall.return_value = [
        {'week_start': (signed_at + timedelta(days=1)).date(), 'overall_status': 'GO'},
        {'week_start': (signed_at + timedelta(days=8)).date(), 'overall_status': 'HOLD'},
    ]

    result = service.check_stabilization_window(conn, required_days=14)
    assert result["passed"] is False
    assert result["reason"].startswith("STREAK_BROKEN_AT")
    assert result["days_stable"] == 7

def test_check_stabilization_window_insufficient(service, mock_conn):
    conn, cur = mock_conn

    signed_at = datetime.now(UTC) - timedelta(days=5)
    cur.fetchone.return_value = {'signed_at': signed_at}

    # Only 1 GREEN week so far
    cur.fetchall.return_value = [
        {'week_start': (signed_at + timedelta(days=1)).date(), 'overall_status': 'GO'},
    ]

    result = service.check_stabilization_window(conn, required_days=14)
    assert result["passed"] is False
    assert result["days_stable"] == 7
    assert result["reason"] == "STABILIZATION_IN_PROGRESS"

@patch("services.atr_program_closure_service.get_conn")
def test_auto_triage_closure_evidence(mock_get_conn, service, mock_conn):
    conn, cur = mock_conn
    mock_get_conn.return_value.__enter__.return_value = conn

    # Mocking all the queries in auto_triage_closure_evidence
    cur.fetchone.side_effect = [
        {'count': 1}, # charter_active
        {'count': 10}, # enforcement_map_active (registry count)
        {'count': 0}, # critical_coverage_gaps
        {'verdict': 'GO_LIVE'}, # golive_signed
        {'count': 0}, # critical_quarantine_active
        {'status': 'certified'}, # e2e_acceptance_passed
        {'signed_at': datetime.now(UTC) - timedelta(days=20)} # golive for check_stabilization_window
    ]

    # Mock streak queries (inside check_stabilization_window)
    cur.fetchall.return_value = [
        {'week_start': '2026-04-01', 'overall_status': 'GO'},
        {'week_start': '2026-04-08', 'overall_status': 'GO'},
    ]

    evidence = service.auto_triage_closure_evidence(conn)

    assert evidence["charter_active"] is True
    assert evidence["enforcement_map_active"] is True
    assert evidence["critical_coverage_gaps"] == 0
    assert evidence["go_live_signed"] is True
    assert evidence["critical_quarantine_active"] is False
    assert evidence["e2e_acceptance_passed"] is True
    assert evidence["stabilization_passed"] is True
