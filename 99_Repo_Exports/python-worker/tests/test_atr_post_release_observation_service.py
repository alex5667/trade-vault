from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from services.atr_post_release_observation_service import ATRPostReleaseObservationService


@patch('services.atr_post_release_observation_service.get_db_connection')
def test_open_post_release_observation(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    obs_id = ATRPostReleaseObservationService.open_post_release_observation(
        change_id="chg_001",
        change_class="HIGH_GOVERNANCE",
        target_scope="BTCUSDT"
    )

    assert obs_id is not None
    assert obs_id.startswith("obs_")
    assert mock_cur.execute.called

@patch('services.atr_post_release_observation_service.get_db_connection')
def test_evaluate_post_release_checks(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {"status": "OBSERVING"}

    mock_telemetry = {
        "execution": {"slippage_shift": True},
        "signal_gates": {"negative_ev_spike": False}
    }

    ATRPostReleaseObservationService.evaluate_post_release_checks("obs_123", mock_telemetry)
    # 4 checks are submitted (Signal/Gate, Execution, Protective, Control Plane)
    assert mock_cur.execute.call_count >= 5 # 1 fetchone + 4 inserts

@patch('services.atr_post_release_observation_service.get_db_connection')
def test_open_promotion_hold(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {"change_id": "c1", "change_class": "MEDIUM_POLICY", "target_scope": "S", "status": "OBSERVING"}

    hold_id = ATRPostReleaseObservationService.open_promotion_hold(
        observation_id="obs_123",
        scope_value="BTCUSDT",
        hold_reason_code="slippage_shift",
        severity="critical"
    )

    assert hold_id is not None
    assert mock_cur.execute.called

@patch('services.atr_post_release_observation_service.get_db_connection')
def test_decide_promotion_status_dwell_not_met(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {
        "status": "OBSERVING",
        "observation_until": datetime.now(UTC) + timedelta(hours=1)
    }
    mock_cur.fetchall.side_effect = [[], []] # no holds, no failed checks

    status = ATRPostReleaseObservationService.decide_promotion_status("obs_123")
    assert status == "KEEP_OBSERVING"

@patch('services.atr_post_release_observation_service.get_db_connection')
def test_decide_promotion_status_eligible(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {
        "change_class": "LOW_RISK_CONFIG",
        "change_id": "id1",
        "target_scope": "scope1",
        "status": "OBSERVING",
        "observation_until": datetime.now(UTC) - timedelta(hours=1)
    }
    mock_cur.fetchall.side_effect = [[], []] # no holds, no failed checks

    status = ATRPostReleaseObservationService.decide_promotion_status("obs_123")
    assert status == "PROMOTION_ELIGIBLE"

@patch('services.atr_post_release_observation_service.get_db_connection')
def test_decide_promotion_status_hold(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {
        "change_class": "MEDIUM_POLICY",
        "status": "OBSERVING",
        "observation_until": datetime.now(UTC) - timedelta(hours=1)
    }
    mock_cur.fetchall.side_effect = [
        [{"severity": "critical", "hold_reason_code": "execution_slippage"}], # holds
    ]

    status = ATRPostReleaseObservationService.decide_promotion_status("obs_123")
    assert status == "PROMOTION_HOLD"

@patch('services.atr_post_release_observation_service.get_db_connection')
def test_decide_promotion_status_rollback_required(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {
        "change_class": "PROTECTIVE_PATH_TOUCHING",
        "status": "OBSERVING",
        "observation_until": datetime.now(UTC) - timedelta(hours=1)
    }
    mock_cur.fetchall.side_effect = [
        [{"severity": "critical", "hold_reason_code": "protective_critical_drift"}], # holds
    ]

    status = ATRPostReleaseObservationService.decide_promotion_status("obs_123")
    assert status == "ROLLBACK_REVIEW_REQUIRED"
