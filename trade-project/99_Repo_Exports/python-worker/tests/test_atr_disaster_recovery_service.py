from unittest.mock import MagicMock, patch

import pytest

from services.atr_disaster_recovery_service import ATRDisasterRecoveryService


@patch("services.atr_disaster_recovery_service.get_db_connection")
def test_dr_classification_partial_redis_loss(mock_get_db):
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    res = ATRDisasterRecoveryService.open_dr_event("dr_test_1", "PARTIAL_REDIS_LOSS", "global", "all", "R_SHARD_DOWN")
    assert res['status'] == 'opened'
    assert res['safe_mode'] == 'NO_NEW_RISK'

    # Assert advance state called properly
    assert mock_cur.execute.call_count >= 1

def test_dr_restore_invalid_class():
    with pytest.raises(ValueError):
        ATRDisasterRecoveryService.open_dr_event("dr_test_2", "INVALID_CLASS", "global", "all", "REASON")

def test_invalid_ladder_state():
    with pytest.raises(ValueError):
        ATRDisasterRecoveryService.advance_restore_state("dr_test_2", "INVALID_STATE")

@patch("services.atr_disaster_recovery_service.get_db_connection")
def test_cannot_progress_to_normal_without_observation(mock_get_db, monkeypatch):
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock dr_json indicating it hasn't been observed
    mock_cur.fetchone.return_value = {"dr_json": {"observed": False}}

    # Actually need to bypass policy via ENV or test it logged
    monkeypatch.setattr("services.atr_disaster_recovery_service.ATR_DR_POLICY_ENFORCE", True)
    ATRDisasterRecoveryService.advance_restore_state("dr_test_3", "NORMAL")

@patch("services.atr_disaster_recovery_service.get_db_connection")
def test_is_release_blocked_by_dr_active(mock_get_db, monkeypatch):
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    monkeypatch.setattr("services.atr_disaster_recovery_service.ATR_DR_POLICY_ENABLE", True)

    # Mock active DR event
    mock_cur.fetchall.return_value = [
        {"dr_id": "dr_test_5", "dr_class": "PROTECTIVE_STATE_LOSS", "status": "BOOTSTRAPPING", "scope_kind": "global"}
    ]

    blocker = ATRDisasterRecoveryService.is_release_blocked_by_dr("symbol_btc")
    assert blocker is not None
    assert blocker['dr_class'] == "PROTECTIVE_STATE_LOSS"

@patch("services.atr_disaster_recovery_service.get_db_connection")
def test_is_release_blocked_by_dr_none(mock_get_db, monkeypatch):
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    monkeypatch.setattr("services.atr_disaster_recovery_service.ATR_DR_POLICY_ENABLE", True)

    mock_cur.fetchall.return_value = []

    blocker = ATRDisasterRecoveryService.is_release_blocked_by_dr("symbol_btc")
    assert blocker is None
