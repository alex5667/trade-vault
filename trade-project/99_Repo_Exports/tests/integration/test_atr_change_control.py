import pytest
import sys
import os
sys.path.insert(0, os.path.abspath('python-worker'))
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_db():
    import services.atr_change_control_service
    with patch.object(services.atr_change_control_service, "get_conn") as mock_get_conn:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        yield mock_cur

def test_submit_change(mock_db):
    from services.atr_change_control_service import submit_change
    mock_db.rowcount = 1
    
    ok = submit_change(
        change_id="chg_001",
        change_type="policy_rollout",
        scope_kind="symbol",
        title="Test Rollout",
        author="alice",
        owner="bob",
        risk_level="medium",
        reason_code="TEST_CODE",
        request_data={"foo": "bar"}
    )
    
    assert ok is True
    # Should perform INSERT into atr_change_requests
    assert mock_db.execute.call_count == 2 # 1 for request, 1 for transition

def test_attach_replay_passed(mock_db):
    from services.atr_change_control_service import attach_replay_report
    mock_db.fetchone.return_value = ["DRAFT"]
    
    report = {"status": "passed", "summary": {}}
    ok = attach_replay_report("chg_001", report)
    
    assert ok is True
    # Should INSERT artifact, UPDATE status, INSERT transition
    # Check that update sets status to REPLAY_PASSED
    calls = mock_db.execute.call_args_list
    update_call = [args for args, kwargs in calls if "UPDATE atr_change_requests" in args[0]]
    assert len(update_call) == 1
    assert update_call[0][1][0] == "REPLAY_PASSED"

def test_approve_change_blocks_without_replay_for_high_risk(mock_db):
    from services.atr_change_control_service import approve_change
    # mock a high risk change without replay
    mock_db.fetchone.return_value = {
        "status": "DRAFT",
        "risk_level": "high"
    }
    
    ok = approve_change("chg_001", actor="charlie")
    assert ok is False # should be blocked

def test_approve_change_allows_after_replay(mock_db):
    from services.atr_change_control_service import approve_change
    # mock a high risk change that passed replay
    mock_db.fetchone.return_value = {
        "status": "REPLAY_PASSED",
        "risk_level": "high"
    }
    
    ok = approve_change("chg_001", actor="charlie")
    assert ok is True

def test_rollout_blocks_if_not_approved(mock_db):
    from services.atr_change_control_service import start_rollout
    mock_db.fetchone.return_value = ["REPLAY_PASSED"]
    
    ok = start_rollout("chg_001", {"stages": []})
    assert ok is False

def test_rollout_allows_if_approved(mock_db):
    from services.atr_change_control_service import start_rollout
    mock_db.fetchone.return_value = ["APPROVED"]
    
    ok = start_rollout("chg_001", {"stages": []})
    assert ok is True
    
    calls = mock_db.execute.call_args_list
    update_call = [args for args, kwargs in calls if "UPDATE atr_change_requests" in args[0]]
    assert len(update_call) == 1
    assert update_call[0][1][0] == "ROLLED_OUT"

def test_request_rollback(mock_db):
    from services.atr_change_control_service import request_rollback
    mock_db.fetchone.return_value = ["ROLLED_OUT"]
    
    ok = request_rollback("chg_001", {"policy": "freeze_then_rollback"})
    assert ok is True
    
    calls = mock_db.execute.call_args_list
    update_call = [args for args, kwargs in calls if "UPDATE atr_change_requests" in args[0]]
    assert len(update_call) == 1
    assert update_call[0][1][0] == "ROLLBACK_PENDING"
