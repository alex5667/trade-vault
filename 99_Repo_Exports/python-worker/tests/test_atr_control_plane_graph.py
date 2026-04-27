import pytest
import os
import json
from unittest.mock import patch, MagicMock

from services.atr_control_plane_graph_service import ControlPlaneGraphService
from services.atr_effective_state_resolver import EffectiveStateResolver
from services.atr_control_plane_projection_service import ControlPlaneProjectionService

@pytest.fixture
def mock_db():
    with patch("services.atr_control_plane_graph_service.get_conn") as mock_get_conn, \
         patch("services.atr_effective_state_resolver.get_conn") as mock_resolver_conn, \
         patch("services.atr_control_plane_projection_service.get_conn") as mock_proj_conn:
        
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_resolver_conn.return_value.__enter__.return_value = mock_conn
        mock_proj_conn.return_value.__enter__.return_value = mock_conn
        
        yield mock_conn

@pytest.fixture
def mock_redis():
    with patch("services.atr_control_plane_projection_service._redis") as mock_redis_func:
        mock_r = MagicMock()
        mock_redis_func.return_value = mock_r
        yield mock_r

def test_create_node(mock_db):
    mock_cursor = mock_db.cursor.return_value.__enter__.return_value
    
    res = ControlPlaneGraphService.create_node(
        node_id="rollout:BTCUSDT:v1",
        node_type="RolloutState",
        scope_kind="global",
        scope_value="BTCUSDT",
        initial_state={"rollout_stage": "canary_25"},
        actor="ops",
        reason_code="MANUAL"
    )
    
    assert res is True
    # Verify execute was called twice: once for event, once for node
    assert mock_cursor.execute.call_count == 2

def test_effective_state_resolver(mock_db):
    mock_cursor = mock_db.cursor.return_value.__enter__.return_value
    
    # Mocking rows for different SELECT queries
    mock_cursor.fetchone.side_effect = [
        {"node_state_json": {"rollout_stage": "live"}},      # 1. RolloutState
        None,                                                # 2. FreezeState (none)
        {"node_state_json": {"status": "active", "expires_at_ms": 9999999999999}}, # 3. OverrideState
        {"c": 0}                                            # 4. Blockers
    ]
    
    state = EffectiveStateResolver.resolve_scope("global", "BTCUSDT")
    
    assert state["rollout_stage"] == "live"
    assert state["freeze_state"] == "none"
    assert state["override_state"] == "active"
    assert state["effective_runtime_state"] == "active"

def test_projection_service(mock_db, mock_redis):
    mock_cursor = mock_db.cursor.return_value.__enter__.return_value
    mock_cursor.fetchone.return_value = {
        "node_id": "rollout:TEST:1",
        "node_type": "RolloutState",
        "scope_value": "TEST",
        "node_state_json": {"rollout_stage": "canary"},
        "version": 1
    }
    
    os.environ["ATR_CONTROL_PLANE_PROJECTION_ENFORCE"] = "1"
    
    res = ControlPlaneProjectionService.project_node("rollout:TEST:1")
    
    assert res is True
    pipeline = mock_redis.pipeline.return_value
    pipeline.set.assert_any_call("cfg:atr_rollout_stage:TEST", "canary")
    pipeline.execute.assert_called_once()
