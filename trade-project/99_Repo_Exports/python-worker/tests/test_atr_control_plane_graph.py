import os
from unittest.mock import MagicMock, patch

import pytest

from services.atr_control_plane_graph_service import ControlPlaneGraphService
from services.atr_control_plane_projection_service import ControlPlaneProjectionService
from services.atr_effective_state_resolver import EffectiveStateResolver


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
    """resolve_legacy: scope_frozen freeze with no override → effective = scope_frozen.

    resolve_scope() without is_shadow_graph_mode=True calls resolve_legacy(), which
    queries atr_policy_rollouts / atr_active_freezes / atr_override_requests /
    atr_release_decisions (legacy schema).  The mock must match those column names.
    """
    mock_cursor = mock_db.cursor.return_value.__enter__.return_value

    mock_cursor.fetchone.side_effect = [
        {"rollout_stage": "live"},      # Q1: atr_policy_rollouts → rollout_stage
        {"freeze_state": "scope_frozen"},  # Q2: atr_active_freezes → freeze_state
        None,                              # Q3: atr_override_requests (no active override)
        None,                              # Q4: atr_release_decisions (no block)
    ]

    state = EffectiveStateResolver.resolve_scope("global", "BTCUSDT")

    # _build_output nests all state fields under state["states"]
    states = state["states"]
    assert states["rollout_stage"] == "live"
    assert states["freeze_state"] == "scope_frozen"
    assert states["override_state"] == "none"
    # scope_frozen is in PRECEDENCE_MAP → effective_runtime_state = scope_frozen
    assert states["effective_runtime_state"] == "scope_frozen"

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
