
import pytest

from orderflow_services.operator_routing_incident_rca_routing_controller_v2_12 import (
    DEFAULT_MODEL,
    DEFAULT_PROVIDER,
    determine_route,
)


class MockRedis:
    async def hget(self, name, key):
        return b"PROMOTE"

@pytest.mark.asyncio
async def test_determine_route():
    mock_r = MockRedis()
    row = {"route_change_id": "test_rc_1"}

    decision = await determine_route(mock_r, row)

    assert decision["route_change_id"] == "test_rc_1"
    assert decision["task_type"] == "routing_incident_root_cause_analysis"
    assert decision["provider"] == DEFAULT_PROVIDER
    assert decision["model_name"] == DEFAULT_MODEL
