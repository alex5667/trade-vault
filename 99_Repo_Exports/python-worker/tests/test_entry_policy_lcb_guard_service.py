import asyncio
import json
import math
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from core.entry_policy_freeze import EntryPolicyFreezeV1
from services.entry_policy_lcb_guard_service import (
    EntryPolicyLcbGuardService,
    LcbConfig,
    _safe_float,
    _safe_int,
)
from utils.time_utils import get_ny_time_millis

@pytest.fixture
def mock_redis():
    mock = AsyncMock()
    # default responses for hgetall
    mock.hgetall.return_value = {}
    mock.get.return_value = None
    mock.hget.return_value = 0
    return mock

@pytest.fixture
def service(mock_redis):
    with patch("services.entry_policy_lcb_guard_service.aioredis.from_url", return_value=mock_redis):
        with patch.dict("os.environ", {
            "LCB_GUARD_IN_STREAM": "test-stream",
            "LCB_GUARD_GROUP": "test-group",
            "LCB_GUARD_CONSUMER": "test-c1",
            "LCB_GUARD_MIN_SAMPLES": "2",
            "LCB_GUARD_Z": "1.0",
            "LCB_GUARD_THRESHOLD": "0.0",
            "LCB_GUARD_STREAK": "2",
            "LCB_GUARD_MIN_FREEZE_MS": "0"
        }):
            svc = EntryPolicyLcbGuardService()
            return svc

def test_safe_helpers():
    assert _safe_float(None) == 0.0
    assert _safe_float("1.23") == 1.23
    assert _safe_float("NaN") == 0.0
    assert _safe_float("inf") == 0.0
    assert _safe_float({}) == 0.0

    assert _safe_int(None) == 0
    assert _safe_int("42") == 42
    assert _safe_int("invalid") == 0
    assert _safe_int([]) == 0

@pytest.mark.asyncio
async def test_update_stats_welford(service):
    # Test welford's algorithm accuracy
    service.r.hgetall.return_value = {}
    n, mean, std = await service._update_stats("test_key", 10.0)
    assert n == 1
    assert mean == 10.0
    assert std == 0.0

    service.r.hgetall.return_value = {"n": "1", "mean": "10.0", "m2": "0.0"}
    n, mean, std = await service._update_stats("test_key", 20.0)
    assert n == 2
    assert mean == 15.0
    # m2 = 0 + (20-10)*(20-15) = 50
    # var = 50 / 2 = 25
    # std = 5
    assert math.isclose(std, 5.0)

@pytest.mark.asyncio
async def test_process_event_ignored(service):
    await service._process_event("NOT_CLOSED", {})
    service.r.hgetall.assert_not_called()

    # not arm A
    await service._process_event("POSITION_CLOSED", {"ab_arm": "B", "scenario": "reversal"})
    service.r.hgetall.assert_not_called()

    # not reversal/continuation
    await service._process_event("POSITION_CLOSED", {"ab_arm": "A", "scenario": "invalid"})
    service.r.hgetall.assert_not_called()

@pytest.mark.asyncio
async def test_process_event_no_freeze_resets_streak(service):
    # Setup stats return
    service.r.hgetall.return_value = {"n": "1", "mean": "1.0", "m2": "0.0"}
    service.r.get.return_value = None # No freeze

    payload = {
        "symbol": "BTCUSDT",
        "ab_arm": "A",
        "scenario": "reversal",
        "r_mult": "1.5"
    }
    await service._process_event("POSITION_CLOSED", payload)
    
    # Check that streak is reset
    service.r.hset.assert_any_call("lcb:stats:v1:BTCUSDT:na:default:reversal:A:0", "streak", 0)

@pytest.mark.asyncio
async def test_process_event_hard_freeze_ignores(service):
    # Mode = hard
    freeze = EntryPolicyFreezeV1(symbol="BTCUSDT", mode="hard", until_ts_ms=get_ny_time_millis() + 100000)
    service.r.get.return_value = freeze.to_json()

    payload = {
        "symbol": "BTCUSDT",
        "ab_arm": "A",
        "scenario": "reversal"
    }
    await service._process_event("POSITION_CLOSED", payload)
    # Shouldn't reset streak or unfreeze
    calls = [c.args[1] for c in service.r.hset.mock_calls if len(c.args) > 1]
    assert "streak" not in calls

@pytest.mark.asyncio
async def test_process_event_shadow_freeze_unfreezes(service):
    # min samples = 2, required streak = 2
    # First good trade
    freeze = EntryPolicyFreezeV1(symbol="BTCUSDT", scenario="reversal", mode="shadow", until_ts_ms=get_ny_time_millis() + 100000)
    service.r.get.return_value = freeze.to_json()

    service.r.hgetall.return_value = {"n": "1", "mean": "5.0", "m2": "0.0"}
    service.r.hget.return_value = "0" # current streak = 0

    payload = {
        "symbol": "BTCUSDT",
        "ab_arm": "A",
        "scenario": "reversal",
        "r_mult": "5.0" # Keeps mean at 5, std at 0
    }
    await service._process_event("POSITION_CLOSED", payload)

    # Streak becomes 1
    service.r.hset.assert_any_call("lcb:stats:v1:BTCUSDT:na:default:reversal:A:0", "streak", 1)

    # Second good trade
    service.r.hgetall.return_value = {"n": "2", "mean": "5.0", "m2": "0.0"}
    service.r.hget.return_value = "1" # current streak = 1

    await service._process_event("POSITION_CLOSED", payload)

    # Streak becomes 2, triggering unfreeze
    service.r.delete.assert_called_with("cfg:entry_policy:freeze:v1:BTCUSDT:default:reversal")
    # Audit event should be emitted
    service.r.xadd.assert_called_once()
