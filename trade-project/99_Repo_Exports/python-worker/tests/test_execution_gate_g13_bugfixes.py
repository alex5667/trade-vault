import json
import pytest
from unittest.mock import AsyncMock

from services.execution_gate_service import ExecutionGateService
from core.redis_keys import RedisStreams as RS
from utils.time_utils import get_ny_time_millis

def _build_service(require_of_confirm=False, enforce_virtual=False):
    svc = ExecutionGateService()
    svc.require_of_confirm = require_of_confirm
    svc.enforce_virtual = enforce_virtual
    svc.match_tolerance_ms = 5000
    svc.proposal_ttl_s = 5.0
    svc.redis = AsyncMock()
    svc.redis.rpush = AsyncMock(return_value=1)
    return svc

@pytest.mark.asyncio
async def test_bugfix_is_virtual_string_zero_parsed_as_false():
    """
    Test that 'is_virtual': '0' is correctly parsed as False.
    Previously bool('0' or 0) evaluated to True, causing real orders to be marked virtual.
    """
    svc = _build_service(require_of_confirm=False)
    payload = {
        "symbol": "BTCUSDT",
        "direction": "long",
        "is_virtual": "0",  # String zero
        "qty": 1.5,
        "sl": 100,
        "tp_levels": [200],
        "generated_at": get_ny_time_millis()
    }
    
    await svc._handle_proposal({"payload": json.dumps(payload)})
    
    # If parsed as real, it should be pushed to the binance queue
    svc.redis.rpush.assert_called_once()
    
    # Let's check the published payload
    published = json.loads(svc.redis.rpush.call_args[0][1])
    assert published["is_virtual"] == "0", "Original value is preserved"
    # Virtual orders are NOT published if they follow the TradeMonitor path
    # The fact that rpush was called proves it was treated as REAL.

@pytest.mark.asyncio
async def test_bugfix_ok_string_one_parsed_as_true():
    """
    Test that 'ok': '1' is correctly parsed as int 1.
    Previously data.get('ok') == 1 failed if it was a string.
    """
    svc = _build_service(require_of_confirm=True)
    now_ms = get_ny_time_millis()
    
    prop_payload = {
        "symbol": "ETHUSDT",
        "direction": "long",
        "qty": 2.0,
        "sl": 3000,
        "tp_levels": [3500],
        "generated_at": now_ms
    }
    await svc._handle_proposal({"payload": json.dumps(prop_payload)})
    
    confirm_payload = {
        "symbol": "ETHUSDT",
        "direction": "long",
        "ok": "1", # String one
        "ts_ms": now_ms,
        "score": 0.99
    }
    await svc._handle_confirmation({"payload": json.dumps(confirm_payload)})
    
    # Must be published because ok="1" is parsed as 1
    svc.redis.rpush.assert_called_once()
    
    published = json.loads(svc.redis.rpush.call_args[0][1])
    assert published["validation_status"] == "passed"
    assert published["gate_verified"] is True

@pytest.mark.asyncio
async def test_bugfix_qty_zero_dropped():
    """
    Test that qty=0.0 is dropped and not sent to executor.
    Previously has_qty allowed qty=0.0
    """
    svc = _build_service(require_of_confirm=False)
    
    payload = {
        "symbol": "SOLUSDT",
        "direction": "short",
        "qty": 0.0, # Dummy qty
        "sl": 100,
        "tp_levels": [200],
        "generated_at": get_ny_time_millis()
    }
    
    await svc._handle_proposal({"payload": json.dumps(payload)})
    
    # Should be dropped, NOT pushed
    svc.redis.rpush.assert_not_called()

@pytest.mark.asyncio
async def test_bugfix_lot_overrides_dummy_qty():
    """
    Test that if qty=0.0 but lot=2.5, it uses the lot and passes.
    """
    svc = _build_service(require_of_confirm=False)
    
    payload = {
        "symbol": "SOLUSDT",
        "direction": "short",
        "qty": 0.0, # Dummy qty
        "lot": 2.5, # Valid lot
        "sl": 100,
        "tp_levels": [200],
        "generated_at": get_ny_time_millis()
    }
    
    await svc._handle_proposal({"payload": json.dumps(payload)})
    
    # Should be pushed because lot overrides dummy qty
    svc.redis.rpush.assert_called_once()
    published = json.loads(svc.redis.rpush.call_args[0][1])
    assert published["qty"] == 2.5
