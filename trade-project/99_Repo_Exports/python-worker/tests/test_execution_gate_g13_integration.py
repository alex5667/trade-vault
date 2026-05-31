import asyncio
import json
import os
import time
import pytest
import redis.asyncio as aioredis
from utils.time_utils import get_ny_time_millis
from services.execution_gate_service import ExecutionGateService
from core.redis_keys import RedisStreams as RS

@pytest.mark.asyncio
async def test_execution_gate_integration():
    redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    
    # Cleanup streams before test
    await redis.delete(RS.CRYPTO_RAW)
    await redis.delete(RS.OF_CONFIRM)
    await redis.delete(RS.ORDERS_QUEUE_BINANCE)

    os.environ["EXEC_GATE_REQUIRE_OF_CONFIRM"] = "true"
    os.environ["EXEC_GATE_MATCH_MS"] = "5000"
    
    svc = ExecutionGateService()
    svc.redis_url = "redis://localhost:6379/0"
    
    # Start service in background
    task = asyncio.create_task(svc.start())
    await asyncio.sleep(1) # give it time to start
    
    try:
        now_ms = get_ny_time_millis()
        
        # 1. Publish Confirmation
        confirm_payload = {
            "symbol": "SOLUSDT",
            "direction": "long",
            "ok": "1", # string to test our bugfix
            "score": 0.99,
            "ts_ms": now_ms,
            "reason": "integration_test"
        }
        await redis.xadd(RS.OF_CONFIRM, {"payload": json.dumps(confirm_payload)})
        
        # Wait a bit
        await asyncio.sleep(0.5)
        
        # 2. Publish Proposal with dummy qty to test bugfix
        prop_payload = {
            "symbol": "SOLUSDT",
            "direction": "long",
            "is_virtual": "0", # string zero
            "generated_at": now_ms,
            "sid": "test-integ-001",
            "entry": 150.0,
            "sl": 140.0,
            "tp_levels": [160.0, 170.0],
            "qty": 0.0,
            "lot": 2.5
        }
        await redis.xadd(RS.CRYPTO_RAW, {"payload": json.dumps(prop_payload)})
        
        # Wait for processing
        await asyncio.sleep(1)
        
        # 3. Check output queue
        list_len = await redis.llen(RS.ORDERS_QUEUE_BINANCE)
        assert list_len == 1, f"Expected 1 order, got {list_len}"
        
        item = await redis.lpop(RS.ORDERS_QUEUE_BINANCE)
        assert item is not None
        
        order = json.loads(item)
        assert order["symbol"] == "SOLUSDT"
        assert order["qty"] == 2.5 # lot was copied to qty
        assert order["gate_verified"] is True
        assert order["validation_status"] == "passed"
        
        print("Integration test passed successfully!")
    finally:
        svc.running = False
        task.cancel()
        await redis.aclose()

if __name__ == "__main__":
    asyncio.run(test_execution_gate_integration())
