
import asyncio
import os
import sys
import logging
from datetime import datetime

# Adjust path to include python-worker
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

try:
    from services.orderflow.policy.circuit_breaker_v1 import decide_circuit_breaker, CircuitBreakerDecision
    from services.orderflow.policy.circuit_breaker_state_v1 import CircuitBreakerState
    import redis.asyncio as aioredis
except ImportError as e:
    print(f"ImportError: {e}")
    sys.exit(1)

async def main():
    print("--- Debugging Circuit Breaker Types ---")
    
    cfg = {"cb_enable": True}
    dq_state = "ok"
    drift_state = "ok"
    
    # 1. Check decide_circuit_breaker return
    print("\n1. Calling decide_circuit_breaker...")
    decision = decide_circuit_breaker(cfg=cfg, dq_state=dq_state, drift_state=drift_state)
    print(f"Result type: {type(decision)}")
    print(f"Result: {decision}")
    
    if hasattr(decision, "regime"):
        print(f"decision.regime type: {type(decision.regime)}")
        print(f"decision.regime value: {decision.regime}")
    else:
        print("decision has no 'regime' attribute!")

    # 2. Check CircuitBreakerState.update (mock redis)
    print("\n2. Testing CircuitBreakerState.update with string...")
    
    class MockRedis:
        async def hgetall(self, key):
            return {}
        def pipeline(self):
            return self
        async def execute(self):
            return []
        async def hset(self, key, mapping=None):
            print(f"MockRedis.hset called with mapping: {mapping}")
            # Simulate Redis strict typing check
            if mapping:
                for k, v in mapping.items():
                    if isinstance(v, CircuitBreakerDecision):
                         raise TypeError("Invalid input of type: 'CircuitBreakerDecision'")
            return 1
        def hdel(self, *args):
            return self
        async def delete(self, *args):
            return 1
    
    redis = MockRedis()
    state = CircuitBreakerState(redis, "TEST_SYM")
    
    try:
        res = await state.update(decision.regime, 1234567890)
        print(f"Update(str) success: {res}")
    except Exception as e:
        print(f"Update(str) failed: {e}")
        
    print("\n3. Testing CircuitBreakerState.update with ATTRIBUTE ACCESS FAIL (Simulated)...")
    try:
        # Simulate what happens if we pass the object itself
        print("Calling update with Decision OBJECT instead of string...")
        res = await state.update(decision, 1234567890)
        print(f"Update(obj) success: {res}")
    except Exception as e:
        print(f"Update(obj) failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
