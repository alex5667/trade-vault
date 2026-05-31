
import asyncio
import logging
import os

# Adjust path to find modules
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.append(os.getcwd())

from services.orderflow.policy.circuit_breaker_state_v1 import CircuitBreakerState


async def test_hysteresis():
    print("--- Testing Hysteresis Logic ---")

    # Mock Redis
    state_store = {}

    class MockPipeline:
        def hset(self, key, mapping):
            state_store.update(mapping)
            return self

        def hdel(self, key, *fields):
            for f in fields:
                if f in state_store:
                    del state_store[f]
            return self

        async def execute(self):
            return

    redis = AsyncMock()

    async def mock_hgetall(key):
        return state_store

    async def mock_hset(key, mapping):
        state_store.update(mapping)
        return len(mapping)

    async def mock_hdel(key, *fields):
        for f in fields:
            if f in state_store:
                del state_store[f]
        return len(fields)

    redis.hgetall.side_effect = mock_hgetall
    redis.hset.side_effect = mock_hset
    redis.hdel.side_effect = mock_hdel
    redis.pipeline = MagicMock(return_value=MockPipeline())

    # Init state with strict hysteresis
    # Dwell 2s, Consecutive 3
    cb = CircuitBreakerState(redis, "TEST", min_dwell_s=2, min_consecutive=3)

    # 1. Initial State (OK)
    ts = 10000
    mode, info = await cb.update("ok", ts)
    print(f"T={ts} In=ok -> Out={mode} (Info={info})")
    assert mode == "ok"

    # 2. Trigger Warn (Consecutive 1)
    ts += 100
    mode, info = await cb.update("warn", ts)
    print(f"T={ts} In=warn -> Out={mode} (Info={info})")
    assert mode == "ok" # Should stay OK
    assert state_store.get("pending_mode") == "warn"
    assert state_store.get("pending_count") == 1

    # 3. Trigger Warn (Consecutive 2)
    ts += 100
    mode, info = await cb.update("warn", ts)
    print(f"T={ts} In=warn -> Out={mode} (Info={info})")
    assert mode == "ok"
    assert state_store.get("pending_count") == 2

    # 4. Trigger Warn (Consecutive 3) -> SWITCH!
    ts += 100
    # But wait, dwell time?
    # Current mode OK, changed_at = 0? (default).
    # elapsed = 1300 - 0 = 1300. min_dwell = 2000.
    # Should FAIL DWELL check if default changed_at wasn't far past.
    # Ah, empty state means changed_at=0.
    # If using epoch ms, 1300 is essentially 1970.
    # Wait, code uses `int(changed_at)`.
    # If changed_at is 0, dwell check passes (1300 > 0).
    # UNLESS we treat 0 as "now"? No, 0 is far past.

    # Let's verify dwell behavior.
    # We pretend current state was established at T=0.
    # So we should be able to switch.

    mode, info = await cb.update("warn", ts)
    print(f"T={ts} In=warn -> Out={mode} (Info={info})")
    assert mode == "warn"
    assert "switched" in info
    # Verify state updated
    assert state_store.get("mode") == "warn"
    assert state_store.get("changed_at") == ts
    assert "pending_mode" not in state_store

    # 5. Try to switch back fast (Dwell violation)
    ts += 500 # only 0.5s passed (T=10800)
    mode, info = await cb.update("ok", ts)
    print(f"T={ts} In=ok -> Out={mode} (Info={info})")
    assert mode == "warn" # Should NOT switch
    assert info.get("reason") == "counting"

    ts += 100
    mode, info = await cb.update("ok", ts)
    assert info.get("reason") == "counting"

    ts += 100
    mode, info = await cb.update("ok", ts)
    # Now count=3, but dwell=700ms < 2000ms
    print(f"T={ts} In=ok -> Out={mode} (Info={info})")
    assert info.get("reason") == "dwell"
    assert mode == "warn"

    # 6. Wait for dwell (2s)
    ts += 2000
    mode, info = await cb.update("ok", ts)
    print(f"T={ts} In=ok -> Out={mode} (Info={info})")
    assert mode == "ok" # Switch immediately because we counted during dwell!
    assert info.get("switched") is True

    # Verify cleaned up
    assert "pending_mode" not in state_store


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_hysteresis())
