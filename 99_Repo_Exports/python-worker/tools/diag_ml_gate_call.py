import asyncio

from services.ml_confirm_gate import MLConfirmGate


async def main():
    gate = MLConfirmGate.from_env()
    # Mock redis_async
    class MockRedisAsync:
        async def get(self, key):
            import redis
            r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)
            return r.get(key)

    await gate.refresh_async(MockRedisAsync())

    dec = await gate.check_async(
        symbol="SOLUSDT",
        ts_ms=1777109030000,
        direction="LONG",
        scenario="continuation",
        indicators={"sid": "crypto-of:SOLUSDT:1777109030000"},
        rule_score=0.6,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1
    )
    print("DECISION DICT:", dec.to_dict())

asyncio.run(main())
