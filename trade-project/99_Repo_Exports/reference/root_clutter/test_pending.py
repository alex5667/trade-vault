import asyncio
import redis.asyncio as aioredis

async def main():
    r = aioredis.from_url("redis://redis-worker-1:6379/0", decode_responses=False)
    try:
        pending = await r.xpending_range(
            "events:decision_snapshot",
            "decision_snapshot_writer",
            min="-",
            max="+",
            count=1,
            idle=0
        )
        print("PENDING TYPE:", type(pending))
        if pending:
            print("PENDING[0] TYPE:", type(pending[0]))
            print("PENDING[0]:", pending[0])
    except Exception as e:
        print("ERROR:", e)

asyncio.run(main())
