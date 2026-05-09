import asyncio
import os
import sys

import redis.asyncio as aioredis

APPROVALS_PREFIX = "cfg:suggestions:entry_policy:approvals"

async def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: approve_cooldown_policy_suggestion.py <sid> <approver>")
        raise SystemExit(2)
    sid = sys.argv[1].strip()
    approver = sys.argv[2].strip()
    r = aioredis.from_url(os.getenv("REDIS_URL","redis://redis-worker-1:6379/0"), decode_responses=True)
    key = f"{APPROVALS_PREFIX}:{sid}"
    await r.sadd(key, approver)
    await r.expire(key, 7*24*3600)
    await r.close()
    print("OK")

if __name__ == "__main__":
    asyncio.run(main())
