import asyncio
import json
import os
import sys

import redis.asyncio as aioredis

META_PREFIX = "cfg:suggestions:entry_policy:meta"
APPROVALS_PREFIX = "cfg:suggestions:entry_policy:approvals"
APPLIED_PREFIX = "cfg:suggestions:entry_policy:applied"

REQUIRED = int(os.getenv("SUGGESTION_APPROVALS_REQUIRED", "2"))

async def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: apply_cooldown_policy_suggestion.py <sid>")
        raise SystemExit(2)
    sid = sys.argv[1].strip()
    r = aioredis.from_url(os.getenv("REDIS_URL","redis://redis-worker-1:6379/0"), decode_responses=True)

    meta_raw = await r.get(f"{META_PREFIX}:{sid}")
    if not meta_raw:
        print("Error: meta not found")
        await r.close()
        raise SystemExit(1)
    meta = json.loads(meta_raw)

    # approvals
    appr = await r.smembers(f"{APPROVALS_PREFIX}:{sid}")
    if len(appr or []) < REQUIRED:
        print(f"Error: need {REQUIRED} approvals, have {len(appr or [])}")
        await r.close()
        raise SystemExit(1)

    # already applied?
    if await r.exists(f"{APPLIED_PREFIX}:{sid}"):
        print("Already applied")
        await r.close()
        return

    override_key = meta["apply"]["override_key"]
    proposed = meta["proposed"]

    cur_raw = await r.get(override_key)
    cur = json.loads(cur_raw) if cur_raw else {}
    if not isinstance(cur, dict):
        cur = {}
    # merge
    for k, v in proposed.items():
        cur[k] = v
    await r.set(override_key, json.dumps(cur, ensure_ascii=False, separators=(",", ":")), ex=7*24*3600)
    await r.set(f"{APPLIED_PREFIX}:{sid}", json.dumps({"ts_ms": int(meta.get("ts_ms",0) or 0), "approvers": list(appr)}, separators=(",", ":")), ex=30*24*3600)
    await r.close()
    print("OK")

if __name__ == "__main__":
    asyncio.run(main())
