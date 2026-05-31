from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()


def _sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def compute_suggestion_id(sugg: dict[str, Any]) -> str:
    proposed = sugg.get("proposed", {}) or {}
    # stable canonical form
    items = sorted((str(k), str(v)) for k, v in proposed.items())
    base = json.dumps(items, separators=(",", ":"), ensure_ascii=False)
    return _sha1(base)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", default=os.getenv("EP_SUGGESTIONS_REDIS_KEY", "cfg:suggestions:entry_policy:latest"))
    ap.add_argument("--id", default="latest", help="suggestion id (or 'latest' to compute from key content)")
    ap.add_argument("--min-approvals", type=int, default=int(os.getenv("EP_MIN_APPROVALS", "2")))
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)

    raw = await r.get(args.key)
    if not raw:
        print("no suggestions in redis key")
        return 2
    sugg = json.loads(raw)
    sid = args.id
    if sid == "latest":
        sid = compute_suggestion_id(sugg)

    approver = os.getenv("APPROVER_ID", "").strip()
    if not approver:
        # safe fallback: hostname/user is still "identity", but explicit is preferred
        approver = os.getenv("USER", "unknown")

    approvals_key = f"cfg:suggestions:entry_policy:approvals:{sid}"
    meta_key = f"cfg:suggestions:entry_policy:meta:{sid}"
    ttl_sec = int(os.getenv("EP_SUGGESTIONS_TTL_SEC", "604800"))

    await r.sadd(approvals_key, approver)
    await r.expire(approvals_key, ttl_sec)
    # write meta once (best-effort)
    with contextlib.suppress(Exception):
        await r.set(meta_key, json.dumps({"ts_ms": _now_ms(), "sid": sid, "key": args.key}, separators=(",", ":")), ex=ttl_sec)

    n = await r.scard(approvals_key)
    print(f"approved sid={sid} by={approver} approvals={n}/{args.min_approvals}")
    return 0


if __name__ == "__main__":
    raise SystemExit(__import__("asyncio").run(main()))
