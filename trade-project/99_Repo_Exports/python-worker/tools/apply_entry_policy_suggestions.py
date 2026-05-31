from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def compute_suggestion_id(sugg: dict[str, Any]) -> str:
    proposed = sugg.get("proposed", {}) or {}
    items = sorted((str(k), str(v)) for k, v in proposed.items())
    base = json.dumps(items, separators=(",", ":"), ensure_ascii=False)
    return _sha1(base)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", default=os.getenv("EP_SUGGESTIONS_REDIS_KEY", "cfg:suggestions:entry_policy:latest"))
    ap.add_argument("--id", default="latest")
    ap.add_argument("--min-approvals", type=int, default=int(os.getenv("EP_MIN_APPROVALS", "2")))
    ap.add_argument("--shadow-sec", type=int, default=int(os.getenv("EP_ROLLOUT_SHADOW_SEC", "1800")))
    ap.add_argument("--overrides-key", default=os.getenv("CFG_ENTRY_POLICY_OVERRIDES_KEY", "cfg:entry_policy:overrides"))
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

    approvals_key = f"cfg:suggestions:entry_policy:approvals:{sid}"
    applied_key = f"cfg:suggestions:entry_policy:applied:{sid}"
    rollout_key = f"cfg:entry_policy:rollout:{sid}"

    if await r.exists(applied_key):
        print(f"already applied sid={sid}")
        return 2

    approvers = list(await r.smembers(approvals_key))
    approvers = sorted([a for a in approvers if a])
    if len(set(approvers)) < int(args.min_approvals):
        print(f"not enough approvals sid={sid} approvals={approvers}")
        return 3

    proposed = sugg.get("proposed", {}) or {}
    if not isinstance(proposed, dict) or not proposed:
        print("no proposed changes")
        return 2

    # Read current overrides for rollback
    prev_raw = await r.get(args.overrides_key)
    prev = prev_raw if isinstance(prev_raw, str) else ""

    # Build new overrides
    # Always start rollout in SHADOW=1 (safety). Promotion is separate tool.
    overrides = {str(k): str(v) for k, v in proposed.items()}
    overrides["ENTRY_POLICY_SHADOW"] = "1"

    # version bump: use timestamp
    ver = _now_ms()
    new_doc = {"version": ver, "updated_ts_ms": _now_ms(), "overrides": overrides}
    await r.set(args.overrides_key, json.dumps(new_doc, ensure_ascii=False, separators=(",", ":")))

    # Mark rollout state with rollback payload
    rollout = {
        "sid": sid,
        "applied_ts_ms": _now_ms(),
        "shadow_until_ts_ms": _now_ms() + int(args.shadow_sec) * 1000,
        "approvers": approvers,
        "overrides_key": args.overrides_key,
        "prev_overrides_raw": prev,
        "new_overrides": new_doc,
        "state": "shadow",
    }
    await r.set(rollout_key, json.dumps(rollout, ensure_ascii=False, separators=(",", ":")), ex=int(os.getenv("EP_SUGGESTIONS_TTL_SEC", "604800")))

    # Mark applied (immutable record)
    actor = os.getenv("APPLY_ACTOR", "").strip() or os.getenv("USER", "unknown")
    await r.set(applied_key, json.dumps({"sid": sid, "ts_ms": _now_ms(), "actor": actor, "approvers": approvers}, separators=(",", ":")), ex=int(os.getenv("EP_SUGGESTIONS_TTL_SEC", "604800")))

    print(f"applied sid={sid} shadow_sec={args.shadow_sec} approvers={approvers} overrides_key={args.overrides_key}")
    print("next: run promote_entry_policy_rollout.py after shadow window (or via timer).")
    return 0


if __name__ == "__main__":
    raise SystemExit(__import__("asyncio").run(main()))
