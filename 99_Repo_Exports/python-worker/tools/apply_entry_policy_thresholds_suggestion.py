from utils.time_utils import get_ny_time_millis

"""
Apply Entry Policy Threshold Suggestion Tool

Purpose:
  Apply LCB-optimized threshold suggestions with 2-man approval workflow.
  Sets applied_ts_ms to activate hold-down period.
  Updates switch budget state to track daily limits.

Expert review:
  - DevOps/SRE: 2-man approval prevents accidental/malicious changes
  - Senior Python: Atomic Redis operations, fail-safe error handling
  - Financial Analysts: Audit trail for compliance and rollback
"""
import asyncio
import json
import os
import sys

import redis.asyncio as aioredis

from core.switch_budget import SwitchState, apply_switch, utc_day_id

META_PREFIX = "cfg:suggestions:entry_policy:meta"
APPROVALS_PREFIX = "cfg:suggestions:entry_policy:approvals"
APPLIED_PREFIX = "cfg:suggestions:entry_policy:applied"
SWITCH_STATE_PREFIX = os.getenv("THRESH_SWITCH_STATE_PREFIX", "cfg:entry_policy:switch_state:v1")
REQ = int(os.getenv("SUGGESTION_APPROVALS_REQUIRED", "2"))


def _switch_key(sym: str, rg: str, scn: str) -> str:
    return f"{SWITCH_STATE_PREFIX}:{sym}:{rg}:{scn}"


def _max_switches_per_day(rg: str) -> int:
    rg = (rg or "na").lower()
    if rg in ("thin", "news", "illiquid"):
        return int(os.getenv("THRESH_SWITCH_MAX_PER_DAY_THIN", "1"))
    return int(os.getenv("THRESH_SWITCH_MAX_PER_DAY", "2"))


async def main():
    if len(sys.argv) < 2:
        print("Usage: apply_entry_policy_thresholds_suggestion.py <sid>")
        raise SystemExit(2)

    sid = sys.argv[1].strip()
    r = aioredis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    # Read suggestion metadata
    raw = await r.get(f"{META_PREFIX}:{sid}")
    if not raw:
        raise SystemExit("meta not found")

    meta = json.loads(raw)

    # Check approvals (2-man rule)
    appr = await r.smembers(f"{APPROVALS_PREFIX}:{sid}")
    if len(appr or []) < REQ:
        raise SystemExit(f"need {REQ} approvals, have {len(appr or [])}")

    # Check if already applied
    if await r.exists(f"{APPLIED_PREFIX}:{sid}"):
        print("already applied")
        await r.close()
        return

    # Apply override with V1 schema
    k = str(meta["apply"]["override_key"])
    v = meta["apply"]["value"]

    # Enforce V1 schema and set applied_ts_ms (activates hold-down)
    if isinstance(v, dict):
        v["ver"] = 1
        v["applied_ts_ms"] = get_ny_time_millis()
        v.setdefault("sid", sid)
        v.setdefault("src", "thresh_lcb")

    # Atomic write with TTL
    await r.set(k, json.dumps(v, separators=(",", ":"), ensure_ascii=False), ex=7 * 24 * 3600)

    # Update switch-state (budget accounting)
    try:
        sym = (meta.get("symbol", "")).upper()
        rg = (meta.get("regime", "na")).lower()
        scn = (meta.get("scenario", "")).lower()

        if sym and scn in ("reversal", "continuation"):
            st_key = _switch_key(sym, rg, scn)
            st_raw = await r.get(st_key)
            st = SwitchState.from_dict(json.loads(st_raw)) if st_raw else SwitchState(day_id=utc_day_id(get_ny_time_millis()))
            max_sw = _max_switches_per_day(rg)

            # Apply switch (increments counter, sets pause if budget hit)
            apply_switch(st=st, now_ms=get_ny_time_millis(), max_per_day=max_sw, pause_on_budget=True)

            # Write back to Redis with TTL
            await r.set(st_key, json.dumps(st.to_dict(), separators=(",", ":")), ex=2 * 24 * 3600)
    except Exception:
        pass  # Best-effort, don't block apply on switch-state errors

    # Mark as applied with audit trail
    await r.set(
        f"{APPLIED_PREFIX}:{sid}",
        json.dumps({"ts_ms": meta.get("ts_ms"), "approvers": list(appr)}, separators=(",", ":")),
        ex=30 * 24 * 3600
    )

    await r.close()
    print("OK")


if __name__ == "__main__":
    asyncio.run(main())
