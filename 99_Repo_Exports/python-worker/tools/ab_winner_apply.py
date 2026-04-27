from utils.time_utils import get_ny_time_millis
import argparse
import asyncio
import json
import os
import time
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

def _now_ms() -> int:
    return get_ny_time_millis()

def _norm_sym(s: str) -> str:
    return (s or "").strip().upper()

def _norm_rg(s: str) -> str:
    return (s or "na").strip().lower()

def _norm_grp(s: str) -> str:
    return (s or "default").strip().lower()

def _norm_arm(a: str) -> str:
    a = (a or "").strip().upper()
    return a if a in ("A","B","C") else ""

def _active_arm_key(sym: str, rg: str, grp: str) -> str:
    return f"cfg:entry_policy:active_arm:{sym}:{rg}:{grp}"

def _lock_key(sym: str, rg: str, grp: str) -> str:
    return f"cfg:entry_policy:active_arm_lock:{sym}:{rg}:{grp}"

async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sid", default="")
    ap.add_argument("--symbol", default="")
    ap.add_argument("--regime", default="")
    ap.add_argument("--group", default="")
    ap.add_argument("--by", default="manual")   # who applies
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=20)

    meta_prefix = os.getenv("AB_WINNER_META_PREFIX", "cfg:suggestions:entry_policy:meta")
    latest_prefix = os.getenv("AB_WINNER_LATEST_PREFIX", "cfg:suggestions:entry_policy:latest:ab_winner")
    approvals_prefix = os.getenv("AB_WINNER_APPROVALS_PREFIX", "cfg:suggestions:entry_policy:approvals")
    applied_prefix = os.getenv("AB_WINNER_APPLIED_PREFIX", "cfg:suggestions:entry_policy:applied")

    need = int(os.getenv("ENTRY_POLICY_APPROVALS_REQUIRED", "2"))
    lock_sec = int(os.getenv("AB_ACTIVE_ARM_LOCK_SEC", "21600"))  # 6h
    active_ttl = int(os.getenv("AB_ACTIVE_ARM_TTL_SEC", "0"))     # 0 => persistent

    sid = (args.sid or "").strip()
    if not sid:
        sym = _norm_sym(args.symbol)
        rg = _norm_rg(args.regime)
        grp = _norm_grp(args.group)
        if not sym or not rg or not grp:
            print("Need --sid OR (--symbol --regime --group)")
            return 2
        sid = await r.get(f"{latest_prefix}:{sym}:{rg}:{grp}") or ""
        sid = str(sid or "").strip()
        if not sid:
            print("NO_LATEST_SID")
            return 3

    meta_key = f"{meta_prefix}:{sid}"
    raw = await r.get(meta_key)
    if not raw:
        print(f"NO_META {meta_key}")
        return 4

    try:
        meta: Dict[str, Any] = json.loads(raw)
    except Exception:
        print("BAD_META_JSON")
        return 5

    sym = _norm_sym(str(meta.get("symbol") or ""))
    rg = _norm_rg(str(meta.get("regime") or "na"))
    grp = _norm_grp(str(meta.get("group") or "default"))
    winner = _norm_arm(str(meta.get("winner_arm") or ""))
    if not sym or not winner:
        print("META_MISSING_FIELDS")
        return 6

    approvals_key = f"{approvals_prefix}:{sid}"
    applied_key = f"{applied_prefix}:{sid}"
    ak = _active_arm_key(sym, rg, grp)
    lk = _lock_key(sym, rg, grp)

    # already applied?
    if await r.get(applied_key):
        print(f"ALREADY_APPLIED {sid}")
        return 0

    # locked?
    if await r.get(lk):
        print(f"LOCKED {lk}")
        return 7

    # approvals
    try:
        n_appr = int(await r.scard(approvals_key) or 0)
        appr = list(await r.smembers(approvals_key) or [])
    except Exception:
        n_appr = 0
        appr = []
    if n_appr < need:
        print(f"NOT_ENOUGH_APPROVALS have={n_appr} need={need} key={approvals_key}")
        return 8

    print(f"APPLY sid={sid} {sym}/{rg}/{grp} -> {winner} approvals={n_appr}/{need} dry={args.dry_run}")
    if args.dry_run:
        return 0

    # apply active arm + lock
    try:
        if active_ttl > 0:
            await r.set(ak, winner, ex=active_ttl)
        else:
            await r.set(ak, winner)
        await r.set(lk, "1", ex=max(60, lock_sec))
    except Exception as e:
        print(f"WRITE_FAIL {e}")
        return 9

    # mark applied
    try:
        applied_payload = {
            "sid": sid,
            "ts_ms": _now_ms(),
            "by": args.by,
            "symbol": sym,
            "regime": rg,
            "group": grp,
            "winner_arm": winner,
            "approvers": appr,
            "meta_key": meta_key,
            "active_arm_key": ak,
            "lock_key": lk,
            "lock_sec": lock_sec,
        }
        await r.set(applied_key, json.dumps(applied_payload, ensure_ascii=False, separators=(",", ":")), ex=int(os.getenv("AB_APPLIED_TTL_SEC","604800")))
    except Exception:
        pass

    # best-effort audit stream (optional)
    try:
        audit_stream = os.getenv("CFG_APPLY_AUDIT_STREAM", "stream:trade:entry_audit")
        msg = {
            "type": "cfg_apply_active_arm",
            "ts_ms": str(_now_ms()),
            "symbol": sym,
            "regime": rg,
            "group": grp,
            "arm": winner,
            "sid": sid,
            "payload": json.dumps({"sid": sid, "approvers": appr, "key": ak}, separators=(",", ":")),
        }
        await r.xadd(audit_stream, msg, maxlen=50000, approximate=True)
    except Exception:
        pass

    return 0

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
