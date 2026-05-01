# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Auto-propose EntryPolicyOverridesV1 based on tm_policy_tuner JSON.

Writes:
  - cfg:suggestions:entry_policy:meta:{sid}
  - cfg:suggestions:entry_policy:latest:autopilot:{group} -> sid

Does NOT auto-apply: approvals workflow remains.

apply_kind="overrides_v1"
"""

from utils.time_utils import get_ny_time_millis

import argparse
import hashlib
import json
import os
import time
from typing import Any, Dict, Optional

import redis


def _now_ms() -> int:
    return get_ny_time_millis()


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return d


def propose_overrides_v1(
    *,
    redis_url: str,
    reco_json_path: str,
    group: str = "default",
    latest_prefix: str = "cfg:suggestions:entry_policy:latest:autopilot",
    meta_prefix: str = "cfg:suggestions:entry_policy:meta",
) -> Dict[str, Any]:
    r = redis.from_url(redis_url, decode_responses=True)
    with open(reco_json_path, "r", encoding="utf-8") as f:
        reco = json.loads(f.read() or "{}")
    g = (group or "default").strip().lower()
    now = _now_ms()

    # Build overrides payload:
    # - store tier map per (symbol, regime, scenario)
    tier_reco = reco.get("tier_reco", {}) or {}
    tier_map = {}
    for _, v in tier_reco.items():
        sym = _s(v.get("symbol", "")).upper()
        rg = _s(v.get("regime", "na")).lower()
        scn = _s(v.get("scenario", "na")).lower()
        tier = int(v.get("abs_lvl_tier", 1) or 1)
        if sym and rg and scn in ("continuation", "reversal"):
            tier_map[f"{sym}:{rg}:{scn}"] = tier

    # Optional: arm winners map (symbol:regime:scenario:group -> arm)
    arm_winners = reco.get("arm_winners", {}) or {}
    arm_map = {}
    for _, v in arm_winners.items():
        sym = _s(v.get("symbol", "")).upper()
        rg = _s(v.get("regime", "na")).lower()
        scn = _s(v.get("scenario", "na")).lower()
        grp = _s(v.get("group", g)).lower()
        arm = _s(v.get("winner_arm", "A")).upper()
        if sym and rg and scn in ("continuation", "reversal") and grp:
            arm_map[f"{sym}:{rg}:{grp}:{scn}"] = arm

    overrides = {
        "v": 1,
        "enabled": 1,
        "updated_ts_ms": now,
        "overrides_hold_down_ms": int(os.getenv("OVR_HOLD_DOWN_MS", "60000")),
        # The consumer (EntryPolicyService) may apply these fields if you wire them:
        "tier_map": tier_map,
        "arm_map": arm_map,
        # optional safety knobs
        "notes": f"autopilot_proposal@{now}",
    }

    sid = _sha1(f"overrides_v1|{g}|{now}|{len(tier_map)}|{len(arm_map)}")
    meta_key = f"{meta_prefix}:{sid}"
    latest_key = f"{latest_prefix}:{g}"
    meta = {
        "sid": sid,
        "apply_kind": "overrides_v1",
        "group": g,
        "updated_ts_ms": now,
        "overrides": overrides,
    }
    pipe = r.pipeline()
    pipe.set(meta_key, json.dumps(meta, ensure_ascii=False))
    pipe.set(latest_key, sid)
    pipe.execute()
    return {"sid": sid, "meta_key": meta_key, "latest_key": latest_key, "tier_n": len(tier_map), "arm_n": len(arm_map)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-json", required=True, type=str)
    ap.add_argument("--group", default="default", type=str)
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    args = ap.parse_args()
    res = propose_overrides_v1(redis_url=args.redis_url, reco_json_path=args.input_json, group=args.group)
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
