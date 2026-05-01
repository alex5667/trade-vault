#!/usr/bin/env python3
from __future__ import annotations
"""Manual tool: emit an emergency cfg:suggestions proposal.

Use-case:
- Approved-but-not-applied proposal is stuck (ApplyRunner down / lock stuck / consumer lag).
- Operator wants to create a tracked 'emergency' suggestion in the same contour.

Notes:
- Default is safe: ops=[] unless --ops_json or CFG_SUGGESTIONS_EMERGENCY_OPS_JSON is provided.
- This tool does NOT depend on ApplyRunner internals.
"""

from utils.time_utils import get_ny_time_millis

import argparse
import hashlib
import json
import os
import time
from typing import Any, Dict, List

import redis

try:
    from tools._ml_common import now_ms, safe_int
except Exception:  # pragma: no cover
    def now_ms() -> int:
        return get_ny_time_millis()

    def safe_int(x: Any, default: int = 0) -> int:
        try:
            return int(float(x))
        except Exception:
            return default


def _loads_ops_json(s: str) -> List[Dict[str, Any]]:
    if not s:
        return []
    try:
        v = json.loads(s)
    except Exception:
        return []
    if isinstance(v, list):
        return [x for x in v if isinstance(x, dict)]
    if isinstance(v, dict):
        return [v]
    return []


def _emergency_sid(emergency_kind: str, scope: str, ref_sid: str, now_ts_ms: int) -> str:
    h = hashlib.sha256(f"{emergency_kind}|{scope}|{ref_sid}".encode("utf-8", "ignore")).hexdigest()[:12]
    return f"emg:{emergency_kind}:{scope}:{now_ts_ms}:{h}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL") or os.getenv("TB_REDIS_URL") or "redis://localhost:6379/0")
    ap.add_argument("--prefix", default=os.getenv("CFG_SUGGESTIONS_PREFIX", "cfg:suggestions:entry_policy"))
    ap.add_argument("--emergency_kind", default=os.getenv("CFG_SUGGESTIONS_EMERGENCY_KIND", "emergency_apply_stuck"))
    ap.add_argument("--scope", default=os.getenv("CFG_SUGGESTIONS_SCOPE", "ALL"))
    ap.add_argument("--ref_kind", required=True)
    ap.add_argument("--ref_sid", required=True)
    ap.add_argument("--severity", default="CRIT")
    ap.add_argument("--age_ms", type=int, default=0)
    ap.add_argument("--alerts", default="")
    ap.add_argument("--ttl_sec", type=int, default=int(os.getenv("CFG_SUGGESTIONS_EMERGENCY_TTL_SEC", "86400")))
    ap.add_argument("--ops_json", default=os.getenv("CFG_SUGGESTIONS_EMERGENCY_OPS_JSON", ""))
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)

    now_ts_ms = now_ms()
    alerts = [a.strip() for a in str(args.alerts).split(",") if a.strip()]
    ops = _loads_ops_json(str(args.ops_json or ""))
    hint = os.getenv(
        "CFG_SUGGESTIONS_EMERGENCY_HINT",
        "Investigate ApplyRunner and apply or rollback the referenced proposal; consider unlocking apply contour if stuck.",
    )

    meta = {
        "kind": args.emergency_kind,
        "scope": args.scope,
        "ts_ms": now_ts_ms,
        "severity": (args.severity or "CRIT").upper(),
        "refs": {"kind": args.ref_kind, "scope": args.scope, "sid": args.ref_sid},
        "age_ms": int(args.age_ms),
        "alerts": alerts[:10],
        "hint": hint,
        "ops": ops,
    }

    sid = _emergency_sid(args.emergency_kind, args.scope, args.ref_sid, now_ts_ms)

    latest_key = f"{args.prefix}:latest:{args.emergency_kind}:{args.scope}"
    r.setex(f"{args.prefix}:meta:{sid}", args.ttl_sec, json.dumps(meta))
    try:
        r.hset(f"{args.prefix}:approvals:{sid}", mapping={"ts_ms": str(now_ts_ms), "status": "pending"})
        r.expire(f"{args.prefix}:approvals:{sid}", args.ttl_sec)
    except Exception:
        pass
    r.setex(latest_key, args.ttl_sec, sid)

    print(sid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
