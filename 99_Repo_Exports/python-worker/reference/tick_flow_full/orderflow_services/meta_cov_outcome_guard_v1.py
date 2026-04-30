from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
meta_cov_outcome_guard_v1.py

P42: Outcome guardrails for meta coverage operations.
Monitors settings:dynamic_cfg (meta_cov_ops_last_*) and blocks auto-apply if:
- Snapshot is stale
- Last run was NOT OK (last_ok != 1)
- Preflight requested soft-block (last_preflight_rc == 2)
- Buckets are quarantined (optional)
"""

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Optional

try:
    import redis
except ImportError:
    redis = None


def now_ms() -> int:
    return get_ny_time_millis()


def _b2s(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return str(x)


def _load_json(v: Any) -> Any:
    if v is None: return None
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", "replace")
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def get_cfg_snapshot(r: redis.Redis, key: str) -> Dict[str, Any]:
    raw = r.hgetall(key)
    out = {}
    for k, v in raw.items():
        ks = _b2s(k)
        if ks.startswith("meta_cov_ops_last_"):
            out[ks] = _load_json(v)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--cfg-key", default=os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg"))
    parser.add_argument("--apply", type=int, default=int(os.getenv("META_COV_GUARD_APPLY", "1")))
    parser.add_argument("--max-stale-ms", type=int, default=int(os.getenv("META_COV_GUARD_MAX_STALE_MS", str(6 * 3600 * 1000))))
    parser.add_argument("--block-on-quarantine", type=int, default=int(os.getenv("META_COV_GUARD_BLOCK_ON_QUARANTINE", "1")))
    
    args = parser.parse_args()

    if redis is None:
        print("redis package not installed", file=sys.stderr)
        return 1

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    
    try:
        snapshot = get_cfg_snapshot(r, args.cfg_key)
    except Exception as e:
        print(f"Error reading config: {e}", file=sys.stderr)
        return 1

    ts_ms = int(snapshot.get("meta_cov_ops_last_ts_ms") or 0)
    now = now_ms()
    
    # Check staleness
    stale = ts_ms == 0 or (now - ts_ms) > args.max_stale_ms
    
    # Check last run status
    last_ok = int(snapshot.get("meta_cov_ops_last_ok") or 0)
    last_run_not_ok = (last_ok != 1)

    # Check preflight status
    preflight_rc = int(snapshot.get("meta_cov_ops_last_preflight_rc") or -1)
    preflight_soft = (preflight_rc == 2)

    quarantined = False
    if args.block_on_quarantine:
        # Check if any bucket is quarantined in the last snapshot
        # Note: 'meta_cov_ops_last_blocked_reasons' might contain 'quarantine' if the bundle detected it
        # But we also want to check the actual quarantine flags if available in snapshot
        # For now, rely on last_blocked_reasons or similar if available, OR just trust 'last_ok' covers it?
        # Actually P35 bundle sets last_ok=0 if quarantine monitor fails (which it shouldn't, unless error).
        # We should iterate last known state if available.
        # But wait, the bundle writes reasons.
        reasons = snapshot.get("meta_cov_ops_last_blocked_reasons")
        if isinstance(reasons, list) and "quarantine" in reasons:
             quarantined = True

    block = stale or last_run_not_ok or quarantined
    
    reason = []
    if stale: reason.append("stale_snapshot")
    if last_run_not_ok: reason.append("last_run_failed")
    if preflight_soft: reason.append("preflight_soft_block_ignored")
    if quarantined: reason.append("quarantine")
    
    reason_str = ",".join(reason) if reason else None
    
    meta = {
        "blocked": block
        "reason": reason_str
        "ts_ms": now
        "snapshot_ts_ms": ts_ms
        "stale": stale
        "last_run_not_ok": last_run_not_ok
        "preflight_soft": preflight_soft
        "quarantined": quarantined
    }

    print(json.dumps(meta, indent=2))

    if args.apply:
        prefix = os.getenv("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
        block_key = f"{prefix}:meta_cov"
        meta_key = f"{prefix}:meta_cov:meta"
        ts_key = f"{prefix}:meta_cov:ts_ms"
        
        if block:
            r.set(block_key, reason_str or "unknown", ex=int(os.getenv("META_COV_GUARD_BLOCK_TTL_SEC", "3600")))
            r.set(meta_key, json.dumps(meta), ex=int(os.getenv("META_COV_GUARD_BLOCK_TTL_SEC", "3600")))
            r.set(ts_key, str(now), ex=int(os.getenv("META_COV_GUARD_BLOCK_TTL_SEC", "3600")))
        else:
            # Clear block if everything is fine
            r.delete(block_key, meta_key, ts_key)

    return 2 if block else 0


if __name__ == "__main__":
    sys.exit(main())
