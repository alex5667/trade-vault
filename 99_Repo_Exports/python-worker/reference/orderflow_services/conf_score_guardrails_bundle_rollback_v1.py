from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""conf_score_guardrails_bundle_rollback_v1.py

Utility to instantly rollback confidence guardrails decisions to a specific bundle.

Usage:
  python -m orderflow_services.conf_score_guardrails_bundle_rollback_v1 \
    --bundle-dir /var/lib/trade/conf_score_guard_bundles \
    --target prev \
    --apply 1 \
    --promote-pointer 1

Targets:
  prev      : The bundle listed as 'prev_file' in current.json
  current   : The bundle listed as 'current_file' in current.json (re-apply)
  <filename>: Specific bundle filename (e.g. bundle_123456_v1_abc.json)
"""

import argparse
import fcntl
import json
import os
import sys
import time
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any, Dict, Optional, ContextManager

# We import apply logic to reuse it
from orderflow_services.conf_score_guardrails_apply_v1 import apply_overrides_redis


@contextmanager
def _acquire_lock(path: str) -> ContextManager[Any]:
    f = None
    try:
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        f = open(path, "w")
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield f
    except BlockingIOError:
        print(f"FATAL: Could not acquire lock {path}. Another instance running?")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: Lock error {path}: {e}")
        sys.exit(1)
    finally:
        if f:
            try:
                fcntl.flock(f, fcntl.LOCK_UN)
                f.close()
            except Exception:
                pass


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return {}


def resolve_target_file(bundle_dir: str, target: str) -> Optional[str]:
    current_path = os.path.join(bundle_dir, "current.json")

    # If asking for dynamic targets (current/prev), we need the pointer file
    if target in ("current", "prev"):
        if not os.path.exists(current_path):
            print(f"Error: current.json not found in {bundle_dir}, cannot resolve '{target}'")
            return None
        ptr = _load_json(current_path)
        
        tgt_file = None
        if target == "current":
            tgt_file = ptr.get("current_file")
        else:  # prev
            tgt_file = ptr.get("prev_file")
        
        if not tgt_file:
            print(f"Error: pointer for '{target}' is empty/null in current.json")
            return None
            
        # The pointer usually contains just the filename
        return os.path.join(bundle_dir, tgt_file)
    
    # Otherwise, treat as explicit filename or path
    # 1. Check if it's a full path that exists
    if os.path.exists(target):
        return target
        
    # 2. Check if it's a filename inside bundle_dir
    p = os.path.join(bundle_dir, target)
    if os.path.exists(p):
        return p
    
    print(f"Error: Could not find target bundle '{target}'")
    return None


def update_pointer(bundle_dir: str, current_file_path: str, ts: int) -> None:
    current_path = os.path.join(bundle_dir, "current.json")
    fname = os.path.basename(current_file_path)
    
    prev_info = {}
    if os.path.exists(current_path):
        prev_info = _load_json(current_path)
    
    # We are promoting an old bundle to be 'current'.
    # The 'prev' becomes what was just current.
    new_pointer = {
        "current_file": fname
        "current_ts": ts
        "current_sha": "rollback_promote"
        "updated_at_iso": datetime.now(timezone.utc).isoformat()
        "prev_file": prev_info.get("current_file")
        "prev_ts": prev_info.get("current_ts")
        "note": f"Rolled back to {fname}"
    }
    
    tmp_ptr = current_path + ".tmp"
    with open(tmp_ptr, "w", encoding="utf-8") as f:
        json.dump(new_pointer, f, indent=2)
    os.replace(tmp_ptr, current_path)
    print(f"Updated pointer: current -> {fname}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle-dir", required=True, help="Directory containing bundles and current.json")
    ap.add_argument("--target", required=True, help="Target to rollback to: 'prev', 'current', or filename")
    ap.add_argument("--apply", type=int, default=0, help="Set 1 to apply decisions to Redis")
    ap.add_argument("--promote-pointer", type=int, default=0, help="Set 1 to update current.json to point to this bundle")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""), help="Redis URL")
    ap.add_argument("--key-prefix", default="cfg:crypto_of:overrides:", help="Redis key prefix")
    ap.add_argument("--lock-path", default=os.getenv("CONF_SCORE_GUARD_LOCK_PATH", "/tmp/conf_score_guard.lock"))

    args = ap.parse_args()

    # Acquire lock to ensure we don't race with the cron apply script
    with _acquire_lock(args.lock_path):
        target_path = resolve_target_file(args.bundle_dir, args.target)
        if not target_path:
            return 1
        
        print(f"Loading bundle: {target_path}")
        bundle = _load_json(target_path)
        
        # Validate bundle structure
        decisions = bundle.get("decisions")
        if not isinstance(decisions, dict):
            # Try loading from 'state' wrapper if logic changed? 
            # In apply_v1, we dump the whole state as bundle content.
            # So bundle.get("decisions") should work.
            print("Error: Bundle has no 'decisions' dictionary.")
            return 1
        
        print(f"Bundle TS: {bundle.get('ts_ms')}")
        print(f"Decisions count: {len(decisions)}")

        if args.apply == 1:
            if not args.redis_url:
                print("Error: --redis-url is required to apply.")
                return 1
            
            print("Applying to Redis with ROLLBACK tagging...")
            now_ms = get_ny_time_millis()
            
            # Custom apply loop to add rollback tags
            import redis
            r_client = redis.Redis.from_url(args.redis_url, decode_responses=True)
            applied_count = 0
            
            check_ts = bundle.get("ts_ms")
            policy_file = os.path.basename(target_path)
            
            for sym, d in decisions.items():
                key = f"{args.key_prefix}{sym}"
                raw = r_client.get(key)
                cur = {}
                if raw:
                    try:
                        cur = json.loads(raw)
                    except:
                        cur = {}
                
                # Enforce decision
                cur["confidence_score_freeze"] = int(d.get("freeze", 0))
                cur["confidence_score_scale"] = float(d.get("scale", 1.0))
                
                # Metadata
                cur["conf_score_guard_ts_ms"] = now_ms
                cur["conf_score_guard_max_abs_dz"] = float(d.get("max_abs_dz", 0.0))
                cur["conf_score_guard_n"] = int(d.get("n", 0))
                
                # Rollback Tags
                cur["conf_score_guard_policy_version"] = check_ts
                cur["conf_score_guard_policy_file"] = policy_file
                cur["conf_score_guard_policy_stage"] = "rollback"
                
                r_client.set(key, json.dumps(cur, separators=(",", ":")))
                applied_count += 1
                
            res = {"applied": applied_count}
            print(f"Result: {res}")
            
            if args.promote_pointer == 1:
                ts = int(bundle.get("ts_ms") or 0)
                update_pointer(args.bundle_dir, target_path, ts)
        else:
            print("Dry run complete. Use --apply 1 to execute.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
