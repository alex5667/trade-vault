from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""conf_score_guardrails_promote_v1.py

Promotes a staged policy bundle to LIVE status, gated by system health checks.

Workflow:
1. Read staged.json (pointer to candidate bundle).
2. Check health state (from Calibration Health Loop / Exporter).
   - Must be fresh (ts_ms).
   - Must not be degraded.
   - ECE/Brier within margins.
   - Minimum sample size (N).
3. If HEALTHY:
   - Load candidate bundle.
   - Apply candidate overrides to LIVE Redis keys (cfg:crypto_of:overrides:{SYMBOL}).
   - Update current.json pointer (atomically switch staged -> current).
4. If UNHEALTHY:
   - Log reason.
   - Exit with 0 (soft failure) or 1 (if strict).

Environment:
  REDIS_URL
  CONF_SCORE_GUARD_BUNDLE_DIR
  CONF_SCORE_GUARD_HEALTH_STATE_PATH
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import redis

# -------------------------------------------------------------------------
# Health Check Logic
# -------------------------------------------------------------------------

def check_health_gates(
    state_path: str,
    max_age_sec: float,
    ece_margin: float,
    brier_margin: float,
    min_n: int,
    allow_missing: int = 0
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Reads health state and verifies gates.
    Returns (passed: bool, reason: str, health_data: dict).
    """
    if not os.path.exists(state_path):
        if allow_missing:
            return True, "missing_allowed", {}
        return False, f"state_file_missing: {state_path}", {}

    try:
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return False, f"json_load_error: {e}", {}

    # 1. Freshness Check
    ts_ms = data.get("ts_ms")
    if not ts_ms:
        # Fallback to file mtime if ts_ms missing?
        # Better to be strict.
        ts_ms = 0
    
    now_ms = get_ny_time_millis()
    age_sec = (now_ms - int(ts_ms)) / 1000.0
    if age_sec > max_age_sec:
        return False, f"stale_state: age={age_sec:.1f}s > {max_age_sec}s", data

    # 2. Degrade Check
    if int(data.get("degrade", 0)) == 1:
        return False, "degraded_state", data

    # 3. Calibration Metrics (ECE / Brier)
    # Support flat keys or nested 'metrics'
    metrics = data.get("metrics", data)
    
    # ECE Check
    ece = float(metrics.get("ece_cal", metrics.get("ece_raw", 0.0)))
    if ece > ece_margin:
        # Warning: this might be too strict if typical ECE is high.
        # Usually we compare ece_cal (calibrated) vs raw.
        # But here we assume ece_margin is the max allowed ERROR.
        return False, f"ece_high: {ece:.4f} > {ece_margin}", data

    # Brier Check
    brier = float(metrics.get("brier_cal", metrics.get("brier_raw", 0.0)))
    if brier > brier_margin:
        return False, f"brier_high: {brier:.4f} > {brier_margin}", data

    # 4. Sample Size
    n = int(metrics.get("n", metrics.get("count", 0)))
    if n < min_n:
        return False, f"insufficient_n: {n} < {min_n}", data

    return True, "ok", data


# -------------------------------------------------------------------------
# Bundle & Redis Ops
# -------------------------------------------------------------------------

def load_staged_bundle(bundle_dir: Path) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Reads staged.json -> bundle filename -> bundle content."""
    staged_ptr = bundle_dir / "staged.json"
    if not staged_ptr.exists():
        return None, None
    
    try:
        with open(staged_ptr, "r") as f:
            ptr = json.load(f)
        
        fname = ptr.get("staged_file")
        if not fname:
            return None, None
            
        bundle_path = bundle_dir / fname
        if not bundle_path.exists():
            return fname, None
            
        with open(bundle_path, "r") as f:
            bundle_data = json.load(f)
            
        return fname, bundle_data
    except Exception:
        return None, None


def apply_bundle_to_live(
    r: redis.Redis,
    bundle_data: Dict[str, Any],
    key_prefix: str,
    dry_run: bool = False
) -> int:
    """Applies decisions from bundle to LIVE keys."""
    if dry_run:
        return 0
        
    decisions = bundle_data.get("decisions", {})
    count = 0
    now_ms = get_ny_time_millis()
    
    for sym, d in decisions.items():
        key = f"{key_prefix}{sym}"
        
        # Merge with existing? 
        # Strategy: read existing, update fields, write back.
        # This preserves other fields if any.
        raw = r.get(key)
        cur = {}
        if raw:
            try:
                cur = json.loads(raw)
            except Exception:
                cur = {}
        
        # Update critical fields
        cur["confidence_score_freeze"] = int(d.get("freeze", 0))
        cur["confidence_score_scale"] = float(d.get("scale", 1.0))
        
        # Metadata
        cur["conf_score_guard_ts_ms"] = now_ms
        cur["conf_score_guard_source"] = "promote"
        cur["conf_score_guard_policy_version"] = bundle_data.get("ts_ms")
        
        r.set(key, json.dumps(cur, separators=(",", ":")))
        count += 1
        
    return count


def update_live_pointer(bundle_dir: Path, staged_file: str, staged_sha: str) -> None:
    """Updates current.json to point to the staged file."""
    current_path = bundle_dir / "current.json"
    
    prev_info = {}
    if current_path.exists():
        try:
            with open(current_path, "r") as f:
                prev_info = json.load(f)
        except Exception:
            pass

    new_pointer = {
        "current_file": staged_file,
        "current_ts": get_ny_time_millis(),
        "current_sha": staged_sha,
        "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        "prev_file": prev_info.get("current_file"),
        "prev_ts": prev_info.get("current_ts"),
        "promoted_from": "staged.json"
    }

    tmp = current_path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(new_pointer, f, indent=2)
    tmp.replace(current_path)


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL"), required=True)
    parser.add_argument("--bundle-dir", default=os.getenv("CONF_SCORE_GUARD_BUNDLE_DIR", "/var/lib/trade/conf_score_guard_bundles"))
    parser.add_argument("--health-state-path", default=os.getenv("CONF_SCORE_GUARD_HEALTH_STATE_PATH", "/tmp/conf_cal_proof_state.json"))
    
    # Gates
    parser.add_argument("--require-health", type=int, default=1, help="If 1, enforce health checks")
    parser.add_argument("--max-age-sec", type=float, default=600.0)
    parser.add_argument("--ece-margin", type=float, default=0.01)
    parser.add_argument("--brier-margin", type=float, default=0.01)
    parser.add_argument("--min-n", type=int, default=300)
    parser.add_argument("--allow-missing", type=int, default=0, help="Allow missing state file (skip check)")
    
    parser.add_argument("--dry-run", type=int, default=0)
    
    args = parser.parse_args()
    
    # 1. Load Staged Bundle
    bundle_dir = Path(args.bundle_dir)
    fname, bundle_data = load_staged_bundle(bundle_dir)
    
    if not fname or not bundle_data:
        print("No staged bundle found or invalid staged.json.")
        sys.exit(0)  # Not an error, just nothing to do
        
    print(f"Candidate bundle: {fname}")

    # 2. Check Health
    if args.require_health:
        is_healthy, reason, hdata = check_health_gates(
            args.health_state_path,
            args.max_age_sec,
            args.ece_margin,
            args.brier_margin,
            args.min_n,
            args.allow_missing
        )
        
        if not is_healthy:
            print(f"Health check FAILED: {reason}. logic=SKIP_PROMOTE")
            sys.exit(0) # Skip cleanly
        else:
            print(f"Health check PASSED: {reason}")
    else:
        print("Health check SKIPPED (require-health=0)")

    # 3. Apply to Redis
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    ops = apply_bundle_to_live(r, bundle_data, "cfg:crypto_of:overrides:", dry_run=bool(args.dry_run))
    print(f"Applied overrides to Redis: {ops} ops (dry_run={args.dry_run})")
    
    # 4. Update Live Pointer
    if not args.dry_run:
        # We need the SHA from the bundle or staged pointer. 
        # Re-calc matches logic in apply script, but easier if we read staged.json
        # For now, let's just use what we have. 
        # Ideally we read staged.json again to get the SHA.
        try:
            with open(bundle_dir / "staged.json", "r") as f:
                staged_ptr = json.load(f)
                sha = staged_ptr.get("staged_sha", "unknown")
        except Exception:
            sha = "unknown"
            
        update_live_pointer(bundle_dir, fname, sha)
        print("Updated current.json pointer.")
    
    # Cleanup Staged?
    # Optional: remove staged.json to prevent re-promotion?
    # Better to leave it as "last staged". 
    # The system is idempotent: if current.json == staged.json, promote is no-op effective.
    # But for "noise", we might want to check if current == staged before doing Redis ops.
    # For now, simplistic is fine.

if __name__ == "__main__":
    main()
