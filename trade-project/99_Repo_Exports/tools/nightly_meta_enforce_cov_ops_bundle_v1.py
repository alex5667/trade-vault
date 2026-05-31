#!/usr/bin/env python3
"""nightly_meta_enforce_cov_ops_bundle_v1.py

P35: Unified bundle for nightly meta enforcement operations.
Orchestrates:
1) meta_cov_rollout_controller (check active buckets vs coverage)
2) meta_cov_outcome_auto_apply (apply outcomes to cfg2 with quarantine logic)
3) meta_cov_quarantine_monitor (verify invariants and emit metrics)

P37: Adds decision-log events and cfg2 snapshot metrics.

Usage:
  python3 -m tools.nightly_meta_enforce_cov_ops_bundle_v1 --apply --emit-metrics --notify
  META_COV_BUNDLE_APPLY=0 python3 -m tools.nightly_meta_enforce_cov_ops_bundle_v1 (dry-run)

Env:
  META_COV_BUNDLE_APPLY: if set to 0, --apply is ignored (dry-run forced).
  DYN_CFG_KEY: redis key for dynamic config
  REDIS_URL: redis connection string
"""

import argparse
import logging
import os
import sys
import time
import uuid
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("OpsBundle")

try:
    import redis
except ImportError:
    redis = None

# Try importing eventlog helper
try:
    from tools import meta_cov_ops_eventlog_v1
except ImportError:
    meta_cov_ops_eventlog_v1 = None


def run_command(cmd: List[str], check: bool = True) -> int:
    """Run a python module command in the same process via runpy or subprocess?
    Using subprocess is safer for tool isolation, but we must use same python env.
    """
    import subprocess

    cmd_str = " ".join(cmd)
    logger.info(f"RUNNING: {cmd_str}")
    
    # We use sys.executable to ensure same python interpreter
    full_cmd = [sys.executable, "-m"] + cmd
    
    try:
        # We want to stream output to stdout/stderr
        result = subprocess.run(full_cmd, check=check)
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {cmd_str} (rc={e.returncode})")
        if check:
            raise
        return e.returncode
    except Exception as e:
        logger.error(f"Execution error: {e}")
        if check:
            raise
        return 1


def main() -> None:
    # Generate run_id
    run_id = str(uuid.uuid4())
    ts_start = int(time.time() * 1000)

    parser = argparse.ArgumentParser(description="Nightly Meta ENFORCE Ops Bundle")
    parser.add_argument("--apply", action="store_true", help="Apply changes (unless META_COV_BUNDLE_APPLY=0)")
    parser.add_argument("--force", action="store_true", help="Force application even if checks fail (passed to tools)")
    parser.add_argument("--emit-metrics", action="store_true", help="Emit metrics to Redis")
    parser.add_argument("--notify", action="store_true", help="Send notifications (Telegram/Slack)")
    parser.add_argument("--print-json", action="store_true", help="Print result as JSON")
    
    # Compatibility arguments (may be passed by caller but not used directly or passed through)
    parser.add_argument("--dry-run", action="store_true", help="Explicit dry-run mode")

    args, unknown = parser.parse_known_args()

    # Redis Config
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    events_stream = os.getenv("META_COV_OPS_EVENTS_STREAM", "events:meta_cov_ops")
    dyn_cfg_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")

    r = None
    if redis:
        try:
            r = redis.from_url(redis_url, decode_responses=False)
        except Exception:
            logger.warning("Redis not available, event logging disabled.")

    def log_event(event: str, **kwargs):
        if meta_cov_ops_eventlog_v1 and r:
             meta_cov_ops_eventlog_v1.write_event(r, events_stream, run_id, event, kwargs)

    # ENV override for apply
    env_apply = os.getenv("META_COV_BUNDLE_APPLY")
    should_apply = args.apply
    if env_apply is not None:
        try:
            val = int(env_apply)
            if val <= 0:
                should_apply = False
                logger.warning(f"META_COV_BUNDLE_APPLY={val} -> FORCE DRY-RUN")
            else:
                should_apply = True
        except ValueError:
            pass

    log_event("bundle_start", args=vars(args), should_apply=should_apply)

    # Track metrics for snapshot
    metrics = {
        "meta_cov_ops_last_run_id": run_id,
        "meta_cov_ops_last_ts_ms": ts_start,
        "meta_cov_ops_last_apply_requested": 1 if should_apply else 0,
        "meta_cov_ops_last_ok": 0,    # Will set to 1 on success
        "meta_cov_ops_last_exit_code": 0,
        "meta_cov_ops_last_blocked_reasons": [],
        "meta_cov_ops_last_preflight_rc": -1,
        "meta_cov_ops_last_step_rc_validate": -1,
        "meta_cov_ops_last_step_rc_rollout": -1,
        "meta_cov_ops_last_step_rc_outcome": -1,
        "meta_cov_ops_last_step_rc_monitor": -1,
        "meta_cov_ops_last_apply_effective": 0,
    }

    blocked_reasons = []

    # 0. Preflight Validation
    logger.info("Step 0: Preflight Validation (meta_cov_ops_validate_v1)")
    cmd_validate = ["tools.meta_cov_ops_validate_v1"]
    # We pass through ENV vars, so no args needed usually.
    
    t0 = time.time()
    rc_validate = run_command(cmd_validate, check=False)
    dt_ms = int((time.time() - t0) * 1000)
    
    metrics["meta_cov_ops_last_preflight_rc"] = rc_validate
    metrics["meta_cov_ops_last_step_rc_validate"] = rc_validate
    log_event("bundle_step", step="validate", rc=rc_validate, dur_ms=dt_ms)
    
    effective_apply = should_apply

    if rc_validate == 0:
        logger.info("Preflight OK.")
    elif rc_validate == 2:
        logger.warning("Preflight returned SOFT-BLOCK (rc=2). Forcing dry-run (apply_effective=0).")
        effective_apply = False
        blocked_reasons.append("preflight_soft_block")
    else:
        logger.error(f"Preflight FAILED (rc={rc_validate}). Hard stop.")
        metrics["meta_cov_ops_last_exit_code"] = 1
        metrics["meta_cov_ops_last_blocked_reasons"] = blocked_reasons
        log_event("bundle_end", ok=0, exit_code=1, blocked_reasons=blocked_reasons)
        if meta_cov_ops_eventlog_v1 and r:
             meta_cov_ops_eventlog_v1.write_cfg2_snapshot(r, dyn_cfg_key, metrics)
        sys.exit(1)
        
    metrics["meta_cov_ops_last_apply_effective"] = 1 if effective_apply else 0

    # 1. Rollout Controller (supports --apply 0/1)
    logger.info("Step 1: Rollout Controller")
    cmd_rollout = ["tools.meta_cov_rollout_controller_v1"]
    # Usually we run rollout controller in read-only mode here (it just updates rollout logic), 
    # unless we want it to apply changes.
    val_apply = "1" if effective_apply else "0"
    cmd_rollout.extend(["--apply", val_apply])
    
    t0 = time.time()
    rc_rollout = run_command(cmd_rollout, check=False)
    dt_ms = int((time.time() - t0) * 1000)
    
    metrics["meta_cov_ops_last_step_rc_rollout"] = rc_rollout
    log_event("bundle_step", step="rollout", rc=rc_rollout, dur_ms=dt_ms)

    if rc_rollout != 0:
        logger.error("Rollout controller failed or reported issues. Proceeding with caution...")

    # 2. Outcome Auto Apply (supports --apply 0/1)
    logger.info("Step 2: Outcome Auto Apply")
    rc_outcome = -1
    if not os.path.exists("tools/meta_cov_outcome_auto_apply_v1.py"):
         logger.warning("tools/meta_cov_outcome_auto_apply_v1.py not found. SKIPPING.")
    else:
        cmd_apply = ["tools.meta_cov_outcome_auto_apply_v1"]
        cmd_apply.extend(["--apply", val_apply])
        # Outcome tool doesn't support --force, --emit-metrics, --notify directly based on analysis.
        
        t0 = time.time()
        rc_outcome = run_command(cmd_apply, check=False) # Don't check=True to catch rc
        dt_ms = int((time.time() - t0) * 1000)
        
        metrics["meta_cov_ops_last_step_rc_outcome"] = rc_outcome
        log_event("bundle_step", step="outcome", rc=rc_outcome, dur_ms=dt_ms)
        
        # run_command(cmd_apply, check=True) # Logic above handles it

    # 3. Quarantine Monitor (supports --emit-metrics, --notify)
    logger.info("Step 3: Quarantine Monitor")
    rc_monitor = -1
    if not os.path.exists("tools/meta_cov_quarantine_monitor_v1.py"):
        logger.warning("tools/meta_cov_quarantine_monitor_v1.py not found. SKIPPING.")
    else:
        cmd_monitor = ["tools.meta_cov_quarantine_monitor_v1"]
        if args.emit_metrics:
            cmd_monitor.append("--emit-metrics")
        if args.notify:
            cmd_monitor.append("--notify")
        
        # Pass --dry-run to monitor if we are in dry-run mode?
        if not effective_apply:
            cmd_monitor.append("--dry-run")
        
        t0 = time.time()
        rc_monitor = run_command(cmd_monitor, check=False)
        dt_ms = int((time.time() - t0) * 1000)

        metrics["meta_cov_ops_last_step_rc_monitor"] = rc_monitor
        log_event("bundle_step", step="monitor", rc=rc_monitor, dur_ms=dt_ms)

    # Determine overall OK status
    # We consider "OK" if all run steps returned 0.
    final_ok = 1
    if rc_rollout != 0 or (rc_outcome != 0 and rc_outcome != -1) or (rc_monitor != 0 and rc_monitor != -1):
        final_ok = 0
    
    metrics["meta_cov_ops_last_ok"] = final_ok
    metrics["meta_cov_ops_last_blocked_reasons"] = blocked_reasons

    logger.info(f"Bundle execution complete. OK={final_ok}")
    
    log_event("bundle_end", ok=final_ok, exit_code=0 if final_ok else 1, blocked_reasons=blocked_reasons)

    if meta_cov_ops_eventlog_v1 and r:
         meta_cov_ops_eventlog_v1.write_cfg2_snapshot(r, dyn_cfg_key, metrics)

    # P73: policy effectiveness Telegram summary (cooldown+dedup)
    # Safe to call on every tick: the reporter persists dedup/cooldown state in Redis.
    try:
        import policy_effectiveness_telegram_report_p73_v1 as p73
        p73.run_once()
    except Exception as e:
        logger.warning(f"P73 policy effectiveness telegram report failed (non-critical): {e}")
         
    if final_ok == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
