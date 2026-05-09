from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
"""nightly_meta_enforce_cov_ops_bundle_v1.py

P35: Unified bundle for nightly meta enforcement operations.
Orchestrates:
1) meta_cov_rollout_controller (check active buckets vs coverage)
2) meta_cov_outcome_guard (P43: check if we should block auto-apply)
3) meta_cov_outcome_auto_apply (apply outcomes to cfg2 with quarantine logic)
4) meta_cov_quarantine_monitor (verify invariants and emit metrics)

P37: Adds decision-log events and cfg2 snapshot metrics.
P43: Self-contained (calls local files), adds guard step.

Usage:
  python3 orderflow_services/nightly_meta_enforce_cov_ops_bundle_v1.py --apply --emit-metrics --notify
  META_COV_BUNDLE_APPLY=0 python3 orderflow_services/nightly_meta_enforce_cov_ops_bundle_v1.py (dry-run)

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

# Try importing eventlog helper locally
try:
    import meta_cov_ops_eventlog_v1
except ImportError:
    try:
        from orderflow_services import meta_cov_ops_eventlog_v1
    except ImportError:
        meta_cov_ops_eventlog_v1 = None


def get_wrapper_script(name: str) -> str:
    """Return path to a script in the same directory as this bundle."""
    # This bundle is likely in orderflow_services/
    # We want to call orderflow_services/<name>.py
    # If we are running as a script, __file__ helps.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, f"{name}.py")
    if not os.path.exists(path):
        # Fallback to current directory
        if os.path.exists(f"{name}.py"):
            return f"{name}.py"
        # Fallback to orderflow_services/ relative
        if os.path.exists(f"orderflow_services/{name}.py"):
            return f"orderflow_services/{name}.py"
    return path


def run_command(script_name: str, args: list[str], check: bool = True) -> int:
    """Run a python script in another process using the same interpreter."""
    import subprocess

    script_path = get_wrapper_script(script_name)
    if not os.path.exists(script_path):
        logger.error(f"Script not found: {script_path}")
        return 127

    full_cmd = [sys.executable, script_path] + args
    cmd_str = " ".join(full_cmd)
    logger.info(f"RUNNING: {cmd_str}")

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
    ts_start = get_ny_time_millis()

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
        "meta_cov_ops_last_step_rc_guard": -1,   # P43
        "meta_cov_ops_last_step_rc_outcome": -1,
        "meta_cov_ops_last_step_rc_monitor": -1,
        "meta_cov_ops_last_apply_effective": 0,
        "meta_cov_ops_last_decision_code": "unknown", # P43
        "meta_cov_ops_last_decision_ts_ms": get_ny_time_millis(), # P43
    }

    blocked_reasons = []

    # 0. Preflight Validation
    logger.info("Step 0: Preflight Validation (meta_cov_ops_validate_v1)")

    t0 = time.time()
    rc_validate = run_command("meta_cov_ops_validate_v1", [], check=False)
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
        metrics["meta_cov_ops_last_decision_code"] = "preflight_fail"
        log_event("bundle_end", ok=0, exit_code=1, blocked_reasons=blocked_reasons)
        if meta_cov_ops_eventlog_v1 and r:
             meta_cov_ops_eventlog_v1.write_cfg2_snapshot(r, dyn_cfg_key, metrics)
        sys.exit(1)

    metrics["meta_cov_ops_last_apply_effective"] = 1 if effective_apply else 0

    # 1. Rollout Controller (supports --apply 0/1)
    logger.info("Step 1: Rollout Controller")
    val_apply = "1" if effective_apply else "0"

    t0 = time.time()
    rc_rollout = run_command("meta_cov_rollout_controller_v1", ["--apply", val_apply], check=False)
    dt_ms = int((time.time() - t0) * 1000)

    metrics["meta_cov_ops_last_step_rc_rollout"] = rc_rollout
    log_event("bundle_step", step="rollout", rc=rc_rollout, dur_ms=dt_ms)

    if rc_rollout != 0:
        logger.error("Rollout controller failed or reported issues. Proceeding with caution...")

    # 2. Guard (P43) - Decide if we should block outcomes
    logger.info("Step 2: Meta Cov Outcome Guard")

    t0 = time.time()
    # P43: Pass --apply same as effective apply
    rc_guard = run_command("meta_cov_outcome_guard_v1", ["--apply", val_apply], check=False)
    dt_ms = int((time.time() - t0) * 1000)

    metrics["meta_cov_ops_last_step_rc_guard"] = rc_guard
    log_event("bundle_step", step="guard", rc=rc_guard, dur_ms=dt_ms)

    if rc_guard == 2:
        logger.warning("Guard returned 2 (BLOCK). It may have set blocking keys if apply=1.")
        metrics["meta_cov_ops_last_decision_code"] = "guard_block"
    elif rc_guard != 0:
        logger.error(f"Guard failed (rc={rc_guard}).")
        metrics["meta_cov_ops_last_decision_code"] = "guard_error"
    else:
        metrics["meta_cov_ops_last_decision_code"] = "ok"

    metrics["meta_cov_ops_last_decision_ts_ms"] = get_ny_time_millis()

    # 3. Outcome Auto Apply (supports --apply 0/1)
    logger.info("Step 3: Outcome Auto Apply")

    t0 = time.time()
    rc_outcome = run_command("meta_cov_outcome_auto_apply_v1", ["--apply", val_apply], check=False)
    dt_ms = int((time.time() - t0) * 1000)

    metrics["meta_cov_ops_last_step_rc_outcome"] = rc_outcome
    log_event("bundle_step", step="outcome", rc=rc_outcome, dur_ms=dt_ms)

    # 4. Quarantine Monitor (supports --emit-metrics, --notify)
    logger.info("Step 4: Quarantine Monitor")
    monitor_args = []
    if args.emit_metrics:
        monitor_args.append("--emit-metrics")
    if args.notify:
        monitor_args.append("--notify")

    if not effective_apply:
        monitor_args.append("--dry-run")

    t0 = time.time()
    rc_monitor = run_command("meta_cov_quarantine_monitor_v1", monitor_args, check=False)
    dt_ms = int((time.time() - t0) * 1000)

    metrics["meta_cov_ops_last_step_rc_monitor"] = rc_monitor
    log_event("bundle_step", step="monitor", rc=rc_monitor, dur_ms=dt_ms)

    # Determine overall OK status

    final_ok = 1
    if rc_rollout != 0 or rc_outcome != 0 or rc_monitor != 0:
        final_ok = 0
    if rc_guard not in (0, 2): # 2 is valid "Decided to block", not an error per se for "ops success"
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
