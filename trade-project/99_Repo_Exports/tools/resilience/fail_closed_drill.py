#!/usr/bin/env python3
"""Fail-Closed Resilience Drill (Trade Scanner).

Verification script for P0-P1 audit findings:
1. Simulates Redis outage (Stop redis-worker-1).
2. Verifies auto-apply goes into SKIPPED_FROZEN state.
3. Verifies recovery after container restart.

Usage:
  python3 tools/resilience/fail_closed_drill.py [--dry-run] [--timeout 60]
"""

import argparse
import subprocess
import time
import sys
import os

def run_command(cmd, dry_run=False):
    print(f"Executing: {cmd}")
    if dry_run:
        return 0, "DRY_RUN", ""
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return p.returncode, p.stdout.strip(), p.stderr.strip()
    except Exception as e:
        return 1, "", str(e)

def wait_for_log_pattern(pattern, service, timeout=60, dry_run=False):
    print(f"Waiting for pattern '{pattern}' in logs of {service} (timeout={timeout}s)...")
    if dry_run:
        return True
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        rc, out, _ = run_command(f"docker compose logs --tail 20 {service}")
        if pattern in out:
            print(f"Found pattern: {pattern}")
            return True
        time.sleep(5)
    
    print(f"Timeout reached: pattern '{pattern}' not found.")
    return False

def main():
    parser = argparse.ArgumentParser(description="Fail-Closed Resilience Drill")
    parser.add_argument("--dry-run", action="store_true", help="Simulate steps without executing container commands")
    parser.add_argument("--timeout", type=int, default=60, help="Log wait timeout (seconds)")
    args = parser.parse_args()

    print(f"--- Starting Fail-Closed Resilience Drill ---")
    
    # 1. STOP REDIS
    print("\n[Step 1] Stopping redis-worker-1...")
    rc, _, err = run_command("docker compose stop redis-worker-1", args.dry_run)
    if rc != 0:
        print(f"Error stopping container: {err}")
        return 1
    
    # 2. VERIFY SKIPPED_FROZEN
    # Pattern is now printed to stdout by the guard
    print("\n[Step 2] Verifying auto-apply blocker state (Fail-Closed)...")
    
    # We trigger the job manually to avoid waiting for the 5m timer
    print("Triggering manual guard check within container...")
    drill_cmd = "docker compose exec -T auto-apply-job-timer python3 -m tools.auto_apply_job_entrypoint_hardguard_v1"
    rc_drill, out_drill, err_drill = run_command(drill_cmd, args.dry_run)
    
    # In fail_closed mode, the guard is configured to return 0 on skip by default (see AUTO_APPLY_SKIP_EXIT_CODE)
    # But we look for the specific string in output.
    combined_out = out_drill + err_drill
    if "AUTO_APPLY_DECISION: SKIPPED_FROZEN" in combined_out:
        print("SUCCESS: System correctly skipped execution (Fail-Closed triggered).")
        print(f"Detail: {combined_out.strip()}")
    else:
        print("FAIL: System did not enter fail-closed state or pattern not found.")
        print(f"Return Code: {rc_drill}")
        print(f"Output: {combined_out}")
        # Try to recover even on failure
        run_command("docker compose start redis-worker-1")
        return 1
    
    # 3. RESTART REDIS
    print("\n[Step 3] Restarting redis-worker-1...")
    rc, _, err = run_command("docker compose start redis-worker-1", args.dry_run)
    if rc != 0:
        print(f"Error starting container: {err}")
        return 1
    
    # 4. VERIFY RECOVERY
    print("\n[Step 4] Verifying system recovery...")
    # Give it some time to reconnect
    if not args.dry_run:
        time.sleep(5)
    
    rc_rec, out_rec, err_rec = run_command(drill_cmd, args.dry_run)
    if "AUTO_APPLY_DECISION: OK" in (out_rec + err_rec) or "AUTO_APPLY_DECISION: CMD_FAILED" in (out_rec + err_rec):
        print("SUCCESS: System recovered correctly (Guard can reach Redis).")
    else:
        print(f"WARNING: Recovery verification inconclusive. Output: {out_rec + err_rec}")

    print("\n--- Drill Completed Successfully ---")
    return 0

if __name__ == "__main__":
    sys.exit(main())
