#!/usr/bin/env python3
"""Wrapper to run a command only if tick quality gate passes.

Purpose:
  Enforce tick quality checks (data integrity, latency, freshness) before
  executing sensitive operations like ramp/rollout scripts.

Usage:
  python -m tools.run_tick_quality_gated_command \
      --metrics-url http://localhost:8000/metrics \
      --window-s 60 \
      --fail-mode fail_closed \
      -- \
      <COMMAND> [ARGS...]

Exit Codes:
  0   Gate PASS and Command PASS (or Fail Open triggered)
  10  Gate PASS but Command FAIL
  20  Gate FAIL (Threshold breach)
  21  Gate INSUFFICIENT_DATA and Fail Closed
  22  Gate Internal Error
"""

import argparse
import sys
import os
import subprocess
import time
import json
import logging
import socket
from typing import List, Optional, Dict, Any

# Ensure we can import tools.tick_quality_gate_check
# Assuming this script is run as a module: python -m tools.run_tick_quality_gated_command
try:
    from tools import tick_quality_gate_check
except ImportError:
    # If run directly from file, try to add parent to path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from tools import tick_quality_gate_check

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TickGateWrapper] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def publish_redis_event(
    redis_url: str,
    stream_key: str,
    event_data: Dict[str, Any]
) -> None:
    """Publish gate result to Redis Stream (best effort)."""
    try:
        import redis  # type: ignore
        r = redis.from_url(redis_url)
        # Use simple XADD
        r.xadd(stream_key, {"json": json.dumps(event_data)}, maxlen=50000)
        logger.info(f"Published event to Redis stream {stream_key}")
    except Exception as e:
        logger.warning(f"Failed to publish to Redis: {e}")

def run_gate_check(
    metrics_url: str,
    window_s: int,
    symbol: Optional[str],
    timeout_s: float
) -> int:
    """Run the tick quality gate check.
    
    Returns:
        Exit code from tick_quality_gate_check (0=PASS, 1=INSUFFICIENT, 2=FAIL)
    """
    logger.info(f"Running tick quality gate check (window={window_s}s)...")
    
    argv = [
        "--metrics-url", metrics_url,
        "--window-s", str(window_s),
        "--timeout-s", str(timeout_s),
    ]
    if symbol:
        argv.extend(["--symbol", symbol])
    
    # We want to capture the classification logic from the main function.
    # The existing main() prints to stdout/json. We can run it in-process.
    # However, to avoid capturing stdout/stderr messy-ness, we trust its return code.
    try:
        # Running via subprocess to ensure clean isolation of existing tool's output
        # or we could call main() and catch SystemExit? 
        # Calling main directly allows us to use the same logic without spawning new python interpreter overhead (though minimal)
        # But main calls sys.stdout.write. We might want to see that output.
        # Let's run it in-process and trust it prints useful info to stdout for the operator.
        return tick_quality_gate_check.main(argv)
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1
    except Exception as e:
        logger.error(f"Gate check exception: {e}")
        return 22 # Gate Tool Error

def main() -> int:
    parser = argparse.ArgumentParser(description="Tick Quality Gated Command Runner")
    
    # Gate arguments
    parser.add_argument("--metrics-url", default="http://localhost:8000/metrics", help="Prometheus metrics URL")
    parser.add_argument("--window-s", type=int, default=60, help="Observation window in seconds")
    parser.add_argument("--symbol", default=None, help="Optional symbol to check")
    parser.add_argument("--timeout-s", type=float, default=5.0, help="Scrape timeout")
    
    # Wrapper arguments
    parser.add_argument("--fail-mode", choices=["fail_open", "fail_closed"], default="fail_closed",
                        help="Action when gate returns INSUFFICIENT_DATA (missing metrics)")
    
    # The command to run
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Command to run if gate passes")
    
    args = parser.parse_args()
    
    # argparse puts the '--' separation into args.command if used properly, 
    # but sometimes it might be mixed. 
    # If the user did `... -- command arg`, args.command will be ['command', 'arg']
    # If the user did `... -- command`, args.command will be ['command']
    if not args.command or (len(args.command) == 1 and args.command[0] == "--"):
        logger.error("No command specified to run.")
        return 22

    # Remove leading '--' if present in the list
    cmd_list = args.command
    if cmd_list[0] == "--":
        cmd_list = cmd_list[1:]
        
    if not cmd_list:
        logger.error("No command specified after '--'")
        return 22

    # 1. Run Gate
    start_time = time.time()
    gate_exit_code = run_gate_check(
        metrics_url=args.metrics_url,
        window_s=args.window_s,
        symbol=args.symbol,
        timeout_s=args.timeout_s
    )
    duration = time.time() - start_time
    
    # 2. Evaluate Result
    should_run = False
    status_str = "UNKNOWN"
    
    if gate_exit_code == 0:
        logger.info("Tick Quality Gate: PASS")
        should_run = True
        status_str = "PASS"
    elif gate_exit_code == 2:
        logger.error("Tick Quality Gate: FAIL (Threshold Breach)")
        should_run = False
        status_str = "FAIL"
    elif gate_exit_code == 1:
        # INSUFFICIENT DATA
        if args.fail_mode == "fail_open":
            logger.warning("Tick Quality Gate: INSUFFICIENT_DATA (Proceeding due to fail_open)")
            should_run = True
            status_str = "INSUFFICIENT_DATA_OPEN"
        else:
            logger.error("Tick Quality Gate: INSUFFICIENT_DATA (Blocking due to fail_closed)")
            should_run = False
            status_str = "INSUFFICIENT_DATA_CLOSED"
    else:
        logger.error(f"Tick Quality Gate: INTERNAL ERROR (Code {gate_exit_code})")
        should_run = False
        status_str = "ERROR"

    # 3. Redis Audit (Best Effort)
    if os.environ.get("TICK_GATE_PUBLISH_REDIS") == "1":
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        sub_stream = os.environ.get("TICK_GATE_REDIS_STREAM", "ops:tick_quality_gate")
        event = {
            "ts": time.time(),
            "hostname": socket.gethostname(),
            "gate_status": status_str,
            "gate_duration_s": duration,
            "gate_exit_code": gate_exit_code,
            "target_command": cmd_list,
            "window_s": args.window_s,
            "fail_mode": args.fail_mode
        }
        publish_redis_event(redis_url, sub_stream, event)

    # 4. Execute or Exit
    if should_run:
        logger.info(f"Executing command: {' '.join(cmd_list)}")
        sys.stdout.flush()
        sys.stderr.flush()
        try:
            # Replace current process with the command? 
            # Or run as subprocess?
            # If we replace, we can't log the outcome. 
            # But normally wrappers might want to just be transparent.
            # However, prompt implies we might want to return specific codes.
            
            # "Exit codes: 0 gate PASS and command successful; 10 gate PASS but command failed"
            proc = subprocess.run(cmd_list)
            if proc.returncode != 0:
                logger.error(f"Command failed with exit code {proc.returncode}")
                return 10
            return 0
        except Exception as e:
            logger.error(f"Failed to execute command: {e}")
            return 10
    else:
        # Decide exit code based on refusal reason
        if gate_exit_code == 2:
            return 20
        elif gate_exit_code == 1:
            return 21
        else:
            return 22

if __name__ == "__main__":
    sys.exit(main())
