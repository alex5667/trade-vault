from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Nightly regression test: run engine replay on baseline inputs and compare with baseline output.

Orchestrates:
  1. Engine replay on fixed baseline inputs
  2. Diff comparison with baseline output
  3. Telegram notification with results
  4. Exit code based on mismatch threshold

Usage:
  python -m tools.nightly_regress_engine_replay
  (reads BASELINE_INPUTS, BASELINE_OUTPUT from env)
"""

import argparse
import json
import os
import subprocess
import sys
import time

import redis

from utils.time_utils import get_ny_time_millis


def main() -> None:
    ap = argparse.ArgumentParser(description="Nightly regression test: engine replay + baseline diff")
    ap.add_argument("--baseline-inputs", default=os.getenv("BASELINE_INPUTS", ""), help="baseline inputs NDJSON path")
    ap.add_argument("--baseline-output", default=os.getenv("BASELINE_OUTPUT", ""), help="baseline output NDJSON path")
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"), help="output directory for run artifacts")
    ap.add_argument("--fail-on-mismatch", type=int, default=int(os.getenv("REGRESS_FAIL", "1") or 1), help="fail if mismatches exceed threshold (default: 1)")
    args = ap.parse_args()

    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    lock_key = os.getenv("REGRESS_LOCK_KEY", "lock:sre:nightly_regress_engine_replay")
    lock_ttl = int(os.getenv("REGRESS_LOCK_TTL_SEC", "7200") or 7200)
    if not r.set(lock_key, "1", nx=True, ex=lock_ttl):
        print(f"Skipping: Regression suite is already running or ran recently (lock {lock_key} active).")
        return

    if not args.baseline_inputs or not args.baseline_output:
        raise SystemExit("BASELINE_INPUTS and BASELINE_OUTPUT required (env or --baseline-inputs/--baseline-output)")

    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{args.out_dir}/regress_{ts}"
    os.makedirs(run_dir, exist_ok=True)

    cand_out = f"{run_dir}/candidate.ndjson"
    diff_out = f"{run_dir}/diff.json"

    # Step 1: engine replay on fixed baseline inputs
    print(f"Running engine replay on {args.baseline_inputs}...")
    subprocess.check_call([
        sys.executable, "-m", "tools.of_engine_replay_from_inputs",
        "--inputs", args.baseline_inputs,
        "--out", cand_out
    ])

    # Step 2: diff comparison
    print(f"Comparing with baseline {args.baseline_output}...")
    cmd = [
        sys.executable, "-m", "tools.of_regress_baseline_check",
        "--baseline", args.baseline_output,
        "--candidate", cand_out,
        "--out", diff_out,
        "--fail-on-mismatch", str(args.fail_on_mismatch),
    ]
    rc = 0
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        rc = e.returncode

    # Step 3: notify telegram
    try:
        r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
        rep = json.loads(open(diff_out, encoding="utf-8").read())
        msg = (
            "<b>Gate regress (engine replay)</b>\n"
            f"mismatches=<code>{rep.get('mismatches',0)}</code> overlap_n=<code>{rep.get('n',0)}</code>\n"
            f"by_field=<code>{rep.get('mismatch_by_field',{})}</code>\n"
            f"top_scn=<code>{rep.get('mismatch_by_scenario_v4_top',[])}</code>"
        )
        r.xadd(
            os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM),
            {"type": "report", "text": msg, "ts": str(get_ny_time_millis())},
            maxlen=200000,
            approximate=True
        )
    except Exception as e:
        print(f"Warning: failed to notify telegram: {e}", file=sys.stderr)

    if args.fail_on_mismatch == 1 and rc != 0:
        raise SystemExit(rc)


if __name__ == "__main__":
    main()

