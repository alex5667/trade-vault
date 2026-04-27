from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

def run_cmd(cmd: list[str]) -> int:
    """Run a shell command and print its status."""
    print(f"\n>>> Running: {' '.join(cmd)}")
    try:
        res = subprocess.run(cmd, capture_output=False, text=True)
        return res.returncode
    except Exception as e:
        print(f"FAILED to execute: {e}")
        return 1

def main():
    ap = argparse.ArgumentParser(description="Nightly Meta Skew Guard Orchestrator v7.")
    ap.add_argument("--train-ndjson", required=True, help="Path to training features NDJSON")
    ap.add_argument("--out-dir", required=True, help="Directory to store intermediate reports and NDJSONs")
    ap.add_argument("--freeze-json", required=True, help="Path where META_FREEZE_FILE should be written")
    ap.add_argument("--redis", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), help="Redis URL")
    ap.add_argument("--hours", type=float, default=24.0, help="Lookback hours for serve-side export")
    ap.add_argument("--count", type=int, default=10000, help="Max entries to export from stream")
    
    args = ap.parse_args()

    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir, exist_ok=True)

    serve_ndjson = os.path.join(args.out_dir, "serve_confirmations_v7.ndjson")
    report_json = os.path.join(args.out_dir, "skew_report_v7.json")
    prom_file = os.path.join(args.out_dir, "meta_skew_v7.prom")

    # Step 1: Export serve-side confirmations from Redis Stream
    export_rc = run_cmd([
        sys.executable, "tools/export_serve_confirmations_ndjson.py",
        "--redis", args.redis,
        "--out", serve_ndjson,
        "--hours", str(args.hours),
        "--count", str(args.count)
    ])
    if export_rc != 0:
        print("CRITICAL: Exporter failed. Aborting nightly guard update.")
        sys.exit(1)

    # Step 2: Run Skew Audit (Train vs Serve)
    audit_rc = run_cmd([
        sys.executable, "tools/audit_train_vs_serve_skew_v7.py",
        "--train", args.train_ndjson,
        "--serve", serve_ndjson,
        "--out-json", report_json,
        "--out-prom", prom_file
    ])
    # Note: audit_rc may be non-zero if BAD skew is detected, but we proceed to Step 3 
    # because write_meta_freeze needs to handle those statuses.

    # Step 3: Write Meta Freeze JSON based on audit results
    freeze_rc = run_cmd([
        sys.executable, "tools/write_meta_freeze_from_skew_v7.py",
        "--skew-json", report_json,
        "--out", args.freeze_json
    ])
    
    if freeze_rc == 0:
        print(f"\nSUCCESS: Nightly Meta Skew Guard updated. Freeze file: {args.freeze_json}")
    else:
        print(f"\nFAILED: Could not update Meta Freeze file.")
        sys.exit(1)

if __name__ == "__main__":
    main()
