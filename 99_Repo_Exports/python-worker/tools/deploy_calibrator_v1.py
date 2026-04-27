#!/usr/bin/env python3
"""
Deploy Platt Logit Calibrator to Redis Configuration.

Usage:
  python3 tools/deploy_calibrator_v1.py --a 2.5 --b 0.0 --apply
"""

import argparse
import json
import os
import sys
import redis

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--key", default="cfg:ml_confirm:champion", help="Redis key to update")
    ap.add_argument("--a", type=float, default=2.5, help="Scaling factor (slope)")
    ap.add_argument("--b", type=float, default=0.0, help="Shift (intercept)")
    ap.add_argument("--apply", action="store_true", help="Actually write to Redis")
    args = ap.parse_args()

    print(f"Connecting to {args.redis_url}...")
    try:
        r = redis.Redis.from_url(args.redis_url, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return 1

    print(f"Target Key: {args.key}")
    
    # Check existing config
    current_cfg = r.hgetall(args.key)
    if not current_cfg:
        print(f"WARNING: Key {args.key} is empty or does not exist.")
        current_cfg = {}
    
    print("Current Config (partial):")
    print(f"  kind: {current_cfg.get('kind')}")
    print(f"  calibrator: {current_cfg.get('calibrator')}")

    # Prepare new calibrator
    new_cal = {
        "type": "platt_logit",
        "a": args.a,
        "b": args.b
    }
    new_cal_json = json.dumps(new_cal)

    print("\nProposed Change:")
    print(f"  calibrator -> {new_cal_json}")
    
    if args.apply:
        r.hset(args.key, "calibrator", new_cal_json)
        # Also ensure calibration is enabled
        r.hset(args.key, "calibrate_p_edge", "1")
        print("\nAPPLIED successfully.")
        
        # Verify
        final_cal = r.hget(args.key, "calibrator")
        final_flag = r.hget(args.key, "calibrate_p_edge")
        print(f"\nVerification:")
        print(f"  calibrator: {final_cal}")
        print(f"  calibrate_p_edge: {final_flag}")
    else:
        print("\nDRY RUN. Use --apply to execute.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
