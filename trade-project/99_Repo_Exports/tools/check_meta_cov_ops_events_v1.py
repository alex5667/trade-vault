#!/usr/bin/env python3
"""check_meta_cov_ops_events_v1.py

P37: Utility to inspect meta_cov_ops events stream.

Usage:
  python3 -m tools.check_meta_cov_ops_events_v1 --n 20
"""

import argparse
import json
import os
import sys
import time

try:
    import redis
except ImportError:
    print("redis package not installed", file=sys.stderr)
    sys.exit(1)

def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect meta_cov_ops events")
    parser.add_argument("--stream", default=os.getenv("META_COV_OPS_EVENTS_STREAM", "events:meta_cov_ops"))
    parser.add_argument("--n", type=int, default=20, help="Number of recent events to show")
    parser.add_argument("--json", action="store_true", help="Output as JSON lines")
    args = parser.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    try:
        r = redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        print(f"Error connecting to Redis: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading last {args.n} events from {args.stream}...")
    
    try:
        # xrevrange to get recent items
        # max='+', min='-'
        items = r.xrevrange(args.stream, max="+", min="-", count=args.n)
    except Exception as e:
        print(f"Error reading stream: {e}", file=sys.stderr)
        sys.exit(1)

    if not items:
        print("No events found.")
        return

    # items is list of (msg_id, fields_dict)
    # We want to display them chronologically if user prefers, but xrevrange returns newest first.
    # Let's reverse for chronological display if not JSON
    if not args.json:
        items = list(reversed(items))

    for msg_id, fields in items:
        if args.json:
            out = {"id": msg_id, "fields": fields}
            print(json.dumps(out))
        else:
            ts_ms = float(fields.get("ts_ms", 0))
            ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000)) if ts_ms > 0 else "N/A"
            event = fields.get("event", "UNKNOWN")
            run_id = fields.get("run_id", "-")
            
            # Formatting nicely
            print(f"[{ts_str}] {msg_id} | {event} | run_id={run_id}")
            for k, v in fields.items():
                if k in ["ts_ms", "event", "run_id"]:
                    continue
                print(f"    {k}: {v}")
            print("-" * 60)

if __name__ == "__main__":
    main()
