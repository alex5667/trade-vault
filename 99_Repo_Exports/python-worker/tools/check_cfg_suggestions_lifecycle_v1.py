#!/usr/bin/env python3
"""
python-worker/tools/check_cfg_suggestions_lifecycle_v1.py

Standalone health check tool for configuration suggestions lifecycle.
Exit codes: 0 OK, 2 FAIL (alerts found or error).
"""
import argparse
import json
import os
import sys

import redis

from tools.cfg_suggestions_lifecycle import check_suggestions_health


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default=os.getenv("CFG_SUGGESTIONS_PREFIX", "cfg:suggestions:entry_policy"))
    ap.add_argument("--kind", default=os.getenv("CFG_SUGGESTIONS_KIND", "meta_freeze"))
    ap.add_argument("--scopes", default=os.getenv("CFG_SUGGESTIONS_SCOPES", "ALL"))
    ap.add_argument("--max-created-age-ms", type=int, default=int(os.getenv("CFG_SUGGESTIONS_MAX_CREATED_AGE_MS", "3600000")))
    ap.add_argument("--max-approved-age-ms", type=int, default=int(os.getenv("CFG_SUGGESTIONS_MAX_APPROVED_AGE_MS", "600000")))
    ap.add_argument("--strict", action="store_true", default=os.getenv("CFG_SUGGESTIONS_SRE_STRICT", "0") == "1")
    ap.add_argument("--print-json", action="store_true")
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        scopes = [s.strip() for s in args.scopes.split(",") if s.strip()]

        summary, alerts = check_suggestions_health(
            r,
            prefix=args.prefix,
            kind=args.kind,
            scopes=scopes,
            max_created_age_ms=args.max_created_age_ms,
            max_approved_age_ms=args.max_approved_age_ms,
            strict=args.strict
        )

        if args.print_json:
            print(json.dumps({"summary": summary, "alerts": alerts}, indent=2))
        else:
            print(f"CFG Suggestions Health: {args.kind} scopes={scopes}")
            print(f"Pending: {summary['n_pending']}, Approved: {summary['n_approved']}, Applied: {summary['n_applied']}")
            if alerts:
                print(f"ALERTS: {alerts}")

        if alerts:
            sys.exit(2)
        sys.exit(0)

    except Exception as e:
        if args.print_json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"ERROR: {e}")
        sys.exit(2)

if __name__ == "__main__":
    main()
