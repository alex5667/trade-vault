#!/usr/bin/env python3
from __future__ import annotations

"""Обновление of_score_min в конфигурации Redis для canary symbols.

Устанавливает of_score_min=0.60 для всех canary symbols.
"""


import argparse
import os
import sys

import redis


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--score-min", type=float, default=0.60)
    ap.add_argument("--dry-run", action="store_true", help="Show what would be changed without applying")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    try:
        r.ping()
    except Exception as e:
        print(f"❌ Cannot connect to Redis: {e}", file=sys.stderr)
        sys.exit(1)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    print(f"Updating of_score_min to {args.score_min} for symbols: {', '.join(symbols)}")
    print(f"Redis: {args.redis_url}")
    print(f"Dry-run: {args.dry_run}\n")

    updated = []
    unchanged = []
    errors = []

    for sym in symbols:
        key = f"config:orderflow:{sym}"
        try:
            current = r.hget(key, "of_score_min")
            current_val = float(current) if current else None

            if current_val is None:
                status = "NOT_SET"
            elif abs(current_val - args.score_min) < 0.001:
                status = "ALREADY_SET"
            else:
                status = "NEEDS_UPDATE"

            print(f"{sym}:")
            print(f"  Current: {current_val if current_val is not None else 'NOT_SET'}")
            print(f"  Status: {status}")

            if status == "NEEDS_UPDATE" or status == "NOT_SET":
                if not args.dry_run:
                    r.hset(key, "of_score_min", str(args.score_min))
                    print(f"  ✅ Updated to {args.score_min}")
                    updated.append(sym)
                else:
                    print(f"  [DRY-RUN] Would update to {args.score_min}")
                    updated.append(sym)
            else:
                print("  ⏭️  No change needed")
                unchanged.append(sym)

        except Exception as e:
            print(f"  ❌ Error: {e}")
            errors.append((sym, str(e)))

    print("\n=== Summary ===")
    print(f"Updated: {len(updated)} ({', '.join(updated) if updated else 'none'})")
    print(f"Unchanged: {len(unchanged)} ({', '.join(unchanged) if unchanged else 'none'})")
    if errors:
        print(f"Errors: {len(errors)}")
        for sym, err in errors:
            print(f"  {sym}: {err}")

    if args.dry_run:
        print("\n⚠️  DRY-RUN mode: no changes were made")
    elif updated:
        print(f"\n✅ Successfully updated {len(updated)} symbol(s)")


if __name__ == "__main__":
    main()

