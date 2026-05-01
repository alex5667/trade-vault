#!/usr/bin/env python3
from __future__ import annotations
"""
Promote edge_stack_v1 champion → cfg:ml_confirm:champion with CANARY mode.

Sets enforce_share=0.05 (first ladder rung). After this, the P61 calibrator
will automatically propose higher ladder steps via Telegram.

Usage:
  # Dry-run (inspect only, no changes)
  python3 tools/promote_edge_stack_to_canary.py --dry-run

  # Apply promotion
  python3 tools/promote_edge_stack_to_canary.py

  # Force re-promotion (overwrite even if champion already in CANARY)
  python3 tools/promote_edge_stack_to_canary.py --force

Rollback:
  python3 tools/promote_edge_stack_to_canary.py --rollback
"""
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time

try:
    import redis
except ImportError:
    print("ERROR: redis-py not installed")
    sys.exit(1)


CHAMPION_KEY = "cfg:ml_confirm:champion"
EDGE_STACK_CHAMPION_KEY = "cfg:ml_confirm:edge_stack_v1:champion"
BACKUP_KEY = "cfg:ml_confirm:champion:backup"

INITIAL_ENFORCE_SHARE = 0.05  # First ladder rung


def _redis(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)


def _load_json(r: redis.Redis, key: str) -> dict | None:
    raw = r.get(key)
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _dump(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def cmd_promote(r: redis.Redis, *, dry_run: bool, force: bool) -> int:
    print(f"\n{'='*70}")
    print("PROMOTE edge_stack_v1:champion → cfg:ml_confirm:champion (CANARY)")
    print(f"{'='*70}\n")

    # 1) Check current champion
    print("📋 Step 1: Current champion state")
    print("-" * 50)
    cur = _load_json(r, CHAMPION_KEY)
    if cur:
        print(f"  kind:          {cur.get('kind', '?')}")
        print(f"  mode:          {cur.get('mode', '?')}")
        print(f"  enforce_share: {cur.get('enforce_share', '?')}")
        print(f"  run_id:        {cur.get('run_id', '?')}")
        if cur.get("mode") == "CANARY" and not force:
            print(f"\n⚠️  Champion already in CANARY mode (enforce_share={cur.get('enforce_share')})")
            print("   Use --force to overwrite.")
            return 0
    else:
        print("  NOT FOUND (will be created)")

    # 2) Check edge_stack_v1 champion (source)
    print(f"\n📋 Step 2: Source model (edge_stack_v1:champion)")
    print("-" * 50)
    src = _load_json(r, EDGE_STACK_CHAMPION_KEY)
    if not src:
        print(f"  ❌ NOT FOUND at {EDGE_STACK_CHAMPION_KEY}")
        print("  Cannot promote — no source model.")
        print("  Wait for nightly training (00:00 UTC) to produce a model first.")
        return 1

    print(f"  kind:          {src.get('kind', '?')}")
    print(f"  mode:          {src.get('mode', '?')}")
    print(f"  run_id:        {src.get('run_id', '?')}")
    print(f"  model_path:    {src.get('model_path', '?')}")
    print(f"  created_ms:    {src.get('created_ms', '?')}")

    # Validate required fields
    required = ["schema_version", "kind", "run_id", "model_path"]
    missing = [f for f in required if not src.get(f)]
    if missing:
        print(f"\n  ❌ Missing required fields: {missing}")
        return 1

    # 3) Build new champion config
    print(f"\n📋 Step 3: New champion config")
    print("-" * 50)
    new_cfg = dict(src)
    new_cfg["mode"] = "CANARY"
    new_cfg["enforce_share"] = INITIAL_ENFORCE_SHARE
    new_cfg["updated_ms"] = get_ny_time_millis()
    new_cfg["updated_by"] = "promote_edge_stack_to_canary"
    # Ensure schema_version is set
    if "schema_version" not in new_cfg:
        new_cfg["schema_version"] = 1

    print(f"  mode:          CANARY")
    print(f"  enforce_share: {INITIAL_ENFORCE_SHARE}")
    print(f"  run_id:        {new_cfg.get('run_id')}")

    if dry_run:
        print(f"\n✅ DRY-RUN: Would SET {CHAMPION_KEY}")
        print(f"   {_dump(new_cfg)[:200]}...")
        return 0

    # 4) Backup + write
    print(f"\n📋 Step 4: Applying promotion")
    print("-" * 50)

    # Backup current champion
    if cur:
        r.set(BACKUP_KEY, _dump(cur))
        print(f"  ✅ Backed up current champion → {BACKUP_KEY}")

    # Write new champion
    r.set(CHAMPION_KEY, _dump(new_cfg))
    print(f"  ✅ SET {CHAMPION_KEY}")

    # Verify
    verify = r.get(CHAMPION_KEY)
    if verify:
        v = json.loads(verify)
        print(f"  ✅ Verified: mode={v.get('mode')} enforce_share={v.get('enforce_share')}")
    else:
        print(f"  ❌ VERIFICATION FAILED: champion is empty after write!")
        return 1

    print(f"\n{'='*70}")
    print("✅ PROMOTION COMPLETE — ML Confirm Gate now in CANARY mode (5%)")
    print(f"{'='*70}")
    print(f"\nNext: Calibrator (daily 03:30 UTC) will propose ladder steps via Telegram.")
    print(f"Rollback: python3 tools/promote_edge_stack_to_canary.py --rollback\n")
    return 0


def cmd_rollback(r: redis.Redis) -> int:
    print(f"\n{'='*70}")
    print("ROLLBACK → SHADOW mode (enforce_share=0.0)")
    print(f"{'='*70}\n")

    cur = _load_json(r, CHAMPION_KEY)
    if not cur:
        print("❌ No champion config found. Nothing to rollback.")
        return 1

    cur["mode"] = "SHADOW"
    cur["enforce_share"] = 0.0
    cur["updated_ms"] = get_ny_time_millis()
    cur["updated_by"] = "rollback_to_shadow"

    r.set(CHAMPION_KEY, _dump(cur))
    print(f"✅ Reverted to SHADOW mode (enforce_share=0.0)")
    print(f"   run_id: {cur.get('run_id')}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Promote edge_stack_v1 champion → cfg:ml_confirm:champion (CANARY mode)",
    )
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    ap.add_argument("--dry-run", action="store_true", help="Inspect only, no writes")
    ap.add_argument("--force", action="store_true", help="Overwrite even if already CANARY")
    ap.add_argument("--rollback", action="store_true", help="Revert to SHADOW mode")
    args = ap.parse_args()

    r = _redis(args.redis_url)
    try:
        r.ping()
    except Exception as e:
        print(f"ERROR: Redis connection failed: {e}")
        return 1

    if args.rollback:
        return cmd_rollback(r)
    return cmd_promote(r, dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    raise SystemExit(main())
