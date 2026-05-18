"""
apply_gate_tuning_v1.py — записывает параметры gate-тюнинга в settings:dynamic_cfg.

Применяет:
  - strong_of_gate:  need_reversal=3, fallback_a=0, z_min=2.5, delta_z_thr=4.0
  - candidate_score: spread_z_penalty_start=1.5, spread_bps_penalty_start=6.0
  - burst_gate_mode: veto
  - taker_flow_gate_mode: enforce

Usage:
    cd python-worker
    python -m tools.apply_gate_tuning_v1 [--dry-run] [--redis-url URL]

Rollback:
    python -m tools.apply_gate_tuning_v1 --rollback [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import redis

DYN_CFG_KEY = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
AUDIT_KEY = "cfg:gate_tuning_v1:last_applied"

# ── Target values ──────────────────────────────────────────────────────────────
TARGET: dict[str, str] = {
    # strong_of_gate
    "strong_need_reversal": "3",           # was default 2 → all 3 legs required
    "strong_need_continuation": "2",       # unchanged
    "strong_cont_allow_fallback_a": "0",   # was 1 → disables regime/direction fallback
    "strong_z_min": "2.5",                 # was 2.0 → stronger delta spike required
    "strong_cont_delta_z_thr": "4.0",      # was 3.0 → harder intensity fallback
    "strong_need_escalated": "3",          # unchanged
    # candidate_score spread penalties
    "spread_z_penalty_start": "1.5",       # was 2.0 → penalty kicks in earlier
    "spread_bps_penalty_start": "6.0",     # was 8.0 → penalty kicks in earlier
    # burst gate
    "burst_gate_mode": "veto",             # was penalty → hard veto on burst
    # taker flow gate
    "taker_flow_gate_mode": "enforce",     # was shadow → blocks contra flow entries
}

# ── Previous defaults (for rollback) ──────────────────────────────────────────
ROLLBACK: dict[str, str] = {
    "strong_need_reversal": "2",
    "strong_need_continuation": "2",
    "strong_cont_allow_fallback_a": "1",
    "strong_z_min": "2.0",
    "strong_cont_delta_z_thr": "3.0",
    "strong_need_escalated": "3",
    "spread_z_penalty_start": "2.0",
    "spread_bps_penalty_start": "8.0",
    "burst_gate_mode": "penalty",
    "taker_flow_gate_mode": "shadow",
}


def _connect(url: str) -> redis.Redis:
    r = redis.from_url(url, decode_responses=True, socket_connect_timeout=5)
    r.ping()
    return r


def _show_current(r: redis.Redis, keys: list[str]) -> None:
    print("\nCurrent values in Redis:")
    vals: list = r.hmget(DYN_CFG_KEY, keys)  # type: ignore[assignment]
    for k, v in zip(keys, vals):
        print(f"  {k} = {v!r} (None = not set, will use code default)")


def _apply(r: redis.Redis, patch: dict[str, str], *, dry_run: bool) -> None:
    if dry_run:
        print("\n[DRY-RUN] Would HSET the following into", DYN_CFG_KEY)
        for k, v in patch.items():
            print(f"  {k} = {v}")
        return

    mapping = {k: v for k, v in patch.items()}
    r.hset(DYN_CFG_KEY, mapping=mapping)

    audit = {
        "ts_ms": int(time.time() * 1000),
        "patch": patch,
        "applied_by": "apply_gate_tuning_v1",
    }
    r.set(AUDIT_KEY, json.dumps(audit), ex=86400 * 7)

    print(f"\n✓ Applied {len(patch)} keys to {DYN_CFG_KEY}")
    for k, v in patch.items():
        print(f"  {k} = {v}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Print what would change, don't write")
    ap.add_argument("--rollback", action="store_true", help="Restore previous defaults")
    ap.add_argument("--redis-url", default=REDIS_URL, help=f"Redis URL (default: {REDIS_URL})")
    args = ap.parse_args()

    patch = ROLLBACK if args.rollback else TARGET
    label = "ROLLBACK" if args.rollback else "APPLY"

    try:
        r = _connect(args.redis_url)
    except Exception as e:
        print(f"Redis connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    _show_current(r, list(patch.keys()))

    print(f"\n{label} gate tuning v1" + (" (DRY-RUN)" if args.dry_run else ""))
    _apply(r, patch, dry_run=args.dry_run)

    if not args.dry_run:
        print("\nNote: running workers pick up dynamic_cfg changes on the next tick.")
        print("Monitor: signals_veto_total{reason='VETO_CONFIRM'} — should rise ~30-50%")
        print("Monitor: of_confirm_ok_rate — expect drop from current baseline")


if __name__ == "__main__":
    main()
