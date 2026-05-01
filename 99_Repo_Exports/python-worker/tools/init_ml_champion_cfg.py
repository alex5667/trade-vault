#!/usr/bin/env python3
from __future__ import annotations
"""
init_ml_champion_cfg.py

Idempotent seeder for cfg:ml_confirm:champion.
Writes a SHADOW-mode config pointing to the latest available model in
/var/lib/trade/ml_models/. Safe to run on every startup.

Usage:
  python tools/init_ml_champion_cfg.py [--force] [--redis-url REDIS_URL]

Exits 0 on success, 1 on error.
"""
from utils.time_utils import get_ny_time_millis

import argparse
import glob
import json
import os
import sys
import time

try:
    import redis  # type: ignore
except ImportError:
    print("ERROR: redis-py not installed")
    sys.exit(1)

CHAMPION_KEY = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
DEFAULT_MODEL_BASE = "/var/lib/trade/ml_models"
MODEL_GLOB = "tb_v10_4_*"


def find_latest_model(base: str, pattern: str) -> str | None:
    """Find the latest model directory by name (sorted lexicographically)."""
    candidates = sorted(glob.glob(os.path.join(base, pattern)))
    for path in reversed(candidates):
        model_file = os.path.join(path, "model.joblib")
        if os.path.exists(model_file):
            return path
    return None


def read_util_floors(model_dir: str) -> dict:
    """Read util_floors.json from model directory, return simplified version."""
    floors_path = os.path.join(model_dir, "util_floors.json")
    try:
        with open(floors_path) as f:
            return json.load(f)
    except Exception:
        return {
            "global": {"floor": -0.05},
            "by_bucket": {
                "trend": {"floor": -0.05},
                "range": {"floor": -0.05},
                "other": {"floor": -0.05},
            },
            "unc_k": 0.5,
        }


def read_meta(model_dir: str) -> dict:
    """Read meta.json from model dir if present."""
    meta_path = os.path.join(model_dir, "meta.json")
    try:
        with open(meta_path) as f:
            return json.load(f)
    except Exception:
        return {}


def build_champion_cfg(model_dir: str) -> dict:
    run_id = os.path.basename(model_dir)
    meta = read_meta(model_dir)
    created_ms = int(meta.get("created_ms", get_ny_time_millis()))
    util_floors = read_util_floors(model_dir)

    return {
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": run_id,
        "created_ms": created_ms,
        "model_path": os.path.join(model_dir, "model.joblib"),
        "mode": "SHADOW",
        "enforce_share": 0.0,
        "util_floors": {
            "global": {"floor": util_floors.get("global", {}).get("floor", -0.05)},
            "by_bucket": {
                k: {"floor": v.get("floor", -0.05)}
                for k, v in util_floors.get("by_bucket", {}).items()
            },
            "unc_k": util_floors.get("unc_k", 0.5),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed cfg:ml_confirm:champion into Redis")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--model-base", default=DEFAULT_MODEL_BASE)
    ap.add_argument("--model-glob", default=MODEL_GLOB)
    ap.add_argument("--champion-key", default=CHAMPION_KEY)
    ap.add_argument("--force", action="store_true", help="Overwrite even if key exists")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    model_dir = find_latest_model(args.model_base, args.model_glob)
    if not model_dir:
        print(f"ERROR: No model found in {args.model_base}/{args.model_glob}")
        return 1

    print(f"INFO: Using model: {model_dir}")
    cfg = build_champion_cfg(model_dir)
    cfg_json = json.dumps(cfg, separators=(",", ":"), ensure_ascii=False)
    print(f"INFO: Champion cfg: run_id={cfg['run_id']} mode={cfg['mode']} enforce_share={cfg['enforce_share']}")

    if args.dry_run:
        print("DRY-RUN: Would SET", args.champion_key)
        print(cfg_json)
        return 0

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        print(f"ERROR: Redis connection failed: {e}")
        return 1

    existing = r.get(args.champion_key)
    if existing and not args.force:
        try:
            d = json.loads(existing)
            print(f"INFO: cfg:ml_confirm:champion already exists (run_id={d.get('run_id')}, mode={d.get('mode')}). Skipping. Use --force to overwrite.")
            return 0
        except Exception:
            pass  # corrupt – overwrite

    r.set(args.champion_key, cfg_json)
    print(f"OK: SET {args.champion_key} -> run_id={cfg['run_id']} mode={cfg['mode']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
