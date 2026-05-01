#!/usr/bin/env python3
from __future__ import annotations
"""Initialize cfg:ml_confirm with v5 fields (per-symbol shares, bucket-aware config)."""


import os
import sys
import redis
from core.share_map import dump_map

def main() -> None:
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    
    # Read existing config
    cfg = r.hgetall(cfg_key) or {}
    
    # Initialize v5 fields if missing
    updates = {}
    
    # Per-symbol share maps (empty by default)
    if "enforce_share_trend_by_symbol" not in cfg:
        updates["enforce_share_trend_by_symbol"] = dump_map({})
    if "enforce_share_range_by_symbol" not in cfg:
        updates["enforce_share_range_by_symbol"] = dump_map({})
    
    # Optional bucket-aware shares (if not set, use default enforce_share)
    if "enforce_share_trend" not in cfg and "enforce_share" in cfg:
        updates["enforce_share_trend"] = cfg.get("enforce_share", "0.0")
    if "enforce_share_range" not in cfg and "enforce_share" in cfg:
        updates["enforce_share_range"] = cfg.get("enforce_share", "0.0")
    
    # Optional bucket-aware p_min thresholds
    if "p_min_trend" not in cfg and "p_min_default" in cfg:
        updates["p_min_trend"] = cfg.get("p_min_default", "0.55")
    if "p_min_range" not in cfg and "p_min_default" in cfg:
        updates["p_min_range"] = cfg.get("p_min_default", "0.58")
    
    if not updates:
        print(f"✅ cfg:ml_confirm already has v5 fields")
        return
    
    # Apply updates
    for k, v in updates.items():
        r.hset(cfg_key, k, v)
        print(f"✅ Set {k} = {v}")
    
    print(f"\n✅ Initialized v5 fields in {cfg_key}")
    print(f"\nCurrent config:")
    final_cfg = r.hgetall(cfg_key)
    for k in sorted(final_cfg.keys()):
        v = final_cfg[k]
        if len(v) > 100:
            v = v[:100] + "..."
        print(f"  {k} = {v}")

if __name__ == "__main__":
    main()

