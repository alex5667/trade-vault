import json
import os
import redis
from typing import Dict, Any

# Correct path for imports
import sys
sys.path.append(os.getcwd())

def clamp_value(v: Any) -> float:
    try:
        f = float(v)
        if f < 0.5:
            return 0.5
        if f > 0.95:
            return 0.95
        return f
    except (ValueError, TypeError):
        return 0.55 # Safe default

def fix_configs():
    redis_url = os.getenv("ML_REDIS_URL") or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    print(f"Connecting to Redis at {redis_url} for fix...")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    patterns = ["cfg:ml_confirm:champion", "cfg:ml_confirm:challenger", "cfg:ml_confirm:champion:*"]
    keys = []
    for pattern in patterns:
        keys.extend(r.keys(pattern))

    for key in keys:
        try:
            raw_payload = r.get(key)
            if not raw_payload: continue
            
            cfg = json.loads(raw_payload)
            modified = False

            # Fix top-level p_min
            if "p_min" in cfg:
                old_v = cfg["p_min"]
                new_v = clamp_value(old_v)
                if old_v != new_v:
                    print(f"[{key}] Clamping p_min: {old_v} -> {new_v}")
                    cfg["p_min"] = new_v
                    modified = True

            # Fix p_min_by_bucket
            pmbb = cfg.get("p_min_by_bucket")
            if isinstance(pmbb, dict):
                for k, v in pmbb.items():
                    new_v = clamp_value(v)
                    if v != new_v:
                        print(f"[{key}] Clamping p_min_by_bucket[{k}]: {v} -> {new_v}")
                        pmbb[k] = new_v
                        modified = True

            # Fix util_floors / edge_floors
            for floor_key in ["util_floors", "edge_floors"]:
                floors = cfg.get(floor_key)
                if isinstance(floors, dict):
                    # Global
                    g = floors.get("global")
                    if isinstance(g, dict) and "floor" in g:
                        old_v = g["floor"]
                        new_v = clamp_value(old_v)
                        if old_v != new_v:
                            print(f"[{key}] Clamping {floor_key}.global.floor: {old_v} -> {new_v}")
                            g["floor"] = new_v
                            modified = True
                    
                    # By bucket
                    bb = floors.get("by_bucket")
                    if isinstance(bb, dict):
                        for k, b_cfg in bb.items():
                            if isinstance(b_cfg, dict) and "floor" in b_cfg:
                                old_v = b_cfg["floor"]
                                new_v = clamp_value(old_v)
                                if old_v != new_v:
                                    print(f"[{key}] Clamping {floor_key}.by_bucket[{k}].floor: {old_v} -> {new_v}")
                                    b_cfg["floor"] = new_v
                                    modified = True

            if modified:
                r.set(key, json.dumps(cfg, ensure_ascii=False, separators=(",", ":")))
                print(f"[FIXED] {key} updated in Redis.")
            else:
                print(f"[OK] {key} is already valid.")

        except Exception as e:
            print(f"[ERROR] Failed to fix key {key}: {e}")

if __name__ == "__main__":
    fix_configs()
