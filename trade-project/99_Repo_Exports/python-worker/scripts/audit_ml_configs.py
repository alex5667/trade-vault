import json
import os

# Correct path for imports
import sys

import redis
from pydantic import ValidationError

sys.path.append(os.getcwd())

from services.ml_confirm_gate import MLConfirmConfig


def audit_configs():
    redis_url = os.getenv("ML_REDIS_URL") or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    print(f"Connecting to Redis at {redis_url}...")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # Patterns to search
    patterns = ["cfg:ml_confirm:*", "trade:champion:v4:*"]
    keys = []
    for pattern in patterns:
        keys.extend(r.keys(pattern))

    if not keys:
        print("No ML configuration keys found.")
        return

    print(f"Found {len(keys)} keys to audit.")
    violations = []

    for key in keys:
        try:
            # Check type first
            k_type = r.type(key)
            if k_type == "string":
                raw_payload = r.get(key)
                cfg = json.loads(raw_payload)
            elif k_type == "hash":
                h = r.hgetall(key)
                cfg = {str(k): v for k, v in h.items()}
            else:
                print(f"Skipping key {key} of type {k_type}")
                continue

            # Validate using MLConfirmConfig
            try:
                MLConfirmConfig.model_validate(cfg)
                print(f"[OK] {key}")
            except ValidationError as ve:
                print(f"[VIOLATION] {key}")
                violations.append({
                    "key": key,
                    "error": str(ve),
                    "config": cfg
                })
        except Exception as e:
            print(f"[ERROR] Failed to process key {key}: {e}")

    print("\n" + "="*50)
    print(f"AUDIT SUMMARY: {len(keys)} checked, {len(violations)} violations.")
    print("="*50)

    for v in violations:
        print(f"\nKEY: {v['key']}")
        print(f"ERRORS:\n{v['error']}")
        # Highlight specific p_min issues if possible from error string
        if "p_min" in v['error']:
            print(f"Current Config Summary: p_min={v['config'].get('p_min')}, p_min_by_bucket={v['config'].get('p_min_by_bucket')}")

if __name__ == "__main__":
    audit_configs()
