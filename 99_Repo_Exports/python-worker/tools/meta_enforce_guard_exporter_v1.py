"""
Meta Enforce Guard Exporter (P31).

Goal:
  - Expose meta-enforce guardrails state to Prometheus.
  - Reads from Redis dynamic config (meta_guard_freeze).
  - Metrics:
    - meta_enforce_guard_freeze (Gauge): 0=Ok, 1=Frozen
"""

import os
import time

import redis
from prometheus_client import Gauge, start_http_server


def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    port = int(os.getenv("META_GUARD_EXPORTER_PORT", "9133"))
    dyn_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")

    print(f"Starting Meta Enforce Guard Exporter on port {port}...")
    start_http_server(port)

    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # Metrics
    g_freeze = Gauge("meta_enforce_guard_freeze", "1 if meta enforce guardrail is active (frozen)")

    print(f"Connected to Redis: {redis_url}")

    while True:
        try:
            val = r.hget(dyn_key, "meta_guard_freeze")
            # Default to 0 if not set
            is_frozen = 1 if (val and str(val) == "1") else 0
            g_freeze.set(is_frozen)
        except Exception as e:
            print(f"Error reading from Redis: {e}")

        time.sleep(5)

if __name__ == "__main__":
    main()
