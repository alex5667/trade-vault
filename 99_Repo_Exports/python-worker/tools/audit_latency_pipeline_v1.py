#!/usr/bin/env python3
import asyncio
import json
import os

import numpy as np
import redis.asyncio as redis
from core.redis_keys import RedisStreams as RS

# P4.1 Unified Latency Contract: Audit Tool v1
# Purpose: Measure real-world latency by comparing ts_event_ms with Redis entry_id timestamps.

async def get_stream_latency(r: redis.Redis, stream_name: str, count: int = 100) -> list[float]:
    """Read the last N entries and calculate entry_ts - ts_event_ms."""
    try:
        entries = await r.xrevrange(stream_name, max=b"+", min=b"-", count=count)
    except Exception as e:
        print(f"❌ Error reading stream {stream_name}: {e}")
        return []

    latencies = []
    for entry_id, fields in entries:
        # Redis entry ID format: <timestamp_ms>-<sequence>
        entry_ts_ms = int(entry_id.split(b"-")[0])

        try:
            payload_raw = fields.get(b"payload")
            if not payload_raw:
                continue

            payload = json.loads(payload_raw)
            # Try different timestamp fields used in the contract
            ts_event_ms = int(payload.get("ts_event_ms") or payload.get("event_ts_ms") or payload.get("ts_ms") or 0)

            if ts_event_ms > 0:
                diff = entry_ts_ms - ts_event_ms
                # Sanity check: ignore legacy data or timezone mismatches (> 1 hour)
                if 0 <= diff < 3600000:
                    latencies.append(diff)
        except Exception:
            continue

    return latencies

async def audit():
    redis_worker_url = os.getenv("REDIS_WORKER_URL", "redis://localhost:63791/0")
    print(f"🔍 Starting Latency Audit on {redis_worker_url}...")

    try:
        r = redis.from_url(redis_worker_url)
        # Check connection
        await r.ping()
    except Exception as e:
        print(f"❌ Could not connect to Redis: {e}")
        return

    streams = [
        "events:delta_spike",      # Go -> Redis (Ingest Stage)
        RS.CRYPTO_RAW,      # Python -> Redis (Feature/Emit Stage)
        "events:decision_snapshot" # Decision Persistence
    ]

    for stream in streams:
        lats = await get_stream_latency(r, stream)
        if not lats:
            print(f"⚠️ No data in {stream} or missing ts_event_ms.")
            continue

        lats_arr = np.array(lats)
        print(f"\n📊 Results for {stream} (N={len(lats)}):")
        print(f"   P50: {np.percentile(lats_arr, 50):.2f} ms")
        print(f"   P95: {np.percentile(lats_arr, 95):.2f} ms")
        print(f"   P99: {np.percentile(lats_arr, 99):.2f} ms")
        print(f"   Max: {np.max(lats_arr):.2f} ms")

    await r.close()

if __name__ == "__main__":
    asyncio.run(audit())
