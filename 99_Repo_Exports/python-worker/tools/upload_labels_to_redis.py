from __future__ import annotations

import argparse
import json
import os

import redis
from core.redis_keys import RedisStreams as RS

# Utility to bridge manual labeling and training steps
# Uploads generated labels to Redis labels:tb stream

def main() -> None:
    ap = argparse.ArgumentParser(description="Upload labels from NDJSON to Redis stream")
    ap.add_argument("--path", required=True, help="Path to NDJSON file")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("TB_LABELS_STREAM", RS.TB_LABELS))
    ap.add_argument("--maxlen", type=int, default=200000)
    args = ap.parse_args()

    if not os.path.exists(args.path):
        print(f"ERROR: File not found: {args.path}")
        return

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    count = 0
    with open(args.path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Ensure it's valid JSON
            try:
                json.loads(line)
            except Exception:
                print(f"WARNING: Skipping invalid JSON line: {line[:50]}...")
                continue

            # Stream expects 'payload' field as per services.tb_labeler_worker_v10_1
            r.xadd(args.stream, {"payload": line}, maxlen=args.maxlen, approximate=True)
            count += 1

    print(f"✅ Successfully uploaded {count} labels to Redis stream: {args.stream}")

if __name__ == "__main__":
    main()
