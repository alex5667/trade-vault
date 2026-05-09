from __future__ import annotations

import argparse
import json
import os
import sys

import redis

from utils.time_utils import get_ny_time_millis


def export_stream_to_ndjson(
    redis_url: str,
    stream_key: str,
    out_path: str,
    count: int = 5000,
    max_age_hrs: float = 24.0
) -> int:
    """Export Redis Stream entries to NDJSON for skew audit."""
    r = redis.from_url(redis_url, decode_responses=True)

    # Calculate start ID based on time
    now_ms = get_ny_time_millis()
    start_ms = now_ms - int(max_age_hrs * 3600 * 1000)
    start_id = f"{start_ms}-0"

    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        # Use XRANGE to get entries from start_id to now
        entries = r.xrange(stream_key, min=start_id, count=count)

        for entry_id, data in entries:
            # Entry data is a dict of fields
            row = {}

            # 1. Look for JSON payload in 'indicators' or 'payload'
            payload_str = data.get("indicators") or data.get("payload")
            if payload_str:
                try:
                    payload = json.loads(payload_str)
                    if isinstance(payload, dict):
                        row.update(payload)
                except Exception:
                    pass

            # 2. Add flat fields from stream (they might be already there)
            row.update(data)

            # Cleanup non-serializable or redundant fields if needed
            # For skew audit we mostly need 'conf_*' and 'symbol'

            f.write(json.dumps(row) + "\n")
            n += 1

    return n

def main():
    ap = argparse.ArgumentParser(description="Export of_gate metrics from Redis Stream to NDJSON.")
    ap.add_argument("--redis", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"), help="Redis URL")
    ap.add_argument("--stream", default="metrics:of_gate", help="Redis Stream key")
    ap.add_argument("--out", required=True, help="Output NDJSON path")
    ap.add_argument("--count", type=int, default=10000, help="Max entries to export")
    ap.add_argument("--hours", type=float, default=24.0, help="Lookback hours")

    args = ap.parse_args()

    print(f"Exporting from {args.stream} to {args.out}...")
    try:
        n = export_stream_to_ndjson(
            redis_url=args.redis,
            stream_key=args.stream,
            out_path=args.out,
            count=args.count,
            max_age_hrs=args.hours
        )
        print(f"Done. Exported {n} entries.")
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
