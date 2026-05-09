from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS


def _ts_ms(payload: dict[str, Any]) -> int:
    try:
        return int(float(payload.get("ts_ms", 0) or 0))
    except Exception:
        return 0

def export_inputs(*, r: redis.Redis, stream: str, since_ms: int, out_path: str, max_scan: int = 500_000) -> tuple[int,int]:
    scanned = 0
    written = 0
    rows: list[dict[str, Any]] = []
    last_id = "+"
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            payload = fields.get("payload")
            if not payload:
                continue
            if isinstance(payload, str):
                try:
                    j = json.loads(payload)
                except Exception:
                    continue
            else:
                # already dict?
                j = payload
            ts = _ts_ms(j)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            # annotate
            j["_stream_id"] = msg_id
            rows.append(j)
    rows.sort(key=_ts_ms)

    with open(out_path, "w", encoding="utf-8") as f:
        for j in rows:
            f.write(json.dumps(j, ensure_ascii=False) + "\n")
            written += 1
    return written, scanned

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--since-hours", type=float, default=24.0)
    ap.add_argument("--stream", type=str, default=os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS))
    ap.add_argument("--redis-url", type=str, default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--max-scan", type=int, default=500_000)
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    since_ms = int((time.time() - args.since_hours * 3600.0) * 1000)
    written, scanned = export_inputs(r=r, stream=args.stream, since_ms=since_ms, out_path=args.out, max_scan=args.max_scan)
    print(f"written={written} scanned={scanned} stream={args.stream}")

if __name__ == "__main__":
    import time
    main()
