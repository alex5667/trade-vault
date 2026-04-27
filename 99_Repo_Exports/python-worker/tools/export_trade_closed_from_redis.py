\
from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List, Tuple

import redis

def _event_ts_ms(fields: Dict[str, Any]) -> int:
    for k in ("ts_ms","ts","timestamp"):
        if k in fields:
            try:
                return int(float(fields.get(k) or 0))
            except Exception:
                pass
    # payload
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            return int(float(j.get("ts_ms", j.get("ts", 0)) or 0))
        except Exception:
            return 0
    return 0

def _is_closed(fields: Dict[str, Any]) -> bool:
    et = str(fields.get("event_type", fields.get("type","")) or "").upper()
    if et in ("POSITION_CLOSED","CLOSE"):
        return True
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            et2 = str(j.get("event_type", j.get("type","")) or "").upper()
            return et2 in ("POSITION_CLOSED","CLOSE")
        except Exception:
            return False
    return False

def _to_json(fields: Dict[str, Any]) -> Dict[str, Any]:
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            j["_raw"] = fields
            return j
        except Exception:
            pass
    return dict(fields)

def export_closed(*, r: redis.Redis, stream: str, since_ms: int, out_path: str, max_scan: int = 500_000) -> Tuple[int,int]:
    scanned = 0
    written = 0
    rows: List[Dict[str, Any]] = []
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
            ts = _event_ts_ms(fields)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            if not _is_closed(fields):
                continue
            j = _to_json(fields)
            j["_stream_id"] = msg_id
            rows.append(j)
    rows.sort(key=lambda x: int(float(x.get("ts_ms", x.get("ts", 0)) or 0)))
    with open(out_path, "w", encoding="utf-8") as f:
        for j in rows:
            f.write(json.dumps(j, ensure_ascii=False) + "\n")
            written += 1
    return written, scanned

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--since-hours", type=float, default=168.0)
    ap.add_argument("--stream", type=str, default=os.getenv("TRADE_EVENTS_STREAM", "events:trades"))
    ap.add_argument("--redis-url", type=str, default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--max-scan", type=int, default=500_000)
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    since_ms = int((time.time() - args.since_hours * 3600.0) * 1000)
    written, scanned = export_closed(r=r, stream=args.stream, since_ms=since_ms, out_path=args.out, max_scan=args.max_scan)
    print(f"written={written} scanned={scanned} stream={args.stream}")

if __name__ == "__main__":
    main()
