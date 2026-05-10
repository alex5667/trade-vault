from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def _safe_json(obj: Any) -> str:
    """Safe JSON serialization."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return d


def _stream_id_ms(msg_id: str) -> int:
    """Extract timestamp from stream message ID (<ms>-<seq>)."""
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


def _get_payload(fields: dict[str, Any], payload_field: str) -> dict[str, Any]:
    """Extract and parse payload from stream fields."""
    raw = fields.get(payload_field)
    if raw is None:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    s = str(raw)
    if not s.strip().startswith("{"):
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def export_stream(
    *,
    r: redis.Redis,
    stream: str,
    since_ms: int,
    out_path: str,
    max_scan: int,
    payload_field: str,
) -> tuple[int, int]:
    """
    Export stream messages to NDJSON file (reverse scan from latest).
    
    Returns:
        (written_count, scanned_count)
    """
    scanned = 0
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
            if not isinstance(fields, dict):
                continue

            obj = _get_payload(fields, payload_field)
            if not obj:
                continue

            ts = _i(obj.get("created_ms", 0), 0)
            if ts <= 0:
                ts = _stream_id_ms(msg_id)
                obj["created_ms"] = ts

            if ts < since_ms:
                scanned = max_scan
                break

            rows.append(obj)

    rows.reverse()
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in rows:
            f.write(_safe_json(obj) + "\n")
    return (len(rows), scanned)


def main() -> None:
    ap = argparse.ArgumentParser(description="Export TB labels from Redis Stream to NDJSON")
    ap.add_argument("--since-hours", type=float, default=6.0, help="Export labels from last N hours")
    ap.add_argument("--out", required=True, help="Output NDJSON file path")
    ap.add_argument("--stream", default=os.getenv("TB_LABELS_STREAM", RS.TB_LABELS), help="Redis stream name")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), help="Redis URL")
    ap.add_argument("--max-scan", type=int, default=500_000, help="Max messages to scan")
    ap.add_argument("--payload-field", default="payload", help="Field name containing JSON payload")
    args = ap.parse_args()

    since_ms = get_ny_time_millis() - int(args.since_hours * 3600_000)

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    written, scanned = export_stream(
        r=r,
        stream=str(args.stream),
        since_ms=since_ms,
        out_path=str(args.out),
        max_scan=int(args.max_scan),
        payload_field=str(args.payload_field),
    )
    print(_safe_json({"written": written, "scanned": scanned, "out": args.out, "stream": args.stream}))


if __name__ == "__main__":
    main()

