from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis


def _safe_json(obj: Any) -> str:
    """Serialize object to JSON string."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _i(x: Any, d: int = 0) -> int:
    """Convert to int with default."""
    try:
        return int(float(x))
    except Exception:
        return d


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


def export_stream_since(
    *,
    r: redis.Redis,
    stream: str,
    payload_field: str,
    since_ms: int,
    out_path: str,
    max_scan: int = 800_000,
    ts_field_guess: str = "ts_ms",
) -> tuple[int, int]:
    """Reads stream backwards and writes NDJSON in chronological order.

    Filters by event timestamp:
      - if payload has ts_field_guess -> use it
      - else fallback to stream id ms
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

            ts = _i(obj.get(ts_field_guess, 0), 0)
            if ts <= 0:
                try:
                    ts = int(str(msg_id).split("-", 1)[0])
                except Exception:
                    ts = 0
                if ts > 0:
                    obj[ts_field_guess] = ts

            if "sid" not in obj:
                obj["sid"] = str(msg_id)

            if ts and ts < since_ms:
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", required=True)
    ap.add_argument("--payload-field", required=True)
    ap.add_argument("--since-hours", type=float, default=24.0)
    ap.add_argument("--out", required=True)
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--max-scan", type=int, default=800_000)
    ap.add_argument("--ts-field-guess", default="ts_ms")
    args = ap.parse_args()

    since_ms = get_ny_time_millis() - int(args.since_hours * 3600_000)
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    written, scanned = export_stream_since(
        r=r,
        stream=args.stream,
        payload_field=args.payload_field,
        since_ms=since_ms,
        out_path=args.out,
        max_scan=int(args.max_scan),
        ts_field_guess=str(args.ts_field_guess),
    )
    print(_safe_json({"written": written, "scanned": scanned, "out": args.out, "stream": args.stream}))


if __name__ == "__main__":
    main()

