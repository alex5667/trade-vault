from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    return get_ny_time_millis()


def _atomic_write(path: str, text: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def _load_state(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(path: str, st: dict[str, Any]) -> None:
    _atomic_write(path, json.dumps(st, ensure_ascii=False, indent=2))


def _decode_payload(fields: dict[str, Any], field: str) -> str | None:
    if field in fields:
        v = fields.get(field)
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore")
        return str(v)
    # fallback: serialize entire entry
    try:
        return json.dumps(fields, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS))
    ap.add_argument("--field", default=os.getenv("OF_INPUTS_STREAM_FIELD", "payload"))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("OF_INPUTS_SINCE_HOURS", "24")))
    ap.add_argument("--since-ts-ms", type=int, default=0)
    ap.add_argument("--max-records", type=int, default=int(os.getenv("OF_INPUTS_MAX_RECORDS", "250000")))
    ap.add_argument("--out", required=True)
    ap.add_argument("--resume", type=int, default=1)
    ap.add_argument("--state-file", default=os.getenv("OF_INPUTS_STATE_FILE", ""))
    args = ap.parse_args()

    since_ms = int(args.since_ts_ms) if int(args.since_ts_ms) > 0 else (_now_ms() - int(args.since_hours * 3600_000))

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    state_path = str(args.state_file or "")
    st = _load_state(state_path) if (int(args.resume) == 1 and state_path) else {}
    last_id = (st.get("last_id", "") or "")
    if not last_id:
        last_id = f"{int(since_ms)}-0"
    else:
        last_id = f"({last_id}"  # exclusive resume

    wrote = 0
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    mode = "a" if (int(args.resume) == 1 and os.path.exists(args.out)) else "w"

    with open(args.out, mode, encoding="utf-8") as out:
        while wrote < int(args.max_records):
            rows = r.xrange(str(args.stream), min=last_id, max="+", count=500)
            if not rows:
                break
            for xid, fields in rows:
                payload = _decode_payload(dict(fields or {}), str(args.field))
                if payload:
                    out.write(payload.strip() + "\n")
                    wrote += 1
                last_id = xid
                if wrote >= int(args.max_records):
                    break
            last_id = f"({last_id}"
            if state_path:
                _save_state(state_path, {"last_id": str(str(last_id).lstrip("(")), "updated_ts_ms": _now_ms(), "wrote": wrote})
            if len(rows) < 500:
                break

    print(json.dumps({"ok": True, "out": args.out, "wrote": wrote, "since_ms": since_ms, "stream": args.stream}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

