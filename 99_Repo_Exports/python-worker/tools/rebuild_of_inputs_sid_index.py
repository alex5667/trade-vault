#!/usr/bin/env python3
# python-worker/tools/rebuild_of_inputs_sid_index.py
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Optional

try:
    import redis  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit("redis package is required") from e


def _safe_loads(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="ignore")
        except Exception:
            return None
    if not isinstance(raw, str) or not raw:
        return None
    try:
        o = json.loads(raw)
        return o if isinstance(o, dict) else None
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", required=True)
    ap.add_argument("--stream", default="signals:of:inputs")
    ap.add_argument("--field", default="payload")
    ap.add_argument("--prefix", default="idx:of_inputs:sid:")
    ap.add_argument("--ttl_sec", type=int, default=172800)
    ap.add_argument("--count", type=int, default=200000)
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    msgs = r.xrevrange(args.stream, max="+", min="-", count=int(args.count))
    n = 0
    for msg_id, fields in msgs:
        if not isinstance(fields, dict):
            continue
        raw = fields.get(args.field)
        o = _safe_loads(raw)
        if not o:
            continue
        sid = str(o.get("sid") or "")
        if not sid:
            continue
        sid_key = f"{args.prefix}{sid}"
        sid_val = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
        try:
            r.setex(sid_key, int(args.ttl_sec), sid_val)
            n += 1
        except Exception:
            continue

    print(json.dumps({"ok": True, "indexed": n, "scanned": len(msgs)}, ensure_ascii=False))


if __name__ == "__main__":  # pragma: no cover
    main()
