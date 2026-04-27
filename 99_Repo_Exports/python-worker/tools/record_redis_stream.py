from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""
CLI: запись Redis stream в JSONL (N минут).

Пример:
  python -m tools.record_redis_stream \
    --redis redis://localhost:6379/0 \
    --stream binance:ticks:BTCUSDT \
    --minutes 3 \
    --out /tmp/replay_ticks.jsonl

Важно:
  - записываем "как есть": id, fields, время записи.
  - decode bytes -> str
"""

import argparse
import time
from typing import Any, Dict, List, Tuple

from replay.jsonl import JsonlWriter


def _decode(x: Any) -> Any:
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", errors="replace")
        except Exception:
            return str(x)
    return x


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", required=True, help="redis://host:port/db")
    ap.add_argument("--stream", required=True, help="Redis stream name")
    ap.add_argument("--minutes", type=int, default=1)
    ap.add_argument("--out", required=True, help="Output JSONL file")
    ap.add_argument("--start_id", default="$", help="XREAD start id, default '$' (new messages)")
    ap.add_argument("--block_ms", type=int, default=2000)
    ap.add_argument("--count", type=int, default=200)
    args = ap.parse_args()

    import redis  # redis-py

    r = redis.Redis.from_url(args.redis)
    w = JsonlWriter(args.out)

    end_ts = time.time() + max(1, int(args.minutes)) * 60
    last_id = str(args.start_id)
    n = 0
    try:
        while time.time() < end_ts:
            resp = r.xread({args.stream: last_id}, count=int(args.count), block=int(args.block_ms))
            if not resp:
                continue
            # resp: [(stream, [(id, {field:val})...])]
            for _stream, items in resp:
                for xid, fields in items:
                    xid_s = _decode(xid)
                    d: Dict[str, Any] = {}
                    for k, v in (fields or {}).items():
                        d[str(_decode(k))] = _decode(v)
                    w.write(
                        {
                            "type": "tick",
                            "ts_ms": get_ny_time_millis(),
                            "redis_stream": str(_decode(_stream)),
                            "redis_id": str(xid_s),
                            "payload": d,
                        }
                    )
                    last_id = str(xid_s)
                    n += 1
    finally:
        w.close()

    print(f"recorded={n} out={args.out}")


if __name__ == "__main__":
    main()
