from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

import redis.asyncio as aioredis


async def capture_range(
    r: aioredis.Redis,
    stream: str,
    start_id: str,
    end_id: str,
    count: int = 2000,
) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    cur = start_id
    while True:
        rows = await r.xrange(stream, min=cur, max=end_id, count=count)
        if not rows:
            break
        for msg_id, fields in rows:
            out.append((msg_id, dict(fields)))
        last_id = rows[-1][0]
        # move forward
        if last_id == cur:
            break
        cur = last_id
        # XRANGE is inclusive; shift to next by appending "-0"? simplest: add suffix
        # We'll rely on uniqueness + dedup on write.
        if len(rows) < count:
            break
    return out


def ndjson_dump(path: str, rows: list[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--stream", action="append", required=True, help="repeatable, e.g. --stream signals:of:inputs")
    ap.add_argument("--start", required=True, help="redis stream id, e.g. 0-0 or 1700000000000-0")
    ap.add_argument("--end", required=True, help="redis stream id, e.g. + or 1700009999999-0")
    ap.add_argument("--out", required=True, help="output prefix, e.g. /tmp/ofcap")
    args = ap.parse_args()

    r = aioredis.from_url(args.dsn, decode_responses=True, socket_connect_timeout=10, socket_timeout=30)

    for s in args.stream:
        rows = await capture_range(r, s, args.start, args.end)
        out = []
        seen = set()
        for msg_id, fields in rows:
            if msg_id in seen:
                continue
            seen.add(msg_id)
            payload = fields.get("payload") or fields.get("data") or ""
            out.append({"stream": s, "id": msg_id, "payload": payload})
        ndjson_dump(f"{args.out}.{s.replace(':','_')}.ndjson", out)

    await r.close()


if __name__ == "__main__":
    asyncio.run(main())
