from __future__ import annotations

import asyncio
import json
import os

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    return get_ny_time_millis()


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)

    in_stream = os.getenv("SMT_ENTRY_STREAM", RS.ENTRY_CANDIDATE)
    snap_prefix = os.getenv("SMT_SNAP_PREFIX", "smt:snap:")
    bundle_prefix = os.getenv("SMT_BUNDLE_PREFIX", "smt:bundle:v1:")

    out_path = os.getenv("OUT", "entry_policy_inputs.ndjson")
    duration_sec = int(os.getenv("DURATION_SEC", "900"))  # 10-20 min typical
    start_id = os.getenv("START_ID", "$")  # "$" -> new only, "0" -> from beginning
    block_ms = int(os.getenv("BLOCK_MS", "1000"))
    count = int(os.getenv("COUNT", "200"))

    t_end = _now_ms() + duration_sec * 1000
    cur = start_id

    with open(out_path, "w", encoding="utf-8") as f:
        while _now_ms() < t_end:
            try:
                msgs = await r.xread({in_stream: cur}, count=count, block=block_ms)
            except Exception:
                await asyncio.sleep(0.2)
                continue
            if not msgs:
                continue
            for _stream, entries in msgs:
                for msg_id, fields in entries:
                    cur = msg_id
                    try:
                        if (fields.get("type", "")) != "entry_candidate":
                            continue
                        sym = (fields.get("symbol", "") or "").upper()
                        bundle = (fields.get("bundle", "") or "")
                        snap_raw = await r.get(f"{snap_prefix}{sym}")
                        snap = json.loads(snap_raw) if snap_raw else {}
                        bstate = await r.hgetall(f"{bundle_prefix}{bundle}") if bundle else {}
                        rec = {
                            "msg_id": msg_id,
                            "captured_ts_ms": _now_ms(),
                            "cand": fields,
                            "snap": snap,
                            "bundle": bstate,
                        }
                        f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
                    except Exception:
                        continue


if __name__ == "__main__":
    asyncio.run(main())
