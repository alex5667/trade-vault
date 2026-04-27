from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Exec slippage eval rowcount probe (P77).

Purpose
  - Query `v_exec_slippage_eval` rowcount by `exec_regime_bucket` for last 24h.
  - Store results into Redis state keys so exporter + alerts can detect staleness.

Redis keys
  - state:exec_slippage_eval:rows_24h_ts_ms
  - state:exec_slippage_eval:rows_24h (hash: bucket->n, total->n)
  - state:exec_slippage_eval:probe_last_rc
  - state:exec_slippage_eval:probe_last_msg

Exit codes
  - 0: OK
  - 2: soft issue (low rows)
  - 1: hard fail (DB/Redis error)
"""

import asyncio
import os
import time
from typing import Dict, List, Tuple

import asyncpg


def _buckets() -> List[str]:
    raw = os.getenv("ENFORCE_STATE_EXPORTER_BUCKETS", "NORMAL,LOW_LIQ,HIGH_VOL,HIGH_VOL_LOW_LIQ")
    xs: List[str] = []
    for p in str(raw).replace(";", ",").split(","):
        s = p.strip().upper()
        if s and s not in xs:
            xs.append(s)
    return xs or ["NORMAL", "LOW_LIQ", "HIGH_VOL", "HIGH_VOL_LOW_LIQ"]


def _db_url() -> str:
    return (
        (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or os.getenv("ANALYTICS_DB_DSN")
        or "postgresql://trading:trading@scanner-postgres:5432/scanner_analytics"
    )


def _redis_url() -> str:
    return os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or "redis://redis-worker-1:6379/0"


async def _connect_redis():
    try:
        import redis.asyncio as aioredis  # type: ignore

        return aioredis.Redis.from_url(_redis_url(), decode_responses=True)
    except Exception:
        return None


async def run() -> Tuple[int, str]:
    r = await _connect_redis()
    if r is None:
        return 1, "redis.asyncio unavailable"

    now_ms = get_ny_time_millis()
    buckets = _buckets()

    min_total = int(os.getenv("EXEC_SLIP_EVAL_PROBE_MIN_TOTAL_24H", "30") or "30")
    min_hvll = int(os.getenv("EXEC_SLIP_EVAL_PROBE_MIN_HVLL_24H", "5") or "5")

    conn = None
    try:
        conn = await asyncpg.connect(_db_url())
        rows = await conn.fetch(
            """
            select exec_regime_bucket, count(*)::bigint as n
            from v_exec_slippage_eval
            where ts >= now() - interval '24 hours'
            group by 1
            """
        )
    except Exception as e:
        try:
            await r.set("state:exec_slippage_eval:probe_last_rc", "1")
            await r.set("state:exec_slippage_eval:probe_last_msg", f"db_error:{e}"[:500])
        except Exception:
            pass
        try:
            await r.aclose()
        except Exception:
            pass
        if conn is not None:
            try:
                await conn.close()
            except Exception:
                pass
        return 1, f"db_error:{e}"

    counts: Dict[str, int] = {b: 0 for b in buckets}
    total = 0
    for row in rows:
        b = str(row.get("exec_regime_bucket") or "NORMAL").strip().upper() or "NORMAL"
        n = int(row.get("n") or 0)
        if b not in counts:
            counts[b] = 0
        counts[b] += n
        total += n

    # Persist state for exporter
    try:
        await r.set("state:exec_slippage_eval:rows_24h_ts_ms", str(now_ms))
        h = {"total": str(total)}
        for b in buckets:
            h[b] = str(int(counts.get(b, 0)))
        await r.hset("state:exec_slippage_eval:rows_24h", mapping=h)
    except Exception:
        pass

    # Decide rc for orchestration notifications
    rc = 0
    msg = f"rows_24h_total={total} " + " ".join([f"{b}={counts.get(b,0)}" for b in buckets])
    if total < min_total:
        rc = 2
        msg = f"LOW_TOTAL(min={min_total}) " + msg
    if int(counts.get("HIGH_VOL_LOW_LIQ", 0)) < min_hvll:
        # HVLL can be rare; keep as soft signal only
        rc = max(rc, 2)
        msg = f"LOW_HVLL(min={min_hvll}) " + msg

    try:
        await r.set("state:exec_slippage_eval:probe_last_rc", str(rc))
        await r.set("state:exec_slippage_eval:probe_last_msg", msg[:500])
    except Exception:
        pass

    try:
        await r.aclose()
    except Exception:
        pass
    try:
        await conn.close()
    except Exception:
        pass

    return rc, msg


def main() -> int:
    rc, msg = asyncio.run(run())
    print({"rc": rc, "msg": msg})
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
