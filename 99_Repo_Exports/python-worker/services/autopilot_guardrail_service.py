from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

# -*- coding: utf-8 -*-
"""
Autopilot Guardrail:
  - Periodic performance check after overrides apply
  - Auto-rollback to prev_sid if degradation is statistically significant

Uses:
  - events:trades (POSITION_CLOSED) with r_mult/regime/scenario
  - LCB(mean R) with robust MAD sigma

Safety:
  - Redis lock SETNX to avoid concurrent runs
"""

import asyncio
import json
import math
import os
from typing import Any

import redis.asyncio as aioredis
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()


def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    m = n // 2
    if n % 2 == 1:
        return float(ys[m])
    return 0.5 * (float(ys[m - 1]) + float(ys[m]))


def _mad_sigma(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    med = _median(xs)
    dev = [abs(x - med) for x in xs]
    return 1.4826 * _median(dev)


def _lcb(xs: list[float], z: float) -> float:
    n = len(xs)
    if n <= 0:
        return float("-inf")
    mu = sum(xs) / float(n)
    sig = _mad_sigma(xs)
    if sig <= 1e-12:
        return mu
    return mu - float(z) * (sig / math.sqrt(float(n)))


class Guardrail:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = aioredis.from_url(self.redis_url, decode_responses=True)
        self.stream = os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)

        self.check_every_sec = int(os.getenv("GUARDRAIL_INTERVAL_SEC", "900"))  # 15m
        self.lock_key = os.getenv("GUARDRAIL_LOCK_KEY", "lock:autopilot:guardrail")
        self.lock_ttl = int(os.getenv("GUARDRAIL_LOCK_TTL_SEC", "840"))

        self.min_n = int(os.getenv("GUARDRAIL_MIN_N", "40"))
        self.z = float(os.getenv("GUARDRAIL_LCB_Z", "1.64"))
        self.rollback_floor = float(os.getenv("GUARDRAIL_ROLLBACK_FLOOR_R", "-0.15"))
        self.lookback_hours = float(os.getenv("GUARDRAIL_LOOKBACK_HOURS", "24"))

    async def _lock(self) -> bool:
        try:
            ok = await self.r.set(self.lock_key, str(_now_ms()), nx=True, ex=self.lock_ttl)
            return bool(ok)
        except Exception:
            return False

    async def _unlock(self) -> None:
        with contextlib.suppress(Exception):
            await self.r.delete(self.lock_key)

    async def _read_closed_since(self, since_ms: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        # reverse range from latest until older than since_ms
        last_id = "+"
        scanned = 0
        while scanned < 200000:
            batch = await self.r.xrevrange(self.stream, max=last_id, min="-", count=1000)
            if not batch:
                break
            if len(batch) == 1 and batch[0][0] == last_id:
                break
            for xid, f in batch:
                scanned += 1
                last_id = xid
                if str(f.get("event_type") or f.get("event") or "") != "POSITION_CLOSED":
                    continue
                ts = int(float(f.get("ts") or f.get("exit_ts_ms") or 0) or 0)
                if ts <= 0:
                    ts = int(str(xid).split("-")[0])
                if ts < since_ms:
                    return out
                with contextlib.suppress(Exception):
                    out.append({
                        "symbol": (f.get("symbol") or "").upper(),
                        "regime": (f.get("regime") or "na").lower(),
                        "scenario": str(f.get("scenario") or f.get("decision") or "").lower(),
                        "r_mult": float(f.get("r_mult") or f.get("r_multiple") or 0.0),
                    })
        return out

    async def run_once(self) -> None:
        if not await self._lock():
            return
        try:
            active_sid = str(await self.r.get("cfg:orderflow:overrides:v1:active_sid") or "")
            prev_sid = str(await self.r.get("cfg:orderflow:overrides:v1:prev_sid") or "")
            if not active_sid or not prev_sid:
                return
            applied_ts = int(float(await self.r.get(f"cfg:orderflow:overrides:v1:applied:{active_sid}") or 0) or 0)
            if applied_ts <= 0:
                return
            since = max(applied_ts, int(_now_ms() - self.lookback_hours * 3600 * 1000))
            rows = await self._read_closed_since(since)
            # aggregate per (symbol,regime,scenario)
            buckets: dict[tuple[str,str,str], list[float]] = {}
            for d in rows:
                scn = d["scenario"]
                if scn not in ("continuation","reversal"):
                    continue
                k = (d["symbol"], d["regime"], scn)
                buckets.setdefault(k, []).append(float(d["r_mult"]))
            # decision: if any key has enough n and LCB < floor => rollback
            bad = []
            for k, xs in buckets.items():
                if len(xs) < self.min_n:
                    continue
                lcb = _lcb(xs, self.z)
                if lcb < self.rollback_floor:
                    bad.append((k, len(xs), lcb))
            if bad:
                # rollback: set active_sid back to prev
                await self.r.set("cfg:orderflow:overrides:v1:active_sid", prev_sid)
                # optional: mark rollback
                await self.r.set(f"cfg:orderflow:overrides:v1:rollback:{active_sid}", json.dumps({"ts_ms": _now_ms(), "bad": bad}))
        finally:
            await self._unlock()

    async def run_forever(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(max(30, self.check_every_sec))


async def _main() -> None:
    await Guardrail().run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
