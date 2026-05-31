from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x or d)
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _price_key(symbol: str) -> str:
    """Keep consistent with your ingest: can be overridden."""
    fmt = os.getenv("AB_PRICE_KEY_FMT", "price:latest:{symbol}")
    return fmt.format(symbol=symbol)


async def _read_mid_ts(r: Any, symbol: str) -> tuple[float, int]:
    """
    Best-effort read of latest price.
    Supports:
      - JSON at price:latest:{symbol} with fields mid/price + ts/ts_ms
      - plain float (mid) if stored directly
    """
    key = _price_key(symbol)
    raw = await r.get(key)
    if not raw:
        return 0.0, 0
    # try json
    try:
        d = json.loads(raw)
        mid = _f(d.get("mid") or d.get("price") or d.get("last") or 0.0, 0.0)
        ts = _i(d.get("ts_ms") or d.get("ts") or 0, 0)
        return mid, ts
    except Exception:
        pass
    # try plain float
    try:
        return float(raw), 0
    except Exception:
        return 0.0, 0


@dataclass
class Pending:
    """Pending entry for proxy-outcome evaluation."""
    symbol: str
    side: str
    arm: str
    group: str
    ts_ms: int
    entry_px: float
    # computed outcomes
    done_60s: int = 0
    done_300s: int = 0


@dataclass
class Agg:
    """Aggregator for arm performance metrics."""
    n: int = 0
    win: int = 0
    sum_ret_bps: float = 0.0

    def add(self, ret_bps: float) -> None:
        self.n += 1
        if ret_bps > 0:
            self.win += 1
        self.sum_ret_bps += float(ret_bps)

    def snapshot(self) -> dict[str, Any]:
        n = max(1, int(self.n))
        return {
            "n": int(self.n),
            "winrate": float(self.win) / float(n),
            "avg_ret_bps": float(self.sum_ret_bps) / float(n),
        }


class EntryPolicyABReportService:
    """
    "Winner per regime" on *proxy outcomes* for both execute and shadow arms.

    Input:
      - stream:trade:entry_audit (entry_policy_audit)
        We only use ok=1 audits (ALLOW or ALLOW_SHADOW), extract:
          symbol, side, arm, arm_ver, regime(group), snap.close_px (entry px proxy), ts_ms
    Outcome:
      - forward return after 60s and 300s using price:latest:{symbol}.
    Output:
      - Redis key: ab:report:entry_policy:v1:{group} (json, TTL)
      - Redis suggestion: cfg:suggestions:entry_policy:ab_winner:v1:{group} (json)
    """

    def __init__(self, *, redis_url: str) -> None:
        self.r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30)
        self.audit_stream = os.getenv("AB_AUDIT_STREAM", RS.ENTRY_AUDIT)
        self.group = os.getenv("AB_AUDIT_GROUP", "ab-report")
        self.consumer = os.getenv("AB_AUDIT_CONSUMER", f"ab-report-{os.getpid()}")
        self.block_ms = int(os.getenv("AB_READ_BLOCK_MS", "1000"))
        self.read_count = int(os.getenv("AB_READ_COUNT", "200"))
        self.report_every_sec = int(os.getenv("AB_REPORT_EVERY_SEC", "60"))
        self.ttl_sec = int(os.getenv("AB_REPORT_TTL_SEC", "86400"))
        self.min_n = int(os.getenv("AB_MIN_N_PER_ARM", "25"))

        self.pending: dict[str, Pending] = {}
        self.agg_60: dict[tuple[str, str, str], Agg] = {}   # (group, arm, horizon)
        self.agg_300: dict[tuple[str, str, str], Agg] = {}
        self._last_report_ms: int = 0

    async def _ensure_group(self) -> None:
        with contextlib.suppress(Exception):
            await self.r.xgroup_create(self.audit_stream, self.group, id="0", mkstream=True)

    def _pid(self, p: dict[str, Any]) -> str:
        """Stable unique id for pending map."""
        return f'{p.get("symbol","")}:{p.get("side","")}:{p.get("arm","")}:{p.get("ts_ms",0)}'

    async def _consume_audit(self) -> None:
        msgs = await self.r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.audit_stream: ">"},
            count=self.read_count,
            block=self.block_ms,
        )
        if not msgs:
            return
        for _stream, entries in msgs:
            for msg_id, fields in entries:
                try:
                    if (fields.get("type", "")) != "entry_policy_audit":
                        await self.r.xack(self.audit_stream, self.group, msg_id)
                        continue
                    ok = int(fields.get("ok", 0) or 0)
                    if ok != 1:
                        await self.r.xack(self.audit_stream, self.group, msg_id)
                        continue
                    arm = _s(fields.get("arm", "A")).upper()
                    payload = {}
                    try:
                        payload = json.loads(fields.get("payload") or "{}")
                    except Exception:
                        payload = {}
                    symbol = _s(fields.get("symbol") or payload.get("symbol") or "")
                    side = _s(fields.get("side") or payload.get("side") or "").upper()
                    ts_ms = _i(fields.get("ts_ms") or payload.get("ts_ms") or payload.get("setup_ts_ms") or 0, 0)
                    rg = _s(payload.get("regime", "na")).lower()
                    group = "thin" if rg in ("thin","news","illiquid") else "default"
                    snap = payload.get("snap") or {}
                    entry_px = _f(snap.get("close_px") or payload.get("snap", {}).get("close_px") or 0.0, 0.0)
                    if not symbol or entry_px <= 0 or ts_ms <= 0 or side not in ("LONG","SHORT"):
                        await self.r.xack(self.audit_stream, self.group, msg_id)
                        continue
                    p = Pending(symbol=symbol, side=side, arm=arm, group=group, ts_ms=ts_ms, entry_px=entry_px)
                    pid = self._pid({"symbol":symbol,"side":side,"arm":arm,"ts_ms":ts_ms})
                    self.pending[pid] = p
                finally:
                    with contextlib.suppress(Exception):
                        await self.r.xack(self.audit_stream, self.group, msg_id)

    def _ret_bps(self, *, entry_px: float, now_px: float, side: str) -> float:
        """Calculate forward return in basis points."""
        if entry_px <= 0 or now_px <= 0:
            return 0.0
        mid = 0.5 * (abs(entry_px) + abs(now_px))
        bps = 10000.0 * (now_px - entry_px) / mid if mid > 0 else 0.0
        return float(bps if side == "LONG" else -bps)

    async def _eval_pending(self) -> None:
        now = _now_ms()
        done_ids = []
        for pid, p in list(self.pending.items()):
            mid, _ts = await _read_mid_ts(self.r, p.symbol)
            if mid <= 0:
                continue
            age = now - int(p.ts_ms)
            # 60s horizon
            if p.done_60s == 0 and age >= 60_000:
                ret = self._ret_bps(entry_px=p.entry_px, now_px=mid, side=p.side)
                k = (p.group, p.arm, "h60")
                self.agg_60.setdefault(k, Agg()).add(ret)
                p.done_60s = 1
            # 300s horizon
            if p.done_300s == 0 and age >= 300_000:
                ret = self._ret_bps(entry_px=p.entry_px, now_px=mid, side=p.side)
                k = (p.group, p.arm, "h300")
                self.agg_300.setdefault(k, Agg()).add(ret)
                p.done_300s = 1
            if p.done_60s and p.done_300s:
                done_ids.append(pid)
        for pid in done_ids:
            self.pending.pop(pid, None)

    def _pick_winner(self, group: str) -> dict[str, Any]:
        """
        Winner per group is selected by:
          - prioritize h300 avg_ret_bps
          - require min_n
          - tie-break by winrate
        """
        best = {"arm": "A", "reason": "fallback", "metrics": {}}
        # gather per arm metrics
        arms = ["A","B","C"]
        cand = []
        for arm in arms:
            k = (group, arm, "h300")
            ag = self.agg_300.get(k)
            if ag is None or ag.n < self.min_n:
                continue
            m = ag.snapshot()
            cand.append((arm, m["avg_ret_bps"], m["winrate"], m))
        if not cand:
            return best
        cand.sort(key=lambda x: (x[1], x[2]), reverse=True)
        arm, avg, wr, m = cand[0]
        return {"arm": arm, "reason": "max_avg_ret_h300", "metrics": m}

    async def _write_reports(self) -> None:
        now = _now_ms()
        if now - self._last_report_ms < self.report_every_sec * 1000:
            return
        self._last_report_ms = now

        for group in ("default","thin"):
            rep = {"ts_ms": now, "group": group, "h60": {}, "h300": {}, "pending": len(self.pending)}
            for arm in ("A","B","C"):
                rep["h60"][arm] = self.agg_60.get((group, arm, "h60"), Agg()).snapshot()
                rep["h300"][arm] = self.agg_300.get((group, arm, "h300"), Agg()).snapshot()
            key = f"ab:report:entry_policy:v1:{group}"
            await self.r.set(key, json.dumps(rep, ensure_ascii=False, separators=(",", ":")), ex=self.ttl_sec)

            win = self._pick_winner(group)
            sug = {
                "ts_ms": now,
                "group": group,
                "winner_arm": win["arm"],
                "reason": win["reason"],
                "metrics": win["metrics"],
                "min_n": self.min_n,
                "note": "Proxy winner via forward-return (60s/300s) on price:latest. Approve manually.",
            }
            sk = f"cfg:suggestions:entry_policy:ab_winner:v1:{group}"
            await self.r.set(sk, json.dumps(sug, ensure_ascii=False, separators=(",", ":")), ex=self.ttl_sec)

    async def run_forever(self) -> None:
        await self._ensure_group()
        while True:
            try:
                await self._consume_audit()
                await self._eval_pending()
                await self._write_reports()
            except Exception:
                # fail-open: keep running
                pass
            await asyncio.sleep(0.2)


async def _async_main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    svc = EntryPolicyABReportService(redis_url=redis_url)
    await svc.run_forever()


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
