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

# Try import TelegramClient, safe fallback
try:
    from services.telegram.telegram_client import TelegramClient
except ImportError:
    TelegramClient = None


@dataclass
class ArmStats:
    n: int = 0
    wins: int = 0
    pnl_sum: float = 0.0
    pnl_sq_sum: float = 0.0

    def add(self, pnl: float) -> None:
        self.n += 1
        if pnl > 0:
            self.wins += 1
        self.pnl_sum += float(pnl)
        self.pnl_sq_sum += float(pnl) * float(pnl)

    @property
    def mean(self) -> float:
        return self.pnl_sum / self.n if self.n > 0 else 0.0

    @property
    def winrate(self) -> float:
        return float(self.wins) / float(self.n) if self.n > 0 else 0.0


def _now_ms() -> int:
    return get_ny_time_millis()


def _json_load(s: str) -> dict[str, Any]:
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


class ABSuggester:
    """
    Periodically computes winner-per-regime using closed-trade events.

    INPUT (stream):
      AB_EVENTS_STREAM (default: position:events)
      Expected event payload contains:
        - type: "position_closed"
        - sid / signal_id
        - pnl
        - ab_arm (A/B/C)
        - regime (optional)

    OUTPUT:
      HSET ab:winner:v1:{regime} arm,ts_ms,stats_json
      XADD stream:ab:suggestions payload
      Telegram Notification (if configured)
    """

    def __init__(self, r: aioredis.Redis) -> None:
        self.r = r
        self.stream = os.getenv("AB_EVENTS_STREAM", RS.EVENTS_TRADES)
        self.group = os.getenv("AB_EVENTS_GROUP", "ab_suggester")
        self.consumer = os.getenv("AB_EVENTS_CONSUMER", f"ab_suggester:{os.getpid()}")
        self.out_stream = os.getenv("AB_SUGGEST_STREAM", "stream:ab:suggestions")
        self.key_prefix = os.getenv("AB_WINNER_KEY_PREFIX", "ab:winner:v1:")
        self.window_ms = int(os.getenv("AB_WINDOW_MS", str(6 * 60 * 60 * 1000)))  # 6h
        self.min_n = int(os.getenv("AB_MIN_N", "30"))
        self.sleep_no_msgs = float(os.getenv("AB_IDLE_SLEEP_SEC", "0.25"))

        # in-memory: regime -> arm -> stats
        self.stats: dict[str, dict[str, ArmStats]] = {}
        self.last_ts_ms: int = 0

        # Telegram Setup
        self.tg: Any | None = None
        if TelegramClient:
            # Try standard env vars first
            self.tg = TelegramClient.from_env()
            # If not set, try common aliases from this project (BOT_TOKEN / CHAT_ID)
            if not self.tg:
                bt = os.getenv("BOT_TOKEN", "").strip()
                ci = os.getenv("CHAT_ID", "").strip()
                if bt and ci:
                    self.tg = TelegramClient(token=bt, chat_id=ci)

    async def _ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                return
            # fail-open
            return

    def _stats_for(self, regime: str, arm: str) -> ArmStats:
        reg = (regime or "na").lower()
        a = (arm or "A").upper()
        self.stats.setdefault(reg, {})
        self.stats[reg].setdefault(a, ArmStats())
        return self.stats[reg][a]

    def _pick_winner(self, regime: str) -> str | None:
        reg = (regime or "na").lower()
        arms = self.stats.get(reg) or {}
        # winner by mean pnl, tie-break by winrate then n
        best = None
        best_key = (-1e18, -1e18, -1)
        for arm, st in arms.items():
            if st.n < self.min_n:
                continue
            key = (st.mean, st.winrate, st.n)
            if key > best_key:
                best_key = key
                best = arm
        return best

    async def _publish_suggestion(self, regime: str, winner: str) -> None:
        reg = (regime or "na").lower()
        arms = self.stats.get(reg) or {}
        stats_json = {
            arm: {"n": st.n, "mean": st.mean, "winrate": st.winrate}
            for arm, st in sorted(arms.items())
        }
        key = f"{self.key_prefix}{reg}"
        ts_ms = _now_ms()
        with contextlib.suppress(Exception):
            await self.r.hset(
                key,
                mapping={
                    "arm": str(winner),
                    "ts_ms": str(ts_ms),
                    "stats": json.dumps(stats_json, ensure_ascii=False, separators=(",", ":")),
                }
            )
        # stream suggestion
        payload = {
            "ts_ms": ts_ms,
            "regime": reg,
            "winner": winner,
            "stats": stats_json,
            "window_ms": self.window_ms,
            "min_n": self.min_n,
        }
        with contextlib.suppress(Exception):
            await self.r.xadd(
                self.out_stream,
                fields={"payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
                maxlen=20000,
                approximate=True,
            )

        # Notify Telegram
        if self.tg:
            # Prepare message
            msg = f"🏆 <b>AB Winner Suggestion</b>\nRegime: <code>{reg}</code>\nWinner: <b>{winner}</b>\n\nStats (last {self.window_ms // 3600000}h):\n"
            for arm, st in sorted(arms.items()):
                marker = "✅" if arm == winner else "  "
                msg += f"{marker} <b>{arm}</b>: n={st.n} μ={st.mean:.2f} WR={int(st.winrate*100)}%\n"

            # Send (non-blocking via executor)
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.tg.send_text, msg)
            except Exception:
                pass

    async def run_forever(self) -> None:
        await self._ensure_group()
        while True:
            try:
                msgs = await self.r.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer,
                    streams={self.stream: ">"},
                    count=200,
                    block=1000,
                )
            except Exception:
                await asyncio.sleep(self.sleep_no_msgs)
                continue

            if not msgs:
                await asyncio.sleep(self.sleep_no_msgs)
                continue

            now_ms = _now_ms()
            for stream_name, entries in msgs:
                for msg_id, fields in entries:
                    try:
                        # support both direct fields and nested JSON payload
                        d: dict[str, Any] = {}
                        if "payload" in fields:
                            d = _json_load(fields.get("payload") or "")
                        else:
                            d = dict(fields)
                        if (d.get("type") or "") != "position_closed":
                            continue
                        pnl = float(d.get("pnl") or 0.0)
                        arm = str(d.get("ab_arm") or d.get("arm") or "A").upper()
                        regime = (d.get("regime") or "na").lower()
                        ts_ms = int(d.get("ts_ms") or d.get("close_ts") or now_ms)

                        # windowed acceptance (drop too old)
                        if now_ms - ts_ms > self.window_ms:
                            continue

                        self._stats_for(regime, arm).add(pnl)
                    finally:
                        with contextlib.suppress(Exception):
                            await self.r.xack(self.stream, self.group, msg_id)

            # periodically suggest winners (simple throttle)
            if self.last_ts_ms == 0 or (now_ms - self.last_ts_ms) >= int(os.getenv("AB_SUGGEST_EVERY_MS", "300000")):
                self.last_ts_ms = now_ms
                for reg in list(self.stats.keys()):
                    w = self._pick_winner(reg)
                    if w:
                        await self._publish_suggestion(reg, w)


async def _main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True)
    try:
        s = ABSuggester(r)
        await s.run_forever()
    finally:
        with contextlib.suppress(Exception):
            await r.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
