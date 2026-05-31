from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis  # type: ignore

from services.telegram.telegram_client import TelegramClient
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    return get_ny_time_millis()


def _j(s: str) -> dict[str, Any]:
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


@dataclass
class Stat:
    n: int = 0
    pnl_sum: float = 0.0
    wins: int = 0

    def add(self, pnl: float) -> None:
        self.n += 1
        self.pnl_sum += float(pnl)
        if pnl > 0:
            self.wins += 1

    @property
    def mean(self) -> float:
        return self.pnl_sum / self.n if self.n else 0.0

    @property
    def winrate(self) -> float:
        return self.wins / self.n if self.n else 0.0


def _is_thin(regime: str) -> bool:
    rg = (regime or "na").lower()
    return rg in ("thin", "news", "illiquid")


class WinnerSuggester:
    """
    Reads closed positions and suggests best arm per regime-group.
    Input stream must include: type=CLOSE or position_closed, containing pnl, ab_arm, regime
    """

    def __init__(self, r: aioredis.Redis) -> None:
        self.r = r
        self.stream = os.getenv("AB_EVENTS_STREAM", RS.EVENTS_TRADES) # Usually stream:trade:events
        self.group = os.getenv("AB_EVENTS_GROUP", "ab_winner")
        self.consumer = os.getenv("AB_EVENTS_CONSUMER", f"ab_winner:{os.getpid()}")
        self.out_key_prefix = os.getenv("AB_WINNER_KEY_PREFIX", "ab:winner:v1:")
        self.out_stream = os.getenv("AB_SUGGEST_STREAM", RS.AB_SUGGESTIONS)
        self.min_n = int(os.getenv("AB_MIN_N", "30"))
        self.window_ms = int(os.getenv("AB_WINDOW_MS", str(6 * 60 * 60 * 1000)))
        self.every_ms = int(os.getenv("AB_SUGGEST_EVERY_MS", str(15 * 60 * 1000))) # 15 min default
        self.last_emit_ms = 0
        self.stats: defaultdict[str, defaultdict[str, Stat]] = defaultdict(lambda: defaultdict(Stat))

        # Telegram
        self.tg = TelegramClient.from_env()

    async def _ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                return

    def _reg_group(self, regime: str) -> str:
        return "thin" if _is_thin(regime) else "default"

    def _pick(self, reg_group: str) -> str:
        best_arm = "A"
        # Tuple comparison: (Winrate, Mean, N) - prioritize Winrate then Mean
        best_key = (-1.0, -1e18, -1)

        # Check A, B, C if they exist
        candidates = []
        for arm, st in self.stats[reg_group].items():
            if st.n < self.min_n:
                continue
            candidates.append(arm)
            # Strategy: Maximize Winrate first, then Mean PNL
            key = (st.winrate, st.mean, st.n)
            if key > best_key:
                best_key = key
                best_arm = arm

        # If A is significantly better or close, prefer A?
        # For now, simple strict winner.
        return best_arm

    async def _publish(self, reg_group: str, winner: str) -> None:
        ts = _now_ms()
        # write key
        key = f"{self.out_key_prefix}{reg_group}"
        st = {arm: {"n": s.n, "mean": float(f"{s.mean:.4f}"), "winrate": float(f"{s.winrate:.4f}")} for arm, s in self.stats[reg_group].items()}

        with contextlib.suppress(Exception):
            await self.r.hset(key, mapping={"arm": winner, "ts_ms": str(ts), "stats": json.dumps(st, separators=(",", ":"), ensure_ascii=False)})

        # write stream
        payload = {"ts_ms": ts, "reg_group": reg_group, "winner": winner, "stats": st}
        with contextlib.suppress(Exception):
            await self.r.xadd(self.out_stream, {"payload": json.dumps(payload, separators=(",", ":"), ensure_ascii=False)}, maxlen=20000, approximate=True)

        # Notify Telegram
        if self.tg:
            msg = [
                f"<b>AB Winner Suggestion</b> ({reg_group})",
                f"Winner: <b>{winner}</b>",
                "",
                "Stats:",
            ]
            for arm in sorted(st.keys()):
                d = st[arm]
                msg.append(f" {arm}: WR={d['winrate']*100:.1f}% Avg={d['mean']:.2f} (N={d['n']})")

            self.tg.send_text("\n".join(msg))

    async def run_forever(self) -> None:
        await self._ensure_group()
        while True:
            msgs = None
            try:
                msgs = await self.r.xreadgroup(self.group, self.consumer, {self.stream: ">"}, count=200, block=1000)
            except Exception:
                await asyncio.sleep(0.2)
                continue
            if not msgs:
                await asyncio.sleep(0.1)
                continue

            now = _now_ms()
            for _s, entries in msgs:
                for msg_id, fields in entries:
                    try:
                        # Event parsing logic
                        event_type = (fields.get("type") or "").lower()
                        d = {}

                        # 1. Payload usually contains the data
                        if "payload" in fields:
                            d = _j(fields["payload"])
                        elif "data" in fields:
                             d = _j(fields["data"])
                        else:
                            d = dict(fields)

                        # 2. Check type (CLOSE or position_closed)
                        if event_type not in ("close", "position_closed"):
                            # handlers.py emits "CLOSE", legacy might emit "position_closed"
                            continue

                        # 3. Extract Meta (ab_arm, regime)
                        # Could be in 'meta' field (json) or in 'payload' itself
                        ab_arm = d.get("ab_arm")
                        regime = d.get("regime")

                        if not ab_arm and "meta" in fields:
                            meta = _j(fields["meta"])
                            ab_arm = meta.get("ab_arm")
                            regime = meta.get("regime")

                        # If still missing, check entries in d
                        if not ab_arm:
                             ab_arm = d.get("ab_arm")
                        if not regime:
                             regime = d.get("regime") or d.get("entry_regime")

                        # 4. Check Window
                        ts_ms = int(fields.get("ts_ms") or d.get("ts_ms") or d.get("exit_ts_ms") or now)
                        if now - ts_ms > self.window_ms:
                            continue

                        # 5. Extract PnL
                        pnl = float(d.get("pnl_net") or d.get("pnl") or d.get("pnl_usd") or 0.0)

                        arm = (ab_arm or "A").upper()
                        regime = (regime or "na").lower()
                        grp = self._reg_group(regime)

                        self.stats[grp][arm].add(pnl)
                    except Exception:
                        pass
                    finally:
                        with contextlib.suppress(Exception):
                            await self.r.xack(self.stream, self.group, msg_id)

            if self.last_emit_ms == 0 or (now - self.last_emit_ms) >= self.every_ms:
                self.last_emit_ms = now
                for grp in ("default", "thin"):
                    if any(st.n >= self.min_n for st in self.stats[grp].values()):
                        w = self._pick(grp)
                        await self._publish(grp, w)


async def _main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True) # type: ignore
    try:
        await WinnerSuggester(r).run_forever()
    finally:
        with contextlib.suppress(Exception):
            await r.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
