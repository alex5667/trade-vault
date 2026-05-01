from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List

import redis.asyncio as aioredis


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _safe_str(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _hash_payload(s: str) -> str:
    # lightweight stable hash without extra deps
    import hashlib
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


@dataclass
class PubCfg:
    symbols: List[str]
    interval_sec: float
    ttl_sec: int
    key_prefix: str
    state_hash_prefix: str
    min_change_publish_sec: float

    @staticmethod
    def from_env() -> "PubCfg":
        syms = [s.strip().upper() for s in (os.getenv("SYMBOLS", "") or "").split(",") if s.strip()]
        # allow alternative env
        if not syms:
            syms = [s.strip().upper() for s in (os.getenv("FUTURES_SYMBOLS", "") or "").split(",") if s.strip()]
        return PubCfg(
            symbols=syms,
            interval_sec=float(os.getenv("HTF_ZONES_PUB_INTERVAL_SEC", "10")),
            ttl_sec=int(os.getenv("HTF_ZONES_TTL_SEC", "120")),
            key_prefix=str(os.getenv("HTF_ZONES_KEY_PREFIX", "zones:htf:v1:")),
            state_hash_prefix=str(os.getenv("HTF_ZONES_HASH_PREFIX", "zones:htf:hash:v1:")),
            min_change_publish_sec=float(os.getenv("HTF_ZONES_MIN_CHANGE_PUBLISH_SEC", "2")),
        )


class HTFZonesPublisher:
    """
    Central publisher: writes zones:htf:v1:<symbol> JSON for all symbols.

    Designed to run as a separate worker (recommended), because:
      - base_handler_legacy is deprecated
      - multiple consumers need consistent zones (SMT / OF / UI)

    Inputs:
      - HTFLevelsService (or equivalent) that can compute HTF levels for a symbol at ts_event_ms
    """

    def __init__(self, *, redis_url: str) -> None:
        self.cfg = PubCfg.from_env()
        self.r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)

        # Lazy import to avoid hard crash if module paths differ; will fail with clear log.
        self._htf = None
        self._init_htf_service()

        # last publish guard
        self._last_pub_ts_ms: Dict[str, int] = {}

    def _init_htf_service(self) -> None:
        """
        Attempt to initialize HTFLevelsService with Redis provider.
        """
        try:
            from geometry.htf_levels import HTFLevelsService
            # Try to import core provider
            try:
                from core.htf_levels import RedisHTFLevelsProvider
                import redis
                # Synchronous client for the provider
                sync_r = redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
                provider = RedisHTFLevelsProvider(sync_r)
                self._htf = HTFLevelsService(htf_provider=provider)
            except Exception:
                 # Fallback to no provider
                 self._htf = HTFLevelsService()
            return
        except Exception:
            self._htf = None

    def _levels_to_zones(self, levels: List[Any], symbol: str, ts_ms: int) -> Dict[str, Any]:
        zones = []
        for lv in levels:
            try:
                ltype = _safe_str(getattr(lv, "level_type", None) or getattr(lv, "type", None) or getattr(lv, "kind", None), "NA")
                px = _safe_float(getattr(lv, "price", None) or getattr(lv, "px", None) or getattr(lv, "value", None), 0.0)
                w = _safe_float(getattr(lv, "weight", None) or 1.0, 1.0)
                if px <= 0:
                    continue
                src = "na"
                side = "NA"
                zid = ltype
                lt = ltype.upper()
                if "DAILY" in lt:
                    src = "daily"
                elif "WEEK" in lt:
                    src = "weekly"
                elif "ASIA" in lt or "EUROPE" in lt or "SESSION" in lt:
                    src = "session"
                # side inference
                if "HIGH" in lt:
                    side = "RES"
                elif "LOW" in lt:
                    side = "SUP"
                elif "OPEN" in lt:
                    side = "MID"
                zones.append(
                    {
                        "id": zid,
                        "type": "LEVEL",
                        "src": src,
                        "side": side,
                        "px_lo": float(px),
                        "px_hi": float(px),
                        "ts_ms": int(ts_ms),
                        "weight": float(w),
                    }
                )
            except Exception:
                continue

        out = {"v": 1, "symbol": symbol, "ts_ms": int(ts_ms), "zones": zones}
        return out

    async def _compute_levels(self, symbol: str, ts_ms: int) -> List[Any]:
        """
        Compute HTF levels using HTFLevelsService.
        """
        if self._htf is None:
            return []
        try:
            # 1. Try direct get_levels (if available)
            if hasattr(self._htf, "get_levels"):
                return list(self._htf.get_levels(symbol, ts_ms))
            
            # 2. Fallback to get_geometry (dummy price 0.0) -> levels
            if hasattr(self._htf, "get_geometry"):
                # some implementations require price
                try:
                   geo = self._htf.get_geometry(symbol, ts_ms, 0.0)
                except TypeError:
                   # maybe it doesn't take price?
                   geo = self._htf.get_geometry(symbol, ts_ms)
                
                if geo and hasattr(geo, "levels"):
                    return list(geo.levels)
        except Exception:
            return []
        return []

    async def tick_once(self) -> int:
        if not self.cfg.symbols:
            return 0
        n = 0
        now = _now_ms()
        for sym in self.cfg.symbols:
            sym = sym.strip().upper()
            if not sym:
                continue
            # throttle per symbol
            lp = int(self._last_pub_ts_ms.get(sym, 0))
            if lp > 0 and (now - lp) < int(self.cfg.min_change_publish_sec * 1000.0):
                continue
            levels = await self._compute_levels(sym, now)
            payload = self._levels_to_zones(levels, sym, now)
            raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            h = _hash_payload(raw)
            key = f"{self.cfg.key_prefix}{sym}"
            hkey = f"{self.cfg.state_hash_prefix}{sym}"
            try:
                prev = await self.r.get(hkey)
            except Exception:
                prev = None
            if prev == h:
                continue
            try:
                await self.r.set(key, raw, ex=self.cfg.ttl_sec)
                await self.r.set(hkey, h, ex=self.cfg.ttl_sec)
                self._last_pub_ts_ms[sym] = now
                n += 1
            except Exception:
                continue
        return n

    async def run_forever(self) -> None:
        while True:
            await self.tick_once()
            await asyncio.sleep(max(0.5, float(self.cfg.interval_sec)))


async def _amain() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    pub = HTFZonesPublisher(redis_url=redis_url)
    await pub.run_forever()


if __name__ == "__main__":
    asyncio.run(_amain())
