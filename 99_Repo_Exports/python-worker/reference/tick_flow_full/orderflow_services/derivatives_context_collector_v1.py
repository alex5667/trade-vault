from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Low-frequency collector for normalized derivatives context.

Responsibilities
----------------
- poll public Binance USDⓈ-M endpoints for premium index / mark / index / OI
- merge with partial funding stream payloads when available
- compute normalized snapshot and publish to Redis key `ctx:deriv:<SYMBOL>`

This service is *not* on the hot tick path.
It is safe to run every 15–60 seconds.
"""

import asyncio
import json
import logging
import os
import time
from typing import Dict, List

import redis.asyncio as aioredis

from services.binance_futures_client import BinanceFuturesPublicREST
from services.orderflow.derivatives_context import (
    DEFAULT_CTX_PREFIX
    aread_derivatives_context
    awrite_derivatives_context
    build_snapshot
)
from services.orderflow.metrics_derivatives_context import deriv_ctx_collector_errors_total, deriv_ctx_collector_up

logger = logging.getLogger("derivatives_context_collector_v1")


def _split_csv(raw: str) -> List[str]:
    return [s.strip().upper() for s in str(raw or "").split(",") if s.strip()]


class DerivativesContextCollector:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = aioredis.from_url(self.redis_url, decode_responses=True)
        self.public = BinanceFuturesPublicREST(
            base_url=(os.getenv("BINANCE_FUTURES_BASE_URL") or "https://fapi.binance.com").strip()
            timeout_s=float(os.getenv("BINANCE_HTTP_TIMEOUT_S", "8.0") or 8.0)
        )
        self.interval_s = float(os.getenv("DERIV_CTX_POLL_INTERVAL_S", "30") or 30.0)
        self.ttl_s = int(os.getenv("DERIV_CTX_TTL_S", "180") or 180)
        self.symbols_key = os.getenv("DERIV_CTX_SYMBOLS_SET", "crypto:symbols")
        self.static_symbols = _split_csv(os.getenv("DERIV_CTX_SYMBOLS", "BTCUSDT,ETHUSDT"))
        self.history_len = int(os.getenv("DERIV_CTX_HISTORY_LEN", "96") or 96)
        self.funding_extreme_abs = float(os.getenv("DERIV_CTX_FUNDING_EXTREME_ABS", "0.0008") or 0.0008)
        self.basis_extreme_abs_bps = float(os.getenv("DERIV_CTX_BASIS_EXTREME_BPS", "10.0") or 10.0)
        self.oi_accel_abs_usd = float(os.getenv("DERIV_CTX_OI_ACCEL_ABS_USD", "5000000") or 5_000_000.0)
        self.partial_prefix = os.getenv("DERIV_CTX_PARTIAL_PREFIX", "ctx:deriv_source:funding:")
        self.concurrency_limit = int(os.getenv("DERIV_CTX_CONCURRENCY", "5") or 5)
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)

    async def _discover_symbols(self) -> List[str]:
        out = set(self.static_symbols)
        try:
            members = await self.r.smembers(self.symbols_key)
            for m in members or []:
                s = str(m or "").strip().upper()
                if s:
                    out.add(s)
        except Exception:
            pass
        if not out:
            out = {"BTCUSDT", "ETHUSDT"}
        return sorted(out)

    async def _read_funding_history(self, symbol: str) -> List[float]:
        key = f"ctx:deriv_hist:funding:{symbol}"
        try:
            vals = await self.r.lrange(key, 0, self.history_len - 1)
            return [float(v) for v in vals or []]
        except Exception:
            return []

    async def _push_funding_history(self, symbol: str, funding_rate: float) -> None:
        key = f"ctx:deriv_hist:funding:{symbol}"
        try:
            pipe = self.r.pipeline()
            pipe.lpush(key, float(funding_rate))
            pipe.ltrim(key, 0, self.history_len - 1)
            pipe.expire(key, max(self.ttl_s * 8, 3600))
            await pipe.execute()
        except Exception:
            pass

    async def _get_partial_funding_payload(self, symbol: str) -> Dict:
        try:
            raw = await self.r.get(f"{self.partial_prefix}{symbol}")
            return json.loads(raw) if raw else {}
        except Exception:
            return {}

    async def _collect_symbol(self, symbol: str) -> None:
        now_ms = get_ny_time_millis()
        prev = await aread_derivatives_context(self.r, symbol=symbol, prefix=DEFAULT_CTX_PREFIX)
        funding_partial = await self._get_partial_funding_payload(symbol)
        funding_hist = await self._read_funding_history(symbol)

        async with self.semaphore:
            # Public REST calls are blocking stdlib HTTP -> offload to thread pool.
            premium = await asyncio.to_thread(self.public.get_premium_index, symbol)
            oi = await asyncio.to_thread(self.public.get_open_interest, symbol)

        funding_rate = funding_partial.get("funding_rate", premium.get("lastFundingRate", premium.get("lastFundingRateRate", 0.0)))
        mark_price = premium.get("markPrice", 0.0)
        index_price = premium.get("indexPrice", 0.0)
        premium_index = premium.get("premiumIndex", premium.get("lastFundingRate", 0.0))
        open_interest = oi.get("openInterest", 0.0)
        prev_oi = float(prev.open_interest if prev else 0.0)

        snap = build_snapshot(
            symbol=symbol
            ts_ms=now_ms
            venue="binance"
            funding_rate=float(funding_rate or 0.0)
            funding_history=funding_hist
            premium_index=float(premium_index or 0.0)
            mark_price=float(mark_price or 0.0)
            index_price=float(index_price or 0.0)
            open_interest=float(open_interest or 0.0)
            previous_open_interest=float(prev_oi)
            funding_extreme_abs=self.funding_extreme_abs
            basis_extreme_abs_bps=self.basis_extreme_abs_bps
            oi_accel_abs_usd=self.oi_accel_abs_usd
        )
        ok = await awrite_derivatives_context(self.r, snap, ttl_s=self.ttl_s)
        if ok:
            await self._push_funding_history(symbol, snap.funding_rate)

    async def run_once(self) -> None:
        syms = await self._discover_symbols()
        tasks = []
        for sym in syms:
            tasks.append(self._safe_collect_symbol(sym))
        
        # Parallelize collection with a global timeout for the whole batch
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.interval_s - 5)
        except asyncio.TimeoutError:
            logger.error("run_once: timed out waiting for symbol collection batch")

    async def _safe_collect_symbol(self, symbol: str) -> None:
        try:
            await self._collect_symbol(symbol)
        except Exception as exc:
            logger.exception("derivatives context collect failed for %s: %s", symbol, exc)
            try:
                deriv_ctx_collector_errors_total.labels(where="collect_symbol").inc()
            except Exception:
                pass

    async def run_forever(self) -> None:
        while True:
            try:
                if deriv_ctx_collector_up is not None:
                    deriv_ctx_collector_up.set(1)
                await self.run_once()
            except Exception as exc:
                logger.exception("collector loop failed: %s", exc)
                try:
                    deriv_ctx_collector_errors_total.labels(where="loop").inc()
                except Exception:
                    pass
            await asyncio.sleep(self.interval_s)


async def _amain() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    svc = DerivativesContextCollector()
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(_amain())
