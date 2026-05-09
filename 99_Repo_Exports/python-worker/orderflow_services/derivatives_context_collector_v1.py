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
""",
import asyncio
import json
import logging
import os

import redis.asyncio as aioredis

from services.binance_futures_client import BinanceFuturesPublicREST
from services.orderflow.derivatives_context import (
    DEFAULT_CTX_PREFIX,
    aread_derivatives_context,
    awrite_derivatives_context,
    build_snapshot,
)
from services.orderflow.metrics_derivatives_context import deriv_ctx_collector_errors_total, deriv_ctx_collector_up
import contextlib

logger = logging.getLogger("derivatives_context_collector_v1")


def _split_csv(raw: str) -> list[str]:
    return [s.strip().upper() for s in (raw or "").split(",") if s.strip()]


class DerivativesContextCollector:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = aioredis.from_url(self.redis_url, decode_responses=True)
        write_urls_env = str(os.getenv("REDIS_WRITE_URLS", self.redis_url) or self.redis_url).split(",")
        self.write_clients = []
        for u in write_urls_env:
            u_str = u.strip()
            if u_str:
                self.write_clients.append(aioredis.from_url(u_str, decode_responses=True))
        if not self.write_clients:
            self.write_clients.append(self.r)

        self.public = BinanceFuturesPublicREST(
            base_url=(os.getenv("BINANCE_FUTURES_BASE_URL") or "https://fapi.binance.com").strip(),
            timeout_s=float(os.getenv("BINANCE_HTTP_TIMEOUT_S", "8.0") or 8.0),
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

    async def _discover_symbols(self) -> list[str]:
        out = set(self.static_symbols)
        try:
            members = await self.r.smembers(self.symbols_key)
            for m in members or []:
                s = (m or "").strip().upper()
                if s:
                    out.add(s)
        except Exception:
            pass
        if not out:
            out = {"BTCUSDT", "ETHUSDT"}
        return sorted(out)

    async def _read_funding_history(self, symbol: str) -> list[float]:
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

    async def _get_partial_funding_payload(self, symbol: str) -> dict:
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

        try:
            async with self.semaphore:
                # Public REST calls are blocking stdlib HTTP -> offload to thread pool.
                # Per-symbol timeout prevents a single unreachable symbol from blocking the batch.
                per_symbol_timeout = min(self.interval_s / 2, 15.0)
                try:
                    premium = await asyncio.wait_for(
                        asyncio.to_thread(self.public.get_premium_index, symbol),
                        timeout=per_symbol_timeout,
                    )
                    oi = await asyncio.wait_for(
                        asyncio.to_thread(self.public.get_open_interest, symbol),
                        timeout=per_symbol_timeout,
                    )
                except Exception as exc:
                    if prev:
                        logger.warning("derivatives context: fetch failed for %s, using STALE prev context: %s", symbol, exc)
                        # Reconstruct exchange-like dicts from prev snapshot (frozen dataclass).
                        # DerivativesContextSnapshot has no mark_price/index_price fields;
                        # use to_dict() and pass zeros for unavailable raw fields.
                        prev_d = prev.to_dict()
                        premium = {
                            "lastFundingRate": prev_d.get("funding_rate", 0.0),
                            "markPrice": 0.0,
                            "indexPrice": 0.0,
                            "premiumIndex": prev_d.get("premium_index", 0.0),
                        }
                        oi = {"openInterest": prev_d.get("open_interest", 0.0)}
                    else:
                        raise exc

            funding_rate = funding_partial.get("funding_rate", premium.get("lastFundingRate", premium.get("lastFundingRateRate", 0.0)))
            mark_price = premium.get("markPrice", 0.0)
            index_price = premium.get("indexPrice", 0.0)
            premium_index = premium.get("premiumIndex", premium.get("lastFundingRate", 0.0))
            open_interest = oi.get("openInterest", 0.0)
            prev_oi = float(prev.open_interest if prev else 0.0)

            snap = build_snapshot(
                symbol=symbol,
                ts_ms=now_ms,
                venue="binance",
                funding_rate=float(funding_rate or 0.0),
                funding_history=funding_hist,
                premium_index=float(premium_index or 0.0),
                mark_price=float(mark_price or 0.0),
                index_price=float(index_price or 0.0),
                open_interest=float(open_interest or 0.0),
                previous_open_interest=float(prev_oi),
                funding_extreme_abs=self.funding_extreme_abs,
                basis_extreme_abs_bps=self.basis_extreme_abs_bps,
                oi_accel_abs_usd=self.oi_accel_abs_usd,
            )
            # Re-verify build_snapshot didn't return something weird
            gather_tasks = [
                awrite_derivatives_context(rc, snap, ttl_s=self.ttl_s)
                for rc in self.write_clients
            ]
            writes = await asyncio.gather(*gather_tasks, return_exceptions=True)
            ok = writes[0] if writes and isinstance(writes[0], bool) else False
            if ok:
                await self._push_funding_history(symbol, snap.funding_rate)

        except Exception as exc:
            logger.error("derivatives context: critical failure for %s even with stale fallback: %s", symbol, exc)
            raise exc

    async def run_once(self) -> None:
        syms = await self._discover_symbols()
        results = await asyncio.gather(
            *[self._safe_collect_symbol(sym) for sym in syms],
            return_exceptions=True,
        )
        ok_count = sum(1 for r in results if r is None)  # None = success from _safe_collect_symbol
        fail_count = len(results) - ok_count
        if fail_count > 0:
            logger.warning("run_once: %d/%d symbols collected OK, %d failed", ok_count, len(syms), fail_count)
        else:
            logger.info("run_once: %d/%d symbols collected OK", ok_count, len(syms))

    async def _safe_collect_symbol(self, symbol: str) -> None:
        try:
            await self._collect_symbol(symbol)
        except Exception as exc:
            logger.exception("derivatives context collect failed for %s: %s", symbol, exc)
            with contextlib.suppress(Exception):
                deriv_ctx_collector_errors_total.labels(where="collect_symbol").inc()

    async def run_forever(self) -> None:
        while True:
            try:
                if deriv_ctx_collector_up is not None:
                    deriv_ctx_collector_up.set(1)
                await self.run_once()
            except Exception as exc:
                logger.exception("collector loop failed: %s", exc)
                with contextlib.suppress(Exception):
                    deriv_ctx_collector_errors_total.labels(where="loop").inc()
            await asyncio.sleep(self.interval_s)


async def _amain() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    svc = DerivativesContextCollector()
    logger.info(
        "derivatives_context_collector starting: redis=%s api=%s interval=%.0fs ttl=%ds symbols=%s",
        svc.redis_url[:30] + "...",
        svc.public.base_url,
        svc.interval_s,
        svc.ttl_s,
        ",".join(svc.static_symbols[:5]) + ("..." if len(svc.static_symbols) > 5 else ""),
    )
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(_amain())
