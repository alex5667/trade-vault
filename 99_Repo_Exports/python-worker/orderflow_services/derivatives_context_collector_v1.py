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
import math
import os

import redis.asyncio as aioredis

from services.binance_futures_client import BinanceFuturesPublicREST
from services.orderflow.derivatives_context import (
    DEFAULT_CTX_PREFIX,
    aread_derivatives_context,
    awrite_derivatives_context,
    build_snapshot_v2,
    robust_zscore,
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
        # Single source of truth: trade universe from config/generated/symbols.env
        # (CRYPTO_SYMBOLS / TRADE_SYMBOLS_UNIVERSE). DERIV_CTX_SYMBOLS override
        # remains for ad-hoc canaries.
        self.static_symbols = _split_csv(
            os.getenv("DERIV_CTX_SYMBOLS")
            or os.getenv("CRYPTO_SYMBOLS")
            or os.getenv("TRADE_SYMBOLS_UNIVERSE")
            or "BTCUSDT,ETHUSDT"
        )
        self.history_len = int(os.getenv("DERIV_CTX_HISTORY_LEN", "96") or 96)
        self.funding_extreme_abs = float(os.getenv("DERIV_CTX_FUNDING_EXTREME_ABS", "0.0008") or 0.0008)
        self.basis_extreme_abs_bps = float(os.getenv("DERIV_CTX_BASIS_EXTREME_BPS", "10.0") or 10.0)
        self.oi_accel_abs_usd = float(os.getenv("DERIV_CTX_OI_ACCEL_ABS_USD", "5000000") or 5_000_000.0)
        self.partial_prefix = os.getenv("DERIV_CTX_PARTIAL_PREFIX", "ctx:deriv_source:funding:")
        self.concurrency_limit = int(os.getenv("DERIV_CTX_CONCURRENCY", "5") or 5)
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)
        # 2026-05-16 extension: L-S ratio / taker ratio / liquidation aggregation.
        self.ls_history_len = int(os.getenv("DERIV_CTX_LS_HISTORY_LEN", "24") or 24)
        self.ls_period = str(os.getenv("DERIV_CTX_LS_PERIOD", "5m") or "5m")
        self.taker_period = str(os.getenv("DERIV_CTX_TAKER_PERIOD", "5m") or "5m")
        self.liq_stream = os.getenv("LIQ_EVT_STREAM", "stream:liq_evt")
        self.liq_window_ms = int(os.getenv("DERIV_CTX_LIQ_WINDOW_MS", "60000") or 60000)
        self.liq_imb_history_len = int(os.getenv("DERIV_CTX_LIQ_IMB_HISTORY_LEN", "48") or 48)

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

    async def _push_ls_history(self, symbol: str, ls_ratio: float) -> None:
        """Persist L-S ratio history for robust z-score across collector restarts."""
        key = f"ctx:deriv_hist:ls:{symbol}"
        try:
            pipe = self.r.pipeline()
            pipe.lpush(key, float(ls_ratio))
            pipe.ltrim(key, 0, self.ls_history_len - 1)
            pipe.expire(key, max(self.ttl_s * 8, 3600))
            await pipe.execute()
        except Exception:
            pass

    async def _push_liq_imb_history(self, symbol: str, imb: float) -> None:
        """Persist liq imbalance history for robust z-score."""
        key = f"ctx:deriv_hist:liq_imb:{symbol}"
        try:
            pipe = self.r.pipeline()
            pipe.lpush(key, float(imb))
            pipe.ltrim(key, 0, self.liq_imb_history_len - 1)
            pipe.expire(key, max(self.ttl_s * 8, 3600))
            await pipe.execute()
        except Exception:
            pass

    async def _read_liq_imb_history(self, symbol: str) -> list[float]:
        key = f"ctx:deriv_hist:liq_imb:{symbol}"
        try:
            vals = await self.r.lrange(key, 0, self.liq_imb_history_len - 1)
            return [float(v) for v in vals or []]
        except Exception:
            return []

    async def _push_oi_delta_history(self, symbol: str, doi: float) -> None:
        key = f"ctx:deriv_hist:oi_delta:{symbol}"
        try:
            pipe = self.r.pipeline()
            pipe.lpush(key, float(doi))
            pipe.ltrim(key, 0, self.history_len - 1)
            pipe.expire(key, max(self.ttl_s * 8, 3600))
            await pipe.execute()
        except Exception:
            pass

    async def _read_oi_delta_history(self, symbol: str) -> list[float]:
        key = f"ctx:deriv_hist:oi_delta:{symbol}"
        try:
            vals = await self.r.lrange(key, 0, self.history_len - 1)
            return [float(v) for v in vals or []]
        except Exception:
            return []

    async def _push_premium_history(self, symbol: str, premium: float) -> None:
        key = f"ctx:deriv_hist:premium:{symbol}"
        try:
            pipe = self.r.pipeline()
            pipe.lpush(key, float(premium))
            pipe.ltrim(key, 0, self.history_len - 1)
            pipe.expire(key, max(self.ttl_s * 8, 3600))
            await pipe.execute()
        except Exception:
            pass

    async def _push_oi_notional_history(self, symbol: str, oi_usd: float) -> None:
        key = f"ctx:deriv_hist:oi_notional:{symbol}"
        try:
            pipe = self.r.pipeline()
            pipe.lpush(key, float(oi_usd))
            pipe.ltrim(key, 0, self.history_len - 1)
            pipe.expire(key, max(self.ttl_s * 8, 3600))
            await pipe.execute()
        except Exception:
            pass

    async def _read_oi_notional_history(self, symbol: str) -> list[float]:
        key = f"ctx:deriv_hist:oi_notional:{symbol}"
        try:
            vals = await self.r.lrange(key, 0, self.history_len - 1)
            return [float(v) for v in vals or []]
        except Exception:
            return []

    async def _read_premium_history(self, symbol: str) -> list[float]:
        key = f"ctx:deriv_hist:premium:{symbol}"
        try:
            vals = await self.r.lrange(key, 0, self.history_len - 1)
            return [float(v) for v in vals or []]
        except Exception:
            return []

    async def _fetch_long_short_ratio(self, symbol: str, per_symbol_timeout: float) -> tuple[float, float]:
        """Returns (current_long_short_ratio, robust_z_over_history)."""
        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(
                    self.public.get_global_long_short_account_ratio,
                    symbol,
                    period=self.ls_period,
                    limit=self.ls_history_len,
                ),
                timeout=per_symbol_timeout,
            )
        except Exception as exc:
            logger.debug("L-S ratio fetch failed for %s: %s", symbol, exc)
            return 0.0, 0.0
        if not isinstance(data, list) or not data:
            return 0.0, 0.0
        try:
            ratios = [float(item.get("longShortRatio") or 0.0) for item in data if isinstance(item, dict)]
        except Exception:
            return 0.0, 0.0
        if not ratios:
            return 0.0, 0.0
        current = ratios[-1]  # API returns oldest→newest
        history = ratios[:-1] if len(ratios) > 1 else []
        z = robust_zscore(x=current, history=history) if history else 0.0
        return float(current), float(z)

    async def _fetch_taker_stats(
        self, symbol: str, per_symbol_timeout: float
    ) -> tuple[float, float, float]:
        """Returns (taker_buy_sell_imbalance, taker_buy_sell_ratio, taker_buy_sell_ratio_z).

        imbalance = (buy - sell) / (buy + sell)  ∈ [-1, 1]
        ratio     = buy / sell                    > 0, 0.0 when sell=0
        ratio_z   = robust z-score over history of ratio values
        """
        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(
                    self.public.get_taker_long_short_ratio,
                    symbol,
                    period=self.taker_period,
                    limit=self.ls_history_len,
                ),
                timeout=per_symbol_timeout,
            )
        except Exception as exc:
            logger.debug("taker ratio fetch failed for %s: %s", symbol, exc)
            return 0.0, 0.0, 0.0
        if not isinstance(data, list) or not data:
            return 0.0, 0.0, 0.0
        ratios: list[float] = []
        last_imb = 0.0
        last_ratio = 0.0
        for item in data:
            if not isinstance(item, dict):
                continue
            try:
                buy = float(item.get("buyVol") or 0.0)
                sell = float(item.get("sellVol") or 0.0)
            except Exception:
                continue
            total = buy + sell
            if total <= 0:
                continue
            ratios.append(buy / sell if sell > 0 else 0.0)
            last_imb = (buy - sell) / total
            last_ratio = ratios[-1]
        if not ratios:
            return 0.0, 0.0, 0.0
        history = ratios[:-1] if len(ratios) > 1 else []
        ratio_z = robust_zscore(x=last_ratio, history=history) if history else 0.0
        return last_imb, last_ratio, float(ratio_z)

    async def _fetch_top_trader_ratio(self, symbol: str, per_symbol_timeout: float) -> float:
        """Top-trader long/short position ratio from Binance /fapi/v1/topLongShortPositionRatio."""
        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(
                    self.public.get_top_long_short_position_ratio,
                    symbol,
                    period=self.ls_period,
                    limit=1,
                ),
                timeout=per_symbol_timeout,
            )
        except Exception as exc:
            logger.debug("top_trader ratio fetch failed for %s: %s", symbol, exc)
            return 0.0
        if not isinstance(data, list) or not data:
            return 0.0
        last = data[-1] if isinstance(data[-1], dict) else {}
        try:
            return float(last.get("longShortRatio") or 0.0)
        except Exception:
            return 0.0

    async def _aggregate_liquidations(self, symbol: str, now_ms: int) -> tuple[float, float, float]:
        """Sliding-window aggregation over stream:liq_evt.

        Returns (liq_buy_notional_1m, liq_sell_notional_1m, liq_imbalance_z).
        Walks the stream from (now - window) backwards via XRANGE — typical N≪1000.
        """
        from_ms = max(0, int(now_ms) - int(self.liq_window_ms))
        try:
            entries = await self.r.xrange(self.liq_stream, min=f"{from_ms}-0", max="+")
        except Exception as exc:
            logger.debug("liq xrange failed: %s", exc)
            return 0.0, 0.0, 0.0
        buy_total = 0.0
        sell_total = 0.0
        sym_u = symbol.upper()
        for _eid, fields in entries or []:
            try:
                if (fields.get("symbol") or "").upper() != sym_u:
                    continue
                notional = float(fields.get("notional_usd") or 0.0)
                side = (fields.get("order_side") or "").upper()
                if side == "BUY":
                    buy_total += notional
                elif side == "SELL":
                    sell_total += notional
            except Exception:
                continue
        total = buy_total + sell_total
        imb = (buy_total - sell_total) / total if total > 0 else 0.0
        history = await self._read_liq_imb_history(symbol)
        z = robust_zscore(x=imb, history=history) if history else 0.0
        return buy_total, sell_total, float(z)

    async def _collect_symbol(self, symbol: str) -> None:
        now_ms = get_ny_time_millis()
        prev = await aread_derivatives_context(self.r, symbol=symbol, prefix=DEFAULT_CTX_PREFIX)
        funding_partial = await self._get_partial_funding_payload(symbol)
        funding_hist = await self._read_funding_history(symbol)
        oi_delta_hist = await self._read_oi_delta_history(symbol)
        oi_notional_hist = await self._read_oi_notional_history(symbol)
        premium_hist = await self._read_premium_history(symbol)

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
            doi = float(open_interest or 0.0) - prev_oi

            # fetch L-S / taker / top-trader / liquidation aggregates
            # (independent endpoints, single semaphore slot already held above).
            try:
                ls_ratio, ls_z = await self._fetch_long_short_ratio(symbol, per_symbol_timeout)
            except Exception:
                ls_ratio, ls_z = 0.0, 0.0
            try:
                taker_imb, taker_ratio, taker_ratio_z = await self._fetch_taker_stats(symbol, per_symbol_timeout)
            except Exception:
                taker_imb, taker_ratio, taker_ratio_z = 0.0, 0.0, 0.0
            try:
                top_trader_ls = await self._fetch_top_trader_ratio(symbol, per_symbol_timeout)
            except Exception:
                top_trader_ls = 0.0
            try:
                liq_buy, liq_sell, liq_z = await self._aggregate_liquidations(symbol, now_ms)
            except Exception:
                liq_buy, liq_sell, liq_z = 0.0, 0.0, 0.0

            # P4 composites computed from already-fetched values
            # force_order_cluster_score: directional liq imbalance weighted by total magnitude
            _liq_total_now = liq_buy + liq_sell
            _liq_dir_imb = (liq_buy - liq_sell) / _liq_total_now if _liq_total_now > 0 else 0.0
            _cluster = _liq_dir_imb * math.log1p(_liq_total_now / 1_000_000.0)
            # futures_crowding_score: funding_z × ls_z (aligned extremes → crowded)
            _fz = 0.0
            try:
                _funding_now = float(funding_partial.get("funding_rate", 0.0))
                _fz = robust_zscore(x=_funding_now, history=funding_hist)
            except Exception:
                pass
            _crowding = max(-3.0, min(3.0, _fz * ls_z / 9.0))

            snap = build_snapshot_v2(
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
                long_short_ratio=float(ls_ratio),
                long_short_ratio_z=float(ls_z),
                taker_buy_sell_imbalance=float(taker_imb),
                liq_buy_notional_1m=float(liq_buy),
                liq_sell_notional_1m=float(liq_sell),
                liq_imbalance_z=float(liq_z),
                # v3
                top_trader_long_short_ratio=float(top_trader_ls),
                taker_buy_sell_ratio=float(taker_ratio),
                taker_buy_sell_ratio_z=float(taker_ratio_z),
                force_order_cluster_score=float(_cluster),
                futures_crowding_score=float(_crowding),
                oi_delta_history=oi_delta_hist,
                premium_history=premium_hist,
                oi_notional_history=oi_notional_hist,
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
                # Persist L-S ratio history (current value) for z-score continuity
                # across restarts; on warm cache the API already provides enough
                # history, so this is a defensive backup.
                if ls_ratio:
                    await self._push_ls_history(symbol, ls_ratio)
                # Liq imbalance history (computed from notionals above).
                _liq_total = liq_buy + liq_sell
                _liq_imb_now = (liq_buy - liq_sell) / _liq_total if _liq_total > 0 else 0.0
                await self._push_liq_imb_history(symbol, _liq_imb_now)
                # oi_delta and premium_index histories for z-score computation.
                await self._push_oi_delta_history(symbol, doi)
                await self._push_premium_history(symbol, float(premium_index or 0.0))
                await self._push_oi_notional_history(symbol, snap.oi_notional_usd)

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
