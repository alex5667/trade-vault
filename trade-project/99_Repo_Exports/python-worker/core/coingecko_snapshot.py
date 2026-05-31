import asyncio
import logging
import time
from typing import Any

import os
import redis.asyncio as aioredis
import contextlib


class CoinGeckoSnapshotReader:
    """
    Асинхронный сервис для чтения данных CoinGecko из Redis.
    Обновляет локальный кэш каждые `refresh_ms`, чтобы не блокировать обработку тиков.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        refresh_ms: int = 10_000,
        max_stale_ms: int = 86_400_000,  # 24 hours to support LOCF during night-time sleep
    ) -> None:
        self.r = redis_client
        self.refresh_ms = refresh_ms
        self.max_stale_ms = max_stale_ms
        
        self.load_derivatives = int(os.getenv("COINGECKO_READER_LOAD_DERIVATIVES", "0")) == 1
        self.load_liquidity = int(os.getenv("COINGECKO_READER_LOAD_LIQUIDITY", "0")) == 1
        
        self._global: dict[str, Any] = {}
        self._markets: dict[str, dict[str, Any]] = {}
        self._derivatives: dict[str, Any] = {}
        self._sectors: dict[str, dict[str, Any]] = {}
        self._liquidity: dict[str, dict[str, Any]] = {}
        self._circuit_status: dict[str, Any] = {}
        self._last_refresh_ms: int = 0
        self._task: asyncio.Task | None = None
        self.logger = logging.getLogger("coingecko_snapshot")

    def start(self) -> None:
        """Запускает фоновую задачу обновления кэша."""
        if self._task is None:
            self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Останавливает фоновую задачу."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _poll_loop(self) -> None:
        self.logger.info("Starting CoinGecko snapshot polling loop (refresh_ms=%d)", self.refresh_ms)
        while True:
            try:
                await self._refresh_cache()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in CoinGecko _poll_loop: {e}", exc_info=True)
            await asyncio.sleep(self.refresh_ms / 1000.0)

    async def _refresh_cache(self) -> None:
        """Вычитывает хэши из Redis и обновляет локальные словари."""
        try:
            # 1. Читаем Global snapshot
            raw_global = await self.r.hgetall("runtime:coingecko:global")
            if not raw_global or b"total_mcap_usd" not in raw_global:
                raw_global_fb = await self.r.hgetall("runtime:provider:global")
                if raw_global_fb:
                    raw_global = raw_global_fb

            if raw_global:
                self._global = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_global.items()}

            # 1.5 Читаем Derivatives
            if self.load_derivatives:
                raw_deriv = await self.r.hgetall("runtime:coingecko:derivatives:binance_futures")
                if raw_deriv:
                    self._derivatives = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_deriv.items()}

            # 1.6 Читаем Circuit Status
            raw_circuit = await self.r.hgetall("runtime:coingecko:circuit:status")
            if raw_circuit:
                self._circuit_status = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_circuit.items()}

            # 2. Ищем все хэши (market, sector, liquidity)
            # SCAN может быть медленным, но мы делаем это раз в 10 сек в бэкграунде
            cursor = 0
            keys_to_fetch = []
            while True:
                cursor, keys = await self.r.scan(cursor, match="runtime:coingecko:market:*", count=100)
                keys_to_fetch.extend([k.decode("utf-8") for k in keys])
                if cursor == 0 or cursor == b"0":
                    break

            cursor = 0
            while True:
                cursor, keys = await self.r.scan(cursor, match="runtime:coingecko:sector:*", count=100)
                keys_to_fetch.extend([k.decode("utf-8") for k in keys])
                if cursor == 0 or cursor == b"0":
                    break

            if self.load_liquidity:
                cursor = 0
                while True:
                    cursor, keys = await self.r.scan(cursor, match="runtime:coingecko:liquidity:*", count=100)
                    keys_to_fetch.extend([k.decode("utf-8") for k in keys])
                    if cursor == 0 or cursor == b"0":
                        break

            # 3. Читаем market/sector/liquidity данные через пайплайн
            new_markets = {}
            new_sectors = {}
            new_liquidity = {}
            if keys_to_fetch:
                pipe = self.r.pipeline()
                for key in keys_to_fetch:
                    pipe.hgetall(key)
                results = await pipe.execute()
                for key, res in zip(keys_to_fetch, results):
                    if not res:
                        continue
                    parts = key.split(":")
                    if len(parts) >= 4:
                        prefix = parts[2]
                        item_id = parts[3]
                        parsed_res = {k.decode("utf-8"): v.decode("utf-8") for k, v in res.items()}
                        if prefix == "market":
                            new_markets[item_id] = parsed_res
                        elif prefix == "sector":
                            new_sectors[item_id] = parsed_res
                        elif prefix == "liquidity":
                            new_liquidity[item_id] = parsed_res
            self._markets = new_markets
            self._sectors = new_sectors
            self._liquidity = new_liquidity

            self._last_refresh_ms = int(time.time() * 1000)
        except Exception as e:
            self.logger.error(f"Failed to refresh CoinGecko cache: {e}")

    def _fresh_age_ms(self, data: dict[str, Any]) -> int:
        try:
            v = int(float(data.get("max_fresh_age_ms", 0) or 0))
            if v > 0:
                return v
        except Exception:
            pass
        return self.max_stale_ms

    def _is_fresh(self, data: dict[str, Any], now_ms: int) -> bool:
        status, _, _, _ = self._snapshot_status(data, now_ms)
        return status == "ok"

    def _snapshot_status(self, data: dict[str, Any], now_ms: int) -> tuple[str, int, float, str]:
        ts = int(data.get("ts_ms", 0) or 0)
        if ts <= 0:
            return "missing", 0, 0.0, "missing"

        age_ms = max(0, now_ms - ts)
        fresh_ms = self._fresh_age_ms(data)

        if age_ms < fresh_ms:
            return "ok", age_ms, 1.0, "ok"

        if age_ms < fresh_ms * 3:
            return "stale", age_ms, 0.5, "stale"

        return "missing", age_ms, 0.0, "expired"

    def get_snapshot(self, symbol: str, now_ms: int) -> dict[str, Any]:
        """
        Синхронно возвращает словарь с индикаторами cg_* для вставки в payloads.
        Не делает I/O!
        """
        ind = {}

        # --- Status & Quality ---
        g = self._global
        status, age_ms, quality, reason = self._snapshot_status(g, now_ms)
        
        ind["cg_status"] = status
        ind["cg_age_ms"] = age_ms
        ind["cg_quality"] = quality
        ind["cg_reason"] = reason

        circuit = self._circuit_status
        circuit_status = circuit.get("status", "closed")
        if circuit_status == "open":
            ind["cg_status"] = "circuit_open"
            ind["cg_quality"] = min(ind.get("cg_quality", 0.0), 0.3)
            ind["cg_reason"] = circuit.get("reason", "provider_429")

        # --- Global ---
        if self._is_fresh(g, now_ms):
            ind["cg_global_mcap_usd"] = float(g.get("total_mcap_usd", g.get("provider_global_mcap", 0.0)) or 0.0)
            ind["cg_global_volume_usd"] = float(g.get("total_volume_usd", g.get("provider_total_volume", 0.0)) or 0.0)
            ind["cg_btc_dom_pct"] = float(g.get("btc_dom_pct", g.get("provider_btc_dominance", 0.0)) or 0.0)
            ind["cg_stable_dom_pct"] = float(g.get("stable_dom_pct", 0.0) or 0.0)
            ind["cg_btc_dom_mom"] = float(g.get("btc_dom_mom", 0.0) or 0.0)
            ind["cg_stable_dom_mom"] = float(g.get("stable_dom_mom", 0.0) or 0.0)

        # --- Market ---
        m = self._markets.get(symbol, {})
        if self._is_fresh(m, now_ms):
            ind["cg_symbol_market_cap_usd"] = float(m.get("market_cap_usd", 0.0) or 0.0)
            ind["cg_symbol_volume_24h_usd"] = float(m.get("volume_24h_usd", 0.0) or 0.0)
            ind["cg_symbol_price_chg_24h"] = float(m.get("price_change_24h_pct", 0.0) or 0.0)
            ind["cg_symbol_rel_strength_btc_1h"] = float(m.get("rel_strength_btc_1h", 0.0) or 0.0)
            ind["cg_symbol_rel_strength_eth_1h"] = float(m.get("rel_strength_eth_1h", 0.0) or 0.0)
            ind["cg_symbol_ath_distance_pct"] = float(m.get("ath_distance_pct", 0.0) or 0.0)

        # --- Derivatives (Disabled, handled via Binance-native V13) ---
        # cg_oi_volume_ratio removed from production decision logic

        # --- Sectors ---
        # Calculate average sector 24h change as a proxy for global sector strength
        valid_sectors = []
        for s_id, s_data in self._sectors.items():
            if self._is_fresh(s_data, now_ms):
                valid_sectors.append(float(s_data.get("market_cap_change_24h", 0.0) or 0.0))
        if valid_sectors:
            ind["cg_sector_mcap_change_24h"] = sum(valid_sectors) / len(valid_sectors)

        # --- Liquidity (Disabled, use Execution-Grade Execution Gate instead) ---
        ind["cg_liquidity_status"] = "disabled"
        ind["cg_liquidity_quality"] = 0.0

        return ind
