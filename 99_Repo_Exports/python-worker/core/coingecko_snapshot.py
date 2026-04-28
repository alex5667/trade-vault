import asyncio
import time
import logging
from typing import Dict, Any, Optional
import redis.asyncio as aioredis


class CoinGeckoSnapshotReader:
    """
    Асинхронный сервис для чтения данных CoinGecko из Redis.
    Обновляет локальный кэш каждые `refresh_ms`, чтобы не блокировать обработку тиков.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        refresh_ms: int = 10_000,
        max_stale_ms: int = 300_000,
    ) -> None:
        self.r = redis_client
        self.refresh_ms = refresh_ms
        self.max_stale_ms = max_stale_ms
        self._global: Dict[str, Any] = {}
        self._markets: Dict[str, Dict[str, Any]] = {}
        self._derivatives: Dict[str, Any] = {}
        self._sectors: Dict[str, Dict[str, Any]] = {}
        self._liquidity: Dict[str, Dict[str, Any]] = {}
        self._last_refresh_ms: int = 0
        self._task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger("coingecko_snapshot")

    def start(self) -> None:
        """Запускает фоновую задачу обновления кэша."""
        if self._task is None:
            self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Останавливает фоновую задачу."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
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
            if raw_global:
                self._global = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_global.items()}
            
            # 1.5 Читаем Derivatives
            raw_deriv = await self.r.hgetall("runtime:coingecko:derivatives:binance_futures")
            if raw_deriv:
                self._derivatives = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_deriv.items()}
            
            # 2. Ищем все хэши (market, sector, liquidity)
            # SCAN может быть медленным, но мы делаем это раз в 10 сек в бэкграунде
            cursor = b"0"
            keys_to_fetch = []
            while True:
                cursor, keys = await self.r.scan(cursor, match="runtime:coingecko:market:*", count=100)
                keys_to_fetch.extend([k.decode("utf-8") for k in keys])
                if cursor == b"0":
                    break
                    
            cursor = b"0"
            while True:
                cursor, keys = await self.r.scan(cursor, match="runtime:coingecko:sector:*", count=100)
                keys_to_fetch.extend([k.decode("utf-8") for k in keys])
                if cursor == b"0":
                    break

            cursor = b"0"
            while True:
                cursor, keys = await self.r.scan(cursor, match="runtime:coingecko:liquidity:*", count=100)
                keys_to_fetch.extend([k.decode("utf-8") for k in keys])
                if cursor == b"0":
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

    def get_snapshot(self, symbol: str, now_ms: int) -> Dict[str, Any]:
        """
        Синхронно возвращает словарь с индикаторами cg_* для вставки в payloads.
        Не делает I/O!
        """
        ind = {}
        
        # --- Global ---
        g = self._global
        g_ts = int(g.get("ts_ms", 0) or 0)
        if g_ts > 0 and (now_ms - g_ts) < self.max_stale_ms:
            ind["cg_global_mcap_usd"] = float(g.get("total_mcap_usd", 0.0) or 0.0)
            ind["cg_global_volume_usd"] = float(g.get("total_volume_usd", 0.0) or 0.0)
            ind["cg_btc_dom_pct"] = float(g.get("btc_dom_pct", 0.0) or 0.0)
            ind["cg_stable_dom_pct"] = float(g.get("stable_dom_pct", 0.0) or 0.0)
            ind["cg_btc_dom_mom"] = float(g.get("btc_dom_mom", 0.0) or 0.0)
            ind["cg_stable_dom_mom"] = float(g.get("stable_dom_mom", 0.0) or 0.0)
        
        # --- Market ---
        m = self._markets.get(symbol, {})
        m_ts = int(m.get("ts_ms", 0) or 0)
        if m_ts > 0 and (now_ms - m_ts) < self.max_stale_ms:
            ind["cg_symbol_market_cap_usd"] = float(m.get("market_cap_usd", 0.0) or 0.0)
            ind["cg_symbol_volume_24h_usd"] = float(m.get("volume_24h_usd", 0.0) or 0.0)
            ind["cg_symbol_price_chg_24h"] = float(m.get("price_change_24h_pct", 0.0) or 0.0)
            ind["cg_symbol_rel_strength_btc_1h"] = float(m.get("rel_strength_btc_1h", 0.0) or 0.0)
            ind["cg_symbol_rel_strength_eth_1h"] = float(m.get("rel_strength_eth_1h", 0.0) or 0.0)
            ind["cg_symbol_ath_distance_pct"] = float(m.get("ath_distance_pct", 0.0) or 0.0)
            
        # --- Derivatives ---
        d = self._derivatives
        d_ts = int(d.get("ts_ms", 0) or 0)
        if d_ts > 0 and (now_ms - d_ts) < self.max_stale_ms:
            ind["cg_oi_volume_ratio"] = float(d.get("oi_volume_ratio", 0.0) or 0.0)

        # --- Sectors ---
        # Calculate average sector 24h change as a proxy for global sector strength
        valid_sectors = []
        for s_id, s_data in self._sectors.items():
            s_ts = int(s_data.get("ts_ms", 0) or 0)
            if s_ts > 0 and (now_ms - s_ts) < self.max_stale_ms:
                valid_sectors.append(float(s_data.get("market_cap_change_24h", 0.0) or 0.0))
        if valid_sectors:
            ind["cg_sector_mcap_change_24h"] = sum(valid_sectors) / len(valid_sectors)

        # --- Liquidity ---
        l = self._liquidity.get(symbol, {})
        l_ts = int(l.get("ts_ms", 0) or 0)
        if l_ts > 0 and (now_ms - l_ts) < self.max_stale_ms:
            ind["cg_cost_to_move_imbalance"] = float(l.get("cost_to_move_imbalance", 0.0) or 0.0)

            
        return ind
