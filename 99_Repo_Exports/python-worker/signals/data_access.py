from utils.time_utils import get_ny_time_millis
"""
Доступ к данным для сигналов: чтение тикеров 24h и ставок финансирования из Redis.
Назначение: предоставить простые функции получения и агрегирования данных для форматирования/публикации.
"""
import json
import logging
import time
from typing import Any, Dict, List

from core.redis_client import get_redis
from core.config import TOP_FUNDING_LIMIT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-process TTL cache for expensive SCAN results
# SCAN over binance:ticker24h:* and binance:fundingRate:* was running p99=143ms
# on every call. Cache for 60s to reduce Redis load.
# ---------------------------------------------------------------------------
_ticker_symbols_cache: list = []
_ticker_symbols_cache_ts: float = 0.0
_TICKER_CACHE_TTL_S: float = 60.0


def get_24h_ticker_symbols() -> List[str]:
    """Возвращает список символов с 24h тикерами из Redis. Кешируется на 60 секунд."""
    global _ticker_symbols_cache, _ticker_symbols_cache_ts
    now = time.monotonic()
    if _ticker_symbols_cache and (now - _ticker_symbols_cache_ts) < _TICKER_CACHE_TTL_S:
        return _ticker_symbols_cache
    try:
        redis_client = get_redis()
        symbols: List[str] = []
        cursor = 0
        
        # Используем SCAN вместо keys для совместимости с Redis
        while True:
            result = redis_client.scan(cursor, match="binance:ticker24h:*", count=10000)
            cursor, keys = result
            
            for key in keys:
                parts = key.split(":")
                if len(parts) >= 3:
                    symbols.append(parts[2])
            
            if cursor == 0:
                break
        
        if not symbols:
            logger.warning("No 24h tickers available in Redis")
        else:
            logger.debug("Found %d symbols with 24h data", len(symbols))
            _ticker_symbols_cache = symbols
            _ticker_symbols_cache_ts = now
        return symbols
    except Exception as e:
        logger.error("Error fetching 24h symbols: %s", e)
        return []


def get_ticker_data(symbol: str) -> Dict[str, Any]:
    """Читает из Redis JSON тикера 24h по символу и возвращает dict."""
    try:
        redis_client = get_redis()
        data = redis_client.get(f"binance:ticker24h:{symbol}")
        if not data:
            return {}
        return json.loads(data)
    except Exception as e:
        logger.error("Error fetching ticker data for %s: %s", symbol, e)
        return {}

_funding_rate_cache: list = []
_funding_rate_cache_ts: float = 0.0
_FUNDING_CACHE_TTL_S: float = 300.0  # funding rates change every 8h, 5min TTL is safe


def get_funding_rate_data() -> List[Dict[str, Any]]:
    """
    Возвращает топ 10 записей funding rates, отсортированных по абсолютному значению ставки.
    Кешируется на 5 минут (funding rates меняются каждые 8 часов).
    
    Логика отбора:
    1. Сначала отбираем данные за последние 24 часа (по fundingTime)
    2. Затем сортируем по абсолютному значению ставки финансирования
    """
    global _funding_rate_cache, _funding_rate_cache_ts
    now = time.monotonic()
    if _funding_rate_cache and (now - _funding_rate_cache_ts) < _FUNDING_CACHE_TTL_S:
        return _funding_rate_cache
    try:
        redis_client = get_redis()
        
        # Текущее время в миллисекундах
        current_time_ms = get_ny_time_millis()
        twenty_four_hours_ago_ms = current_time_ms - (24 * 60 * 60 * 1000)

        logger.debug("Filtering funding rates: cutoff=%d ms", twenty_four_hours_ago_ms)
        
        funding_data: List[Dict[str, Any]] = []
        filtered_count = 0
        total_count = 0
        cursor = 0
        
        # Используем SCAN вместо keys для совместимости с Redis
        while True:
            result = redis_client.scan(cursor, match="binance:fundingRate:*", count=10000)
            cursor, keys = result
            
            for key in keys:
                total_count += 1
                try:
                    data = redis_client.get(key)
                    if data:
                        funding_info = json.loads(data)
                        if "fundingRate" in funding_info:
                            # Проверяем время ставки финансирования
                            funding_time = funding_info.get("fundingTime")
                            if not funding_time:
                                logger.debug("Funding rate for %s has no fundingTime, skipping", key)
                                continue

                            # Проверяем, что ставка не старше 24 часов
                            if funding_time < twenty_four_hours_ago_ms:
                                logger.debug("Funding rate for %s is stale: %d < %d", key, funding_time, twenty_four_hours_ago_ms)
                                continue
                            
                            rate = float(funding_info["fundingRate"])
                            filtered_count += 1
                            
                            funding_data.append({
                                "symbol": funding_info.get("symbol", ""),
                                "fundingRate": rate,
                                "fundingTime": funding_time,
                                "key": key,
                            })
                except (json.JSONDecodeError, ValueError, TypeError) as e:
                    logger.warning("Error processing funding rate for %s: %s", key, e)
                    continue
            
            if cursor == 0:
                break
        
        logger.debug("Filtered %d/%d funding rates (last 24h)", filtered_count, total_count)

        if not funding_data:
            logger.warning("No current funding rate data for last 24h")
            return []

        funding_data.sort(key=lambda x: abs(x["fundingRate"]), reverse=True)
        result = funding_data[:TOP_FUNDING_LIMIT]
        logger.info("Top funding rates: %d entries, best: %s %.6f",
                    len(result),
                    result[0].get('symbol', 'N/A') if result else 'N/A',
                    result[0].get('fundingRate', 0.0) if result else 0.0)
        # Store in cache
        _funding_rate_cache = result
        _funding_rate_cache_ts = now
        return result
    except Exception as e:
        logger.error("Error fetching funding rate data: %s", e)
        return []