from utils.time_utils import get_ny_time_millis
"""
Оркестратор метрик: сбор топ‑метрик, объёмов и funding; форматирование и публикация в стримы.
"""
import asyncio
import logging
from typing import List, Tuple

from .sorting import get_sorted_tickers_by_change
from .formatters import format_entries, format_volume_entries, format_funding_entries
from .volumes import get_volume_data
from .data_access import get_funding_rate_data
from publisher.stream_publisher import publish_signal_to_stream
from infra.redis_repo import RedisTradeRepository
from core.redis_client import get_redis

logger = logging.getLogger(__name__)


def fetch_and_publish_top_metrics() -> Tuple[List[str], List[str]]:
    """Получает и публикует топ-метрики (gainers/losers)."""
    logger.debug("Fetching top metrics...")
    try:
        top_gainers, top_losers = get_sorted_tickers_by_change()
        if not top_gainers and not top_losers:
            logger.warning("No data for top metrics publication")
            return [], []
        if top_gainers:
            publish_signal_to_stream('top:gainers', {
                'type': 'top:gainers',
                'payload': format_entries(top_gainers)
            })
        if top_losers:
            publish_signal_to_stream('top:losers', {
                'type': 'top:losers',
                'payload': format_entries(top_losers)
            })
        gainers = [t.get("symbol", "") for t in top_gainers if t.get("symbol")]
        losers = [t.get("symbol", "") for t in top_losers if t.get("symbol")]
        return gainers, losers
    except Exception as e:
        logger.error("Error fetching and publishing top metrics: %s", e)
        return [], []


async def run_metrics_screener() -> None:
    """Основная функция запуска скринера метрик."""
    logger.info("Starting metrics screener...")
    try:
        gainer_symbols, loser_symbols = fetch_and_publish_top_metrics()
        logger.debug("Processed gainers: %d, losers: %d",
                     len(gainer_symbols), len(loser_symbols))

        volume_data = get_volume_data()
        if volume_data:
            # Сохраняем данные volume в Redis
            try:
                redis_client = get_redis()
                repo = RedisTradeRepository(redis_client)
                repo.persist_volume_data(volume_data)
                logger.debug("Persisted %d volume records to Redis", len(volume_data))
            except Exception as e:
                logger.warning("Error persisting volume data to Redis: %s", e)

            # Публикуем массив как событие типа 'volume'
            try:
                loop = asyncio.get_running_loop()
                ts = int(loop.time() * 1000)
            except RuntimeError:
                import time
                ts = get_ny_time_millis()

            publish_signal_to_stream('signal:volume', {
                'type': 'volume',
                'payload': format_volume_entries(volume_data),
                'timestamp': ts
            })
            logger.debug("Published %d volume records", len(volume_data))

        funding_data = get_funding_rate_data()
        if funding_data:
            try:
                loop = asyncio.get_running_loop()
                ts = int(loop.time() * 1000)
            except RuntimeError:
                import time
                ts = get_ny_time_millis()

            publish_signal_to_stream('signal:funding', {
                'type': 'funding',
                'payload': format_funding_entries(funding_data),
                'timestamp': ts
            })
            logger.debug("Published %d funding rate records", len(funding_data))

        logger.info("Metrics screener completed successfully")
    except Exception as e:
        logger.error("Error in metrics screener: %s", e)