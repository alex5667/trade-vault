from utils.time_utils import get_ny_time_millis

"""
Оркестратор метрик: сбор топ‑метрик, объёмов и funding; форматирование и публикация в стримы.
Улучшенная версия с экспортом всех сигналов в Redis на порт 6380.
"""
import asyncio
import logging

from publisher.stream_publisher import publish_signal_to_stream

from .data_access import get_funding_rate_data
from .formatters import format_entries, format_funding_entries, format_volume_entries
from .signal_exporter import export_all_signals_to_redis_6380
from .sorting import get_sorted_tickers_by_change
from .volumes import get_volume_data

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    """Current timestamp in milliseconds."""
    try:
        loop = asyncio.get_running_loop()
        return int(loop.time() * 1000)
    except RuntimeError:
        return get_ny_time_millis()


def fetch_and_publish_top_metrics() -> tuple[list[str], list[str]]:
    """Получает и публикует топ-метрики (gainers/losers)."""
    logger.debug("Fetching top metrics...")
    try:
        top_gainers, top_losers = get_sorted_tickers_by_change()
        if not top_gainers and not top_losers:
            logger.warning("No data for top metrics publication")
            return [], []

        # Публикуем в существующие стримы
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

        # Экспортируем в Redis на порт 6380
        export_all_signals_to_redis_6380(
            gainers=format_entries(top_gainers) if top_gainers else None,
            losers=format_entries(top_losers) if top_losers else None,
        )

        gainers = [t.get("symbol", "") for t in top_gainers if t.get("symbol")]
        losers = [t.get("symbol", "") for t in top_losers if t.get("symbol")]
        return gainers, losers
    except Exception as e:
        logger.error("Error fetching top metrics: %s", e)
        return [], []


async def run_metrics_screener() -> None:
    """Основная функция запуска скринера метрик."""
    logger.info("Starting metrics screener (enhanced)...")
    try:
        gainer_symbols, loser_symbols = fetch_and_publish_top_metrics()
        logger.debug("Processed gainers: %d, losers: %d",
                     len(gainer_symbols), len(loser_symbols))

        volume_data = get_volume_data()
        if volume_data:
            publish_signal_to_stream('signal:volume', {
                'type': 'volume',
                'payload': format_volume_entries(volume_data),
                'timestamp': _now_ms()
            })
            export_all_signals_to_redis_6380(volume=format_volume_entries(volume_data))
            logger.debug("Published %d volume records", len(volume_data))

        funding_data = get_funding_rate_data()
        if funding_data:
            publish_signal_to_stream('signal:funding', {
                'type': 'funding',
                'payload': format_funding_entries(funding_data),
                'timestamp': _now_ms()
            })
            export_all_signals_to_redis_6380(funding=format_funding_entries(funding_data))
            logger.debug("Published %d funding rate records", len(funding_data))

        logger.info("Metrics screener (enhanced) completed successfully")
    except Exception as e:
        logger.error("Error in metrics screener: %s", e)


def export_volatility_signals_to_redis_6380(
    volatility_by_range_data: dict = None,
    volatility_spike_data: dict = None
) -> None:
    """Экспортирует сигналы волатильности в Redis на порт 6380."""
    try:
        export_all_signals_to_redis_6380(
            volatility_by_range=volatility_by_range_data,
            volatility_spike=volatility_spike_data
        )
        logger.info("Volatility signals exported to Redis (port 6380)")
    except Exception as e:
        logger.error("Error exporting volatility signals: %s", e)


def export_all_available_signals_to_redis_6380() -> dict:
    """Экспортирует все доступные сигналы в Redis на порт 6380."""
    try:
        top_gainers, top_losers = get_sorted_tickers_by_change()
        volume_data = get_volume_data()
        funding_data = get_funding_rate_data()

        results = export_all_signals_to_redis_6380(
            gainers=format_entries(top_gainers) if top_gainers else None,
            losers=format_entries(top_losers) if top_losers else None,
            volume=format_volume_entries(volume_data) if volume_data else None,
            funding=format_funding_entries(funding_data) if funding_data else None
        )

        successful = sum(1 for success in results.values() if success)
        logger.info("Exported %d/%d signal types to Redis (port 6380)",
                    successful, len(results))
        return results
    except Exception as e:
        logger.error("Error exporting all signals: %s", e)
        return {}
