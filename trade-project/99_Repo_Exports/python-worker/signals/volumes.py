from utils.time_utils import get_ny_time_millis

"""
Утилиты получения и подготовки топа по объёмам торгов по 24h тикерам.
"""
import logging
from typing import Any

from core.config import TOP_VOLUME_LIMIT
from utils.log_throttler import log_throttler

from .data_access import get_24h_ticker_symbols, get_ticker_data

logger = logging.getLogger(__name__)


def get_volume_data() -> list[dict[str, Any]]:
    """
    Возвращает топ-10 по quoteVolume среди доступных 24h тикеров.

    Логика отбора:
    1. Сначала отбираем тикеры за последние 24 часа (по closeTime)
    2. Затем сортируем по объему торгов (quoteVolume)

    Если в тикере присутствует изменение объёма в процентах, добавляет поле 'change' (float).
    Поддерживаемые ключи источника: 'volumeChangePercent', 'quoteVolumeChangePercent',
    'volumeChangePct', 'volume_change_percent'.
    """
    symbols = get_24h_ticker_symbols()
    if not symbols:
        return []

    # Текущее время в миллисекундах
    current_time_ms = get_ny_time_millis()
    twenty_four_hours_ago_ms = current_time_ms - (24 * 60 * 60 * 1000)

    logger.debug("Filtering volumes: cutoff=%d ms", twenty_four_hours_ago_ms)

    volume_data: list[dict[str, Any]] = []
    filtered_count = 0
    total_count = 0

    for symbol in symbols:
        total_count += 1
        ticker = get_ticker_data(symbol)

        if ticker and isinstance(ticker, dict) and "quoteVolume" in ticker:
            try:
                # Проверяем время закрытия тикера
                close_time = ticker.get("closeTime")
                if not close_time:
                    logger.debug("Ticker %s has no closeTime, skipping", symbol)
                    continue

                # Проверяем, что тикер не старше 24 часов
                if close_time < twenty_four_hours_ago_ms:
                    log_throttler.log_with_count(
                        "expired_ticker_volumes",
                        f"⏰ Тикер {symbol} устарел: {close_time} < {twenty_four_hours_ago_ms}",
                        10000
                    )
                    continue

                quote_volume = float(ticker.get("quoteVolume", 0))
                if quote_volume <= 0:
                    continue

                filtered_count += 1

                entry: dict[str, Any] = {
                    "symbol": ticker.get("symbol", ""),
                    "volume": ticker.get("volume", ""),
                    "quoteVolume": quote_volume,
                    "timestamp": close_time,
                    "close_time": close_time,
                }

                # Пытаемся извлечь изменение объёма в процентах, если поле существует
                change_keys = [
                    "volumeChangePercent",
                    "quoteVolumeChangePercent",
                    "volumeChangePct",
                    "volume_change_percent",
                ]
                for key in change_keys:
                    if key in ticker and ticker[key] is not None:
                        try:
                            entry["change"] = float(ticker[key])
                            break
                        except (ValueError, TypeError):
                            continue

                volume_data.append(entry)

            except (ValueError, TypeError) as e:
                logger.warning("Error processing ticker %s: %s", symbol, e)
                continue
        elif ticker and not isinstance(ticker, dict):
            logger.warning("Ticker data for %s is not a dict: %s", symbol, type(ticker))

    logger.debug("Filtered %d/%d tickers (last 24h)", filtered_count, total_count)

    if not volume_data:
        logger.warning("No current volume data for last 24h")
        return []

    # Сортируем по объему торгов
    volume_data.sort(key=lambda x: x["quoteVolume"], reverse=True)

    result = volume_data[:TOP_VOLUME_LIMIT]

    if result:
        logger.info("Top volumes: %d entries, best: %s %.0f quote",
                    len(result),
                    result[0].get('symbol', 'N/A'),
                    result[0].get('quoteVolume', 0.0))

    return result
