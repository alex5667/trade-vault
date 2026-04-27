from utils.time_utils import get_ny_time_millis
"""
Сортировка данных для сигналов по различным критериям:
Модуль содержит утилиты для фильтрации по времени, объёму и рейтингу.
"""
import logging
import time
from typing import Dict, List, Tuple

from .data_access import get_24h_ticker_symbols, get_ticker_data
from core.config import TOP_GAINERS_LIMIT, TOP_LOSERS_LIMIT
from utils.log_throttler import log_throttler

logger = logging.getLogger(__name__)


def get_sorted_tickers_by_change() -> Tuple[List[Dict], List[Dict]]:
    """
    Возвращает топ-10 gainers и losers по изменению цены.
    
    Логика отбора:
    1. Сначала отбираем тикеры за последние 24 часа (по closeTime)
    2. Затем сортируем по величине изменения цены (priceChangePercent)
    """
    symbols = get_24h_ticker_symbols()
    if not symbols:
        return [], []

    # Текущее время в миллисекундах
    current_time_ms = get_ny_time_millis()
    twenty_four_hours_ago_ms = current_time_ms - (24 * 60 * 60 * 1000)

    logger.debug("Filtering tickers: cutoff=%d ms", twenty_four_hours_ago_ms)
    
    tickers_with_change = []
    filtered_count = 0
    total_count = 0
    
    for symbol in symbols:
        total_count += 1
        ticker = get_ticker_data(symbol)
        
        if ticker and isinstance(ticker, dict) and "priceChangePercent" in ticker:
            try:
                # Проверяем время закрытия тикера
                close_time = ticker.get("closeTime")
                if not close_time:
                    logger.debug("Ticker %s has no closeTime, skipping", symbol)
                    continue
                
                # Проверяем, что тикер не старше 24 часов
                if close_time < twenty_four_hours_ago_ms:
                    log_throttler.log_with_count(
                        "expired_ticker_sorting", 
                        f"⏰ Тикер {symbol} устарел: {close_time} < {twenty_four_hours_ago_ms}",
                        10000
                    )
                    continue
                
                change_percent = float(ticker["priceChangePercent"])
                filtered_count += 1
                
                tickers_with_change.append({
                    "symbol": ticker.get("symbol", ""),
                    "change_percent": change_percent,
                    "close_time": close_time,
                    "data": ticker
                })
                
            except (ValueError, TypeError) as e:
                logger.warning("Error processing ticker %s: %s", symbol, e)
                continue
        elif ticker and not isinstance(ticker, dict):
            logger.warning("Ticker data for %s is not a dict: %s", symbol, type(ticker))

    logger.debug("Filtered %d/%d tickers (last 24h)", filtered_count, total_count)

    if not tickers_with_change:
        logger.warning("No current tickers for last 24h")
        return [], []

    tickers_with_change.sort(key=lambda x: x["change_percent"], reverse=True)

    top_gainers = [ticker["data"] for ticker in tickers_with_change[:TOP_GAINERS_LIMIT]]
    top_losers = [ticker["data"] for ticker in tickers_with_change[-TOP_LOSERS_LIMIT:]]

    if top_gainers:
        logger.debug("Best gainer: %s +%s%%",
                     top_gainers[0].get('symbol', 'N/A'),
                     top_gainers[0].get('priceChangePercent', 'N/A'))
    if top_losers:
        logger.debug("Worst loser: %s %s%%",
                     top_losers[-1].get('symbol', 'N/A'),
                     top_losers[-1].get('priceChangePercent', 'N/A'))

    return top_gainers, top_losers 