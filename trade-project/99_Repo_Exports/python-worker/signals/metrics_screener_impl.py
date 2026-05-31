"""
Реализация скринера метрик: сбор данных и переэкспорт утилит для удобного импорта.
"""
import asyncio

from .data_access import get_24h_ticker_symbols, get_funding_rate_data, get_ticker_data
from .formatters import (
    format_entries,
    format_funding_entries,
    format_volume_entries,
)
from .orchestrator import fetch_and_publish_top_metrics, run_metrics_screener
from .publishers import publish_list
from .sorting import get_sorted_tickers_by_change
from .volumes import get_volume_data

__all__ = [
    'get_24h_ticker_symbols',
    'get_ticker_data',
    'get_sorted_tickers_by_change',
    'format_entries',
    'publish_list',
    'fetch_and_publish_top_metrics',
    'get_volume_data',
    'get_funding_rate_data',
    'format_volume_entries',
    'format_funding_entries',
    'run_metrics_screener',
]

if __name__ == "__main__":
    asyncio.run(run_metrics_screener())
