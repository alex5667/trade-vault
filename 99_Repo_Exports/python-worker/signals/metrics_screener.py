"""
Тонкая оболочка‑обёртка для стабильности импортов.
Полная реализация находится в `metrics_screener_impl.py`.
"""
from .metrics_screener_impl import (
    get_24h_ticker_symbols,
    get_ticker_data,
    get_sorted_tickers_by_change,
    format_entries,
    publish_list,
    fetch_and_publish_top_metrics,
    get_volume_data,
    get_funding_rate_data,
    format_volume_entries,
    format_funding_entries,
    run_metrics_screener,
)

# Re-export names for external modules
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