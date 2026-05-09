from utils.time_utils import get_ny_time_millis

"""
Форматирование данных для публикации: тикеры, объёмы и ставки финансирования.
Содержит функции приведения структур к единым полям для последующей отправки в стримы.
"""
import logging
from typing import Any
import contextlib

logger = logging.getLogger(__name__)


def format_entries(tickers: list[dict]) -> list[dict[str, Any]]:
    """Форматирует список тикеров для публикации."""
    formatted: list[dict[str, Any]] = []
    for ticker in tickers:
        try:
            # Вычисляем процент изменения: приоритет у поля Binance 'priceChangePercent'
            change_val = 0.0
            raw = ticker.get('priceChangePercent')
            if raw is not None:
                try:
                    change_val = float(raw)
                except (ValueError, TypeError):
                    change_val = 0.0
            elif 'change_percent' in ticker:
                try:
                    change_val = float(ticker.get('change_percent', 0))
                except (ValueError, TypeError):
                    change_val = 0.0
            elif 'change_percent_float' in ticker:
                try:
                    change_val = float(ticker.get('change_percent_float', 0))
                except (ValueError, TypeError):
                    change_val = 0.0

            formatted.append({
                "symbol": ticker.get("symbol", ""),
                "priceChangePercent": f"{change_val:+.2f}",
                "price": ticker.get("lastPrice", ""),
                "volume": ticker.get("volume", ""),
                "quoteVolume": ticker.get("quoteVolume", ""),
                "timestamp": ticker.get("closeTime", get_ny_time_millis()),
            })
        except Exception as e:
            logger.warning("Error formatting ticker %s: %s", ticker.get('symbol', 'unknown'), e)
            continue
    return formatted


def format_volume_entries(volume_data: list[dict]) -> list[dict[str, Any]]:
    """Форматирует список объемов для публикации. Добавляет 'change' как +/-% строку, если доступно."""
    formatted: list[dict[str, Any]] = []
    for entry in volume_data:
        try:
            item: dict[str, Any] = {
                "symbol": entry.get("symbol", ""),
                "volume": entry.get("volume", ""),
                "quoteVolume": f"{entry.get('quoteVolume', 0):,.0f}",
                "timestamp": entry.get("timestamp", get_ny_time_millis()),
            }
            if "change" in entry and entry.get("change") is not None:
                with contextlib.suppress(ValueError, TypeError):
                    item["change"] = f"{float(entry.get('change', 0)):+.2f}%"
            formatted.append(item)
        except Exception as e:
            logger.warning("Error formatting volume for %s: %s", entry.get('symbol', 'unknown'), e)
            continue
    return formatted


def format_funding_entries(funding_data: list[dict]) -> list[dict[str, Any]]:
    """Форматирует список funding rates для публикации."""
    formatted: list[dict[str, Any]] = []
    for entry in funding_data:
        try:
            formatted.append({
                "symbol": entry.get("symbol", ""),
                "fundingRate": f"{entry.get('fundingRate', 0):.6f}",
                "fundingRatePercent": f"{entry.get('fundingRate', 0) * 100:.4f}%",
                "fundingTime": entry.get("fundingTime", get_ny_time_millis()),
            })
        except Exception as e:
            logger.warning("Error formatting funding entry for %s: %s", entry.get('symbol', 'unknown'), e)
            continue
    return formatted


def format_ticker_data(ticker: dict[str, Any]) -> dict[str, Any]:
    """Форматирует данные тикера для вывода."""
    try:
        if not isinstance(ticker, dict):
            if isinstance(ticker, list):
                logger.warning("Ticker data is a list, expected dict")
                if len(ticker) > 0 and isinstance(ticker[0], dict):
                    ticker = ticker[0]
                else:
                    return {}
            else:
                logger.warning("Ticker data is not a dict: %s", type(ticker))
                return {}

        # Подготовка процента изменения
        change_val = 0.0
        raw = ticker.get('priceChangePercent')
        if raw is not None:
            try:
                change_val = float(raw)
            except (ValueError, TypeError):
                change_val = 0.0
        elif 'change_percent' in ticker:
            try:
                change_val = float(ticker.get('change_percent', 0))
            except (ValueError, TypeError):
                change_val = 0.0
        elif 'change_percent_float' in ticker:
            try:
                change_val = float(ticker.get('change_percent_float', 0))
            except (ValueError, TypeError):
                change_val = 0.0

        return {
            "symbol": ticker.get("symbol", ""),
            "priceChangePercent": f"{change_val:+.2f}",
            "price": ticker.get("lastPrice", ""),
            "volume": ticker.get("volume", ""),
            "quoteVolume": ticker.get("quoteVolume", ""),
            "timestamp": ticker.get("closeTime", get_ny_time_millis()),
        }
    except Exception as e:
        sym = ticker.get('symbol', 'unknown') if isinstance(ticker, dict) else 'unknown'
        logger.warning("Error formatting ticker data for %s: %s", sym, e)
        return {}


def format_ticker_signal(ticker: dict[str, Any]) -> dict[str, Any]:
    """Форматирует тикер в сигнал."""
    return {
        "type": "ticker",
        "symbol": ticker.get("symbol", ""),
        "price": ticker.get("price", "0"),
        "change": ticker.get("change", "0"),
        "changePercent": ticker.get("changePercent", "0"),
        "volume": ticker.get("volume", "0"),
        "timestamp": ticker.get("closeTime", get_ny_time_millis()),  # Используем правильное NY время
        "source": "binance"
    }

def format_entry_signal(entry: dict[str, Any]) -> dict[str, Any]:
    """Форматирует entry в сигнал."""
    return {
        "type": "entry",
        "symbol": entry.get("symbol", ""),
        "side": entry.get("side", ""),
        "price": entry.get("price", "0"),
        "quantity": entry.get("quantity", "0"),
        "timestamp": entry.get("timestamp", get_ny_time_millis()),  # Используем правильное NY время
        "source": "telegram"
    }

def format_funding_signal(entry: dict[str, Any]) -> dict[str, Any]:
    """Форматирует funding в сигнал."""
    return {
        "type": "funding",
        "symbol": entry.get("symbol", ""),
        "rate": entry.get("rate", "0"),
        "nextFundingTime": entry.get("nextFundingTime", "0"),
        "fundingTime": entry.get("fundingTime", get_ny_time_millis()),  # Используем правильное NY время
        "source": "binance"
    }


def format_volume_signal(ticker: dict[str, Any]) -> dict[str, Any]:
    """Форматирует volume в сигнал."""
    return {
        "type": "volume",
        "symbol": ticker.get("symbol", ""),
        "volume": ticker.get("volume", "0"),
        "price": ticker.get("price", "0"),
        "timestamp": ticker.get("closeTime", get_ny_time_millis()),  # Используем правильное NY время
        "source": "binance"
    }
