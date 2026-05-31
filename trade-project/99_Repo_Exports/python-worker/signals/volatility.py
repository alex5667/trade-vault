"""
Модуль сигналов базовой волатильности (volatilitySpike).

Назначение:
- Рассчитать мгновенную волатильность свечи ( (high - low) / open * 100 ).
- Сформировать и опубликовать сигнал при превышении порога `VOLATILITY_SPIKE_MIN_PCT`.
- Публиковать сигнал только по ЗАКРЫТОЙ свече (Binance: kline['x'] == True), чтобы избежать шума.
- Добавлять время открытия свечи `t` для точной дедупликации в publisher.
"""

import logging
from datetime import UTC, datetime

from core.config import DEFAULT_INTERVAL, REDIS_CHANNEL_VOLATILITY, VOLATILITY_SPIKE_MIN_PCT
from core.ticker_data import get_ticker_24h_metrics
from publisher.stream_publisher import publish_signal_to_stream

logger = logging.getLogger(__name__)


def check_volatility_spike(symbol: str, high: float, low: float, open_price: float, volume: float,
                          high_24h: float, low_24h: float, price_change_percent: float,
                          volume_change_percent: float, open_time_ms: int | None = None) -> dict | None:
    """
    Проверяет волатильность цены и отправляет сигнал в Redis Stream, если обнаружен всплеск.

    Аргументы:
        symbol: Торговая пара (символ), например 'BTCUSDT'
        high: Максимум свечи
        low: Минимум свечи
        open_price: Цена открытия свечи
        volume: Объем свечи
        high_24h: Максимум за 24 часа (на будущее; сейчас используется для расчета относительного изменения)
        low_24h: Минимум за 24 часа (на будущее)
        price_change_percent: Изменение цены в процентах (зарезервировано)
        volume_change_percent: Изменение объема в процентах (зарезервировано)
        open_time_ms: Время открытия свечи (мс) — используется для дедупликации сигналов

    Возвращает:
        dict с данными сигнала или None, если порог не превышен/данные некорректны.
    """

    # Защита от деления на ноль/некорректных данных
    if open_price <= 0:
        return None

    # Рассчитываем текущую волатильность как процент от цены открытия
    current_volatility = ((high - low) / open_price) * 100

    # Рассчитываем волатильность за 24 часа (для сопоставления и вспомогательной информации)
    volatility_24h = ((high_24h - low_24h) / open_price) * 100

    # Рассчитываем изменение волатильности (разность между текущей и 24h)
    volatility_change = current_volatility - volatility_24h

    # Используем порог из конфигурации
    volatility_threshold = VOLATILITY_SPIKE_MIN_PCT

    if current_volatility > volatility_threshold:
        # Формируем данные сигнала (каждому полю дан комментарий)
        signal = {
            'type': 'volatilitySpike',                 # тип сигнала
            'symbol': symbol,                          # торговый символ (например, BTCUSDT)
            'volatility': round(current_volatility, 2),# относительная волатильность (%) текущей свечи
            'volatilityChange': round(volatility_change, 2),  # изменение волатильности относительно 24h (%)
            'volatility_24h': round(volatility_24h, 2),       # справочная волатильность за 24 часа (%)
            'threshold': volatility_threshold,         # порог волатильности (%) для триггера
            'high': high,                              # максимум свечи
            'low': low,                                # минимум свечи
            'open': open_price,                        # цена открытия свечи
            'volume': volume,                          # объём за свечу
            'price_change_percent': price_change_percent,      # изменение цены (%) (зарезервировано)
            'volume_change_percent': volume_change_percent,    # изменение объёма (%) (зарезервировано)
            'timestamp': datetime.now(UTC).isoformat(),   # время формирования сигнала (ISO, UTC)
            'interval': DEFAULT_INTERVAL,              # интервал свечи (ожидается '1m')
            't': open_time_ms                          # время открытия свечи (мс) для дедупликации
        }

        logger.debug("Volatility spike for %s: %.2f%% (change: %.2f%%)",
                     symbol, current_volatility, volatility_change)

        # Отправляем сигнал в Redis Stream
        result = publish_signal_to_stream(REDIS_CHANNEL_VOLATILITY, signal)

        if result:
            logger.info("Volatility signal for %s sent to Redis Stream", symbol)
        else:
            logger.error("Failed to send volatility signal for %s", symbol)

        return signal

    return None


def handle_volatility(kline):
    """
    Обрабатывает данные kline для анализа волатильности.

    Обязательно публикуем сигнал ТОЛЬКО по закрытой свече (kline['x'] == True)
    для исключения многократных публикаций в течение формирования свечи.

    Аргументы:
        kline: Словарь с полями свечи Binance (ключ 'k' из WS-сообщения)
    """
    try:
        # Публикуем сигнал только по закрытой свече, чтобы избежать спама за минуту
        is_closed = bool(kline.get('x'))
        if not is_closed:
            return

        symbol = kline['s']
        high = float(kline['h'])
        low = float(kline['l'])
        open_price = float(kline['o'])
        volume = float(kline['v'])
        open_time_ms = int(kline.get('t') or 0)

        # Получаем реальные 24h метрики из Redis Stream
        ticker_metrics = get_ticker_24h_metrics(symbol)

        if ticker_metrics:
            # Используем реальные данные
            high_24h = ticker_metrics['high_24h']
            low_24h = ticker_metrics['low_24h']
            price_change_percent = ticker_metrics['price_change_percent']
            volume_change_percent = 0.0  # Пока не реализовано в API
        else:
            # Fallback на 0 если данные недоступны
            logger.warning("Ticker data unavailable for %s, using 0", symbol)
            high_24h = 0.0
            low_24h = 0.0
            price_change_percent = 0.0
            volume_change_percent = 0.0

        # Вызываем проверку волатильности
        check_volatility_spike(
            symbol=symbol,
            high=high,
            low=low,
            open_price=open_price,
            volume=volume,
            high_24h=high_24h,
            low_24h=low_24h,
            price_change_percent=price_change_percent,
            volume_change_percent=volume_change_percent,
            open_time_ms=open_time_ms
        )

    except Exception as e:
        logger.error("Error handling volatility for kline: %s", e)
