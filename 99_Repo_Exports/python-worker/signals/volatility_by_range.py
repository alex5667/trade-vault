from utils.time_utils import get_ny_time_millis
"""
Модуль сигналов волатильности по диапазону (volatilityRange).

Назначение:
- Отслеживать аномально широкий диапазон свечи относительно средней ширины диапазона по истории.
- История хранится отдельно по каждому символу в `KlineDataHandler`.
- Сигнал формируется, если одновременно:
  1) текущий диапазон > среднего диапазона * RANGE_MULTIPLIER_THRESHOLD
  2) относительная волатильность (range/open*100) > VOLATILITY_RANGE_MIN_PCT
- Публикация выполняется только по ЗАКРЫТОЙ свече.
"""

from publisher.stream_publisher import publish_signal_to_stream
from signals.history_utils import update_and_check_history
from core.config import VOLATILITY_RANGE_MIN_PCT, REDIS_CHANNEL_VOLATILITY_RANGE, RANGE_MULTIPLIER_THRESHOLD
import json
import redis
import sys
from typing import Optional


import time
def calculate_volatility_by_range(kline: dict, avg_range: float) -> Optional[dict]:
    """
    Считает метрики по свече и возвращает сигнал (dict) либо None.
    Историю НЕ трогаем здесь.

    Аргументы:
        kline: Сырые данные свечи Binance (ключи: 'o','h','l','c','v','i','t', 's')
        avg_range: Средний диапазон (в абсолютных значениях цены) по истории для данного символа

    Возвращает:
        dict сигнала или None, если пороги не выполнены/данные некорректны.
    """
    symbol = kline.get('s')
    high = float(kline.get('h', 0))
    low = float(kline.get('l', 0))
    open_price = float(kline.get('o', 0))
    close_price = float(kline.get('c', 0))
    volume = float(kline.get('v', 0))
    interval = kline.get('i', '1m')
    timestamp = kline.get('t', get_ny_time_millis())

    if not symbol or open_price <= 0:
        print(f"⚠️ Недостаточно данных: symbol={symbol}, open={open_price}")
        sys.stdout.flush()
        return None

    price_range = high - low
    range_ratio = (price_range / avg_range * 100) if avg_range > 0 else 0.0
    volatility_pct = price_range / open_price * 100
    price_change_pct = (close_price - open_price) / open_price * 100

    if volatility_pct <= VOLATILITY_RANGE_MIN_PCT:
        return None

    signal = {
        'type': 'volatilityRange',        # тип сигнала
        'symbol': symbol,                 # торговый символ (например, BTCUSDT)
        'range': round(price_range, 8),   # текущий абсолютный диапазон цены (high-low)
        'avgRange': round(avg_range or 0.0, 8),  # средний диапазон по истории (скользящее окно)
        'volatility': round(volatility_pct, 2),  # относительная волатильность (%) = range/open*100
        'priceChangePct': round(price_change_pct, 2),  # изменение цены (%) = (close-open)/open*100
        'threshold': VOLATILITY_RANGE_MIN_PCT,   # порог минимальной относительной волатильности (%)
        'high': high,                    # максимум свечи
        'low': low,                      # минимум свечи
        'open': open_price,              # цена открытия свечи
        'close': close_price,            # цена закрытия свечи
        'volume': volume,                # объём за свечу
        'timestamp': timestamp,          # время открытия свечи (мс, из Binance kline 't')
        'interval': interval,            # интервал свечи (ожидается '1m')
        'rangeRatio': round(range_ratio, 2),  # отношение текущего диапазона к среднему (%)
        't': timestamp,                  # время открытия свечи (мс) для дедупликации
    }
    return signal


def handle_volatility_by_range(kline: dict, history: list) -> None:
    """
    ЕДИНСТВЕННОЕ место работы с историей:
    1) считаем текущий диапазон,
    2) берём СРЕДНЕЕ ПО ПРЕДЫДУЩИМ (без включения текущей свечи),
    3) проверяем условия/публикуем,
    4) добавляем текущую свечу в историю.

    Аргументы:
        kline: Сырые данные закрытой свечи Binance
        history: Мутируемый список значений диапазонов для данного символа (maintained in-place)
    """
    try:
        # Обрабатываем только закрытую свечу, чтобы не триггерить сигнал несколько раз за минуту
        is_closed = bool(kline.get('x'))
        if not is_closed:
            return

        current_range = float(kline['h']) - float(kline['l'])

        # 2) среднее по прошлым (без включения текущей)
        avg_range_prev = update_and_check_history(history, update=False)

        # 3) правило аномалии диапазона
        if avg_range_prev is not None and current_range > (avg_range_prev * RANGE_MULTIPLIER_THRESHOLD):
            signal = calculate_volatility_by_range(kline, avg_range_prev)
            if signal:
                ok = publish_signal_to_stream(REDIS_CHANNEL_VOLATILITY_RANGE, signal)
                if ok:
                    print(f"✅ {signal['symbol']} volatilityRange опубликован "
                          f"(vol={signal['volatility']}%, ratio={signal['rangeRatio']}%)")
                else:
                    print("❌ Ошибка отправки в Redis Stream")
                sys.stdout.flush()

        # 4) теперь обновляем историю добавлением текущего диапазона
        update_and_check_history(history, value=current_range, update=True)

    except KeyError as e:
        print(f"❌ Нет ключа в kline: {e}")
        sys.stdout.flush()
    except ValueError as e:
        print(f"❌ Ошибка преобразования чисел: {e}")
        sys.stdout.flush()
    except redis.exceptions.ConnectionError as e:
        print(f"❌ Ошибка Redis: {e}")
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ handle_volatility_by_range: {e}")
        sys.stdout.flush()


def process_kline_for_volatility_range(kline_data: str, history: Optional[list] = None) -> None:
    """
    Парсим вход, определяем формат, передаём в обработчик.

    Поддерживаются 2 формы:
    - полное WS-сообщение Binance (dict с ключом 'k')
    - непосредственно kline-словарь

    Аргументы:
        kline_data: JSON-строка входных данных
        history: Внешнее хранилище истории; если не передано — создаётся локальный список
    """
    try:
        data = json.loads(kline_data)
        kline = data['k'] if isinstance(data, dict) and 'k' in data else data

        if not isinstance(kline, dict):
            print(f"⚠️ Неизвестный формат данных: {type(kline)}")
            sys.stdout.flush()
            return

        if history is None:
            history = []

        handle_volatility_by_range(kline, history)

    except json.JSONDecodeError as e:
        print(f"❌ Ошибка JSON: {e}")
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ process_kline_for_volatility_range: {e}")
        sys.stdout.flush()
