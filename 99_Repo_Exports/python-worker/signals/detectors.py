"""
Detectors - тестируемые функции для детекции сигналов ордер-флоу.

ФУНКЦИОНАЛ:
- Z-score расчет для delta spike detection
- Weak progress детекция (absorption)
- Реальный OBI расчет из Order Book
- Условия для absorption сигналов

ИСПОЛЬЗОВАНИЕ:
- z = zscore(latest_value, window_of_values)
- is_weak = weak_progress(bar_range, atr, threshold)
- obi = obi_from_book(book_data, depth=5)
- is_abs = is_absorption(z, weak, near_level, z_threshold)

ПРЕИМУЩЕСТВА:
- Чистые функции (легко тестировать)
- Нет зависимостей от Redis/состояния
- Unit тесты в tests/test_detectors.py
"""

from statistics import mean, pstdev
from typing import Any


def zscore(latest: float, window: list[float]) -> float:
    """
    Вычисляет Z-score для последнего значения относительно окна.
    
    Args:
        latest: Последнее значение (например, delta)
        window: Окно исторических значений
        
    Returns:
        Z-score (количество стандартных отклонений от среднего)
    """
    if not window or len(window) < 30:
        return 0.0

    m = mean(window)
    s = pstdev(window)

    if s == 0 or s < 1e-9:
        return 0.0

    return (latest - m) / s


def weak_progress(bar_range: float, atr: float, threshold: float = 0.10) -> bool:
    """
    Проверяет условие "слабого прогресса" цены.
    
    Используется для детекции absorption: большой объем торговли,
    но цена двигается слабо относительно ATR.
    
    Args:
        bar_range: Диапазон текущего бара (high - low)
        atr: Значение ATR
        threshold: Порог (по умолчанию 0.10 = 10% от ATR)
        
    Returns:
        True если прогресс слабый (absorption condition)
    """
    if atr <= 0:
        return False

    return (abs(bar_range) / atr) <= threshold


def obi_from_book(book: dict[str, Any] | None, depth: int = 5) -> float | None:
    """
    Вычисляет Order Book Imbalance (OBI) из DOM snapshot.
    
    OBI = (bid_volume - ask_volume) / (bid_volume + ask_volume)
    
    Args:
        book: Словарь с ключами "bids" и "asks"
              bids: [[price, volume], ...]
              asks: [[price, volume], ...]
        depth: Количество уровней для учета (default 5)
        
    Returns:
        OBI в диапазоне [-1, 1] или None если book пустой
        +1 = сильное преобладание bid
        -1 = сильное преобладание ask
         0 = баланс
    """
    if not book:
        return None

    bids = book.get("bids", [])[:depth]
    asks = book.get("asks", [])[:depth]

    # Суммируем объемы
    bid_volume = sum(max(0.0, float(v)) for _, v in bids)
    ask_volume = sum(max(0.0, float(v)) for _, v in asks)

    total_volume = bid_volume + ask_volume

    if total_volume <= 0:
        return 0.0

    # OBI в диапазоне [-1, 1]
    return (bid_volume - ask_volume) / total_volume


def is_absorption(z: float, weak: bool, near_level: bool, z_threshold: float = 3.0) -> bool:
    """
    Проверяет условия для absorption сигнала.
    
    Absorption возникает когда:
    - Сильный delta spike (|z| >= threshold)
    - Слабый прогресс цены (weak progress)
    - Цена у ключевого уровня (Pivot, S/R)
    
    Args:
        z: Z-score delta
        weak: Флаг weak progress
        near_level: Флаг близости к уровню
        z_threshold: Порог Z-score (default 3.0)
        
    Returns:
        True если все условия absorption выполнены
    """
    return weak and near_level and abs(z) >= z_threshold


def obi_is_sustained(obi_buffer: list[tuple], threshold: float = 0.5) -> bool:
    """
    Проверяет устойчивость OBI во времени.
    
    Args:
        obi_buffer: Буфер с (timestamp, obi) парами
        threshold: Порог среднего OBI для "sustained" condition
        
    Returns:
        True если OBI устойчиво в одном направлении
    """
    if not obi_buffer:
        return False

    # Вычисляем средний OBI
    avg_obi = sum(obi for _, obi in obi_buffer) / len(obi_buffer)

    return abs(avg_obi) >= threshold


def classify_delta_by_aggressor(last: float, bid: float, ask: float, volume: float) -> float:
    """
    Классифицирует направление сделки по цене исполнения.
    
    Логика:
    - last >= ask → агрессивная покупка (+ volume)
    - last <= bid → агрессивная продажа (- volume)
    - иначе → направление по spread
    
    Args:
        last: Цена последней сделки
        bid: Bid цена
        ask: Ask цена
        volume: Объем сделки
        
    Returns:
        Signed delta (+volume для покупок, -volume для продаж)
    """
    if last and ask and last >= ask:
        return +volume  # Агрессивная покупка

    if last and bid and last <= bid:
        return -volume  # Агрессивная продажа

    # Fallback: по направлению spread
    if ask > bid:
        return +volume
    else:
        return -volume

