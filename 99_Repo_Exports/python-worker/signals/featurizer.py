"""
Модуль фичеринга для анализа потока ордеров XAUUSD.

Предоставляет скользящую статистику и извлечение фич из тиковых и стаканных данных.
"""

from collections import deque
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple


class Rolling:
    """
    Эффективное скользящее окно с вычислением mean/std.

    Использует онлайн-алгоритм через sum/sumsq для O(1) обновления.
    Хранение через deque для O(1) pop с левого конца.
    """

    def __init__(self, size: int) -> None:
        """
        Инициализация скользящего окна.

        Args:
            size: Максимальный размер окна
        """
        self.size = size
        self.buf: deque = deque(maxlen=size)
        self.sum: float = 0.0
        self.sumsq: float = 0.0

    def add(self, x: float) -> None:
        """Добавить значение в скользящее окно."""
        if len(self.buf) == self.size:
            y = self.buf[0]  # leftmost (будет вытолкнут)
            self.sum -= y
            self.sumsq -= y * y
        self.buf.append(x)
        self.sum += x
        self.sumsq += x * x

    def mean(self) -> Optional[float]:
        """Вычислить среднее значение текущего окна."""
        n = len(self.buf)
        if n == 0:
            return None
        return self.sum / n

    def std(self) -> Optional[float]:
        """Вычислить стандартное отклонение текущего окна."""
        n = len(self.buf)
        if n < 2:
            return None
        m = self.sum / n
        var = max(0.0, self.sumsq / n - m * m)
        return var ** 0.5

    def __len__(self) -> int:
        """Вернуть текущий размер окна."""
        return len(self.buf)


def classify_delta(tick: Dict) -> float:
    """
    Классифицировать дельту тика (знаковый объем) используя tick rule.

    Правила:
    - Если last >= ask: покупка (положительная дельта)
    - Если last <= bid: продажа (отрицательная дельта)
    - Иначе: сравнение bid-ask

    Args:
        tick: Данные тика с bid, ask, last, volume

    Returns:
        Знаковый объем (volume) (положительный = покупка, отрицательный = продажа)
    """
    bid = tick.get("bid")
    ask = tick.get("ask")
    last = tick.get("last")
    vol = tick.get("volume", 0.0)

    if last is not None and ask is not None and last >= ask:
        return +vol
    if last is not None and bid is not None and last <= bid:
        return -vol

    # Fallback: compare bid/ask
    return +vol if (ask or 0) > (bid or 0) else -vol


def obi_from_book(book: Dict, depth: int = 5) -> Optional[float]:
    """
    Вычислить Order Book Imbalance из снимка DOM.

    OBI = (BidVolume - AskVolume) / (BidVolume + AskVolume)

    Args:
        book: Стакан (книга ордеров) со списками 'bids' и 'asks'
        depth: Количество уровней для включения

    Returns:
        OBI в диапазоне [-1, 1] или None, если стакан пуст
    """
    if not book:
        return None

    bids = book.get("bids", [])
    asks = book.get("asks", [])

    # Sort and take top N levels
    bids_sorted = sorted(bids, key=lambda x: x[0], reverse=True)[:depth]
    asks_sorted = sorted(asks, key=lambda x: x[0])[:depth]

    # Sum volumes
    bv = sum(max(0.0, float(v)) for _, v in bids_sorted)
    av = sum(max(0.0, float(v)) for _, v in asks_sorted)

    tot = bv + av
    if tot <= 1e-12:
        return 0.0

    return (bv - av) / tot


def make_features(
    tick: Dict,
    book: Optional[Dict],
    rdelta: Rolling
) -> Dict:
    """
    Извлечь фичи (features) из данных тика и стакана.

    Features:
    - ts: метка времени (мс)
    - mid: средняя цена (mid price)
    - spread: спред bid-ask
    - delta: знаковый объем
    - delta_z: z-score дельты
    - obi: дисбаланс стакана (OBI) (если стакан доступен)

    Args:
        tick: Данные тика
        book: Снимок стакана (опционально)
        rdelta: Скользящее окно для статистики дельты

    Returns:
        Словарь фич
    """
    bid = tick.get("bid") or 0.0
    ask = tick.get("ask") or 0.0
    last = tick.get("last") or 0.0

    # Mid price
    mid = (bid + ask) / 2 if (bid and ask) else last

    # Spread
    spread = ask - bid

    # Delta
    d = classify_delta(tick)
    rdelta.add(d)

    # Z-score
    m = rdelta.mean() or 0.0
    s = rdelta.std() or 1e-9
    z = (d - m) / s

    # OBI
    obi = None
    if book:
        obi = obi_from_book(book, depth=5)

    return {
        "ts": int(tick.get("ts", 0)),
        "mid": mid,
        "spread": spread,
        "delta": d,
        "delta_z": z,
        "obi": obi
    }


def compute_rolling_metrics(
    deltas: List[float],
    window: int = 120
) -> Tuple[float, float]:
    """
    Вычислить скользящее среднее и std для списка дельт.

    Args:
        deltas: Список значений дельты
        window: Размер окна

    Returns:
        Кортеж (mean, std)
    """
    if len(deltas) < 2:
        return 0.0, 1.0

    data = deltas[-window:] if len(deltas) > window else deltas
    m = mean(data)
    s = pstdev(data)

    return m, s if s > 0 else 1.0
