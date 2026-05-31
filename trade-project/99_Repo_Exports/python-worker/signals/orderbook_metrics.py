"""
Order Book Metrics - метрики для детекции iceberg orders из DOM данных.

ФУНКЦИОНАЛ:
- Отслеживание best bid/ask уровней
- Детекция "залипания" цены (price hold duration)
- Подсчет refresh-ей объема на том же уровне
- Iceberg condition: duration + refresh count

ИСПОЛЬЗОВАНИЕ:
    from signals.orderbook_metrics import BestLevelTracker
    
    tracker = BestLevelTracker(
        min_duration_ms=1500,
        refresh_min_abs=1.0,
        refresh_count_target=2
    )
    
    # При каждом DOM update
    tracker.feed_book(book_data, timestamp_ms)
    
    # Проверка iceberg
    if tracker.is_iceberg("bid", timestamp_ms):
        print("Iceberg detected at bid level!")

ИНТЕГРАЦИЯ:
- Используется в XAU OrderFlow Handler v3
- Работает с данными из stream:book_
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LevelState:
    """
    Состояние одного уровня (best bid или best ask).
    """
    price: float | None = None          # Текущая цена уровня
    since_ms: int | None = None         # С какого времени на этой цене (мс)
    last_vol: float | None = None       # Последний объем
    saw_decrease: bool = False             # Видели уменьшение объема
    refresh: int = 0                       # Количество refresh-ей


@dataclass
class BestLevelTracker:
    """
    Трекер best bid/ask уровней для детекции iceberg orders.
    
    Iceberg order детектируется когда:
    1. Цена "держится" на уровне >= min_duration_ms
    2. Объем уменьшается и потом увеличивается (refresh)
    3. Количество refresh-ей >= refresh_count_target
    
    Это индикатор скрытого крупного ордера, который "докладывает"
    объем постепенно, чтобы не показывать полный размер.
    """

    # Параметры детекции
    min_duration_ms: int = 1500              # Минимальная длительность "залипания" (мс)
    refresh_min_abs: float = 1.0             # Минимальное увеличение объема для refresh
    refresh_count_target: int = 2            # Целевое количество refresh-ей

    # Состояние уровней
    bid: LevelState = field(default_factory=LevelState)
    ask: LevelState = field(default_factory=LevelState)

    def _update_side(self, side: LevelState, price: float, vol: float, ts: int) -> None:
        """
        Обновляет состояние одного уровня (bid или ask).
        
        Args:
            side: Состояние уровня для обновления
            price: Текущая цена уровня
            vol: Текущий объем на уровне
            ts: Timestamp в миллисекундах
        """
        # Если цена изменилась - сброс
        if side.price is None or abs(price - side.price) > 1e-9:
            side.price = price
            side.since_ms = ts
            side.last_vol = vol
            side.saw_decrease = False
            side.refresh = 0
            return

        # Цена та же - отслеживаем изменения объема
        if side.last_vol is None:
            side.last_vol = vol
            return

        # Уменьшение объема (часть ордера исполнена)
        if vol < side.last_vol - 1e-9:
            side.saw_decrease = True

        # Увеличение объема после уменьшения (refresh!)
        elif side.saw_decrease and vol > side.last_vol + self.refresh_min_abs:
            side.refresh += 1
            side.saw_decrease = False

        # Обновляем last_vol
        side.last_vol = vol

    def feed_book(self, book: dict[str, Any], ts: int) -> None:
        """
        Обрабатывает Order Book snapshot.
        
        Args:
            book: Словарь с ключами "bids" и "asks"
                  bids: [[price, volume], ...]
                  asks: [[price, volume], ...]
            ts: Timestamp в миллисекундах
        """
        if not book:
            return

        # Сортируем и берем best levels
        bids = sorted(book.get("bids", []), key=lambda x: x[0], reverse=True)
        asks = sorted(book.get("asks", []), key=lambda x: x[0])

        # Обновляем best bid
        if bids:
            price_bid, vol_bid = float(bids[0][0]), float(bids[0][1])
            self._update_side(self.bid, price_bid, vol_bid, ts)

        # Обновляем best ask
        if asks:
            price_ask, vol_ask = float(asks[0][0]), float(asks[0][1])
            self._update_side(self.ask, price_ask, vol_ask, ts)

    def metrics(self, ts: int) -> dict[str, dict[str, float]]:
        """
        Возвращает текущие метрики для обоих уровней.
        
        Args:
            ts: Текущий timestamp в миллисекундах
            
        Returns:
            Словарь с метриками bid и ask уровней
        """
        def calc_duration(state: LevelState) -> float:
            if state.since_ms is None:
                return 0.0
            return max(0, ts - state.since_ms) / 1000.0  # в секундах

        return {
            "bid": {
                "duration": calc_duration(self.bid),
                "refresh": self.bid.refresh,
                "price": self.bid.price or 0.0,
                "volume": self.bid.last_vol or 0.0
            },
            "ask": {
                "duration": calc_duration(self.ask),
                "refresh": self.ask.refresh,
                "price": self.ask.price or 0.0,
                "volume": self.ask.last_vol or 0.0
            }
        }

    def is_iceberg(self, side: str, ts: int) -> bool:
        """
        Проверяет условие iceberg для указанного уровня.
        
        Args:
            side: "bid" или "ask"
            ts: Текущий timestamp в миллисекундах
            
        Returns:
            True если детектирован iceberg order
        """
        state = self.bid if side.lower() == "bid" else self.ask

        if state.since_ms is None:
            return False

        # Проверяем длительность "залипания"
        duration_ms = ts - state.since_ms

        # Iceberg = долго держится + достаточно refresh-ей
        if duration_ms >= self.min_duration_ms and state.refresh >= self.refresh_count_target:
            return True

        return False

    def reset(self) -> None:
        """Сбрасывает все состояние трекера."""
        self.bid = LevelState()
        self.ask = LevelState()

