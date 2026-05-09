from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass
class L3LiteEvent:
    """Событие L3-Lite потока"""
    ts_ms: int             # время события
    kind: str              # 'trade', 'cancel', 'new', 'replace', ...
    side: str              # 'bid' / 'ask'
    price: float
    qty: float


@dataclass
class BookSnapshot:
    """Снимок книги ордеров"""
    ts_ms: int
    bids: list[tuple[float, float]]   # [(price, qty), ...] отсортированы по убыванию цены
    asks: list[tuple[float, float]]   # [(price, qty), ...] по возрастанию цены


@dataclass
class L3LiteFeatures:
    """Рассчитанные L3-Lite метрики"""
    cancel_to_trade_bid_5s: float
    cancel_to_trade_ask_5s: float
    cancel_to_trade_bid_20s: float
    cancel_to_trade_ask_20s: float

    microprice: float
    microprice_shift_bps_20: float

    spread_bps: float
    obi_5: float
    obi_20: float
    obi_50: float
    obi_persistence_score: float

    # Дополнительные метрики
    microprice_velocity_bps: float = 0.0  # скорость изменения микропрайса
    queue_pressure_bid: float = 0.0       # давление на bid (cancel/trade + obi)
    queue_pressure_ask: float = 0.0       # давление на ask (cancel/trade + obi)
    market_depth_imbalance: float = 0.0   # несбалансированность глубины книги


@dataclass
class CancelTradeBuffers:
    """Буферы для накопления объемов cancel/trade"""
    # (ts_ms, volume)
    cancels_bid: deque[tuple[int, float]] = field(default_factory=deque)
    cancels_ask: deque[tuple[int, float]] = field(default_factory=deque)
    trades_bid: deque[tuple[int, float]] = field(default_factory=deque)
    trades_ask: deque[tuple[int, float]] = field(default_factory=deque)


@dataclass
class MicropriceHistoryPoint:
    """Точка истории микропрайса"""
    ts_ms: int
    microprice: float
