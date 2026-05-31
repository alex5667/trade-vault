from __future__ import annotations

"""
Signal Execution Domain Models for Production Use

Core data structures for signal execution planning and performance tracking
in the scanner_infra system. These models integrate with existing SignalContext.
"""


from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


class Side(StrEnum):
    LONG = "long"
    SHORT = "short"


# Минимальный "профиль" счёта, который нужен планировщику
@dataclass
class AccountState:
    equity_usd: float
    open_risk_usd: float
    max_risk_per_trade_pct: float   # % от equity на одну сделку (0.5 = 0.5%)
    max_portfolio_risk_pct: float   # общий лимит риска (5.0 = 5%)


# Локальный экстремум для микроструктурного стопа
@dataclass
class SwingPoint:
    ts: datetime
    price: float
    type: str              # "high" или "low"
    volume: float = 0.0    # опционально
    delta: float = 0.0     # опционально


# HTF-уровень (D/H4/H1/VWAP-зоны и т.п.)
@dataclass
class HTFLevel:
    ts: datetime
    price: float
    kind: str              # "D_high", "D_low", "H1_vwap" и т.п.
    strength: float = 1.0  # 0..1


# Снимок книги — запас на будущее (если захотите учитывать книгу/спред)
@dataclass
class OrderBookSnapshot:
    ts: datetime
    best_bid: float
    best_ask: float
    bids: list[float] = field(default_factory=list)
    asks: list[float] = field(default_factory=list)


# 1m-бар для performance-трекера
@dataclass
class Bar1m:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


# ExecutionPlan — это то, что идёт дальше в MT5 / execution-движок
@dataclass
class ExecutionPlan:
    signal_id: str
    symbol: str
    side: Side
    setup_type: str

    ts_signal: datetime
    price_at_signal: float

    entry_zone_low: float
    entry_zone_high: float

    stop_price: float
    tp_levels: list[float]           # реальные ценовые уровни TP
    partials: list[float]            # доли объёма для частичных выходов (сумма ≤ 1.0)

    pos_risk_R: float                # риск в R на сделку (до стопа)
    risk_usd: float                  # риск в USD
    position_size: float             # лоты/контракты

    expiry_bars: int                 # сколько 1m-баров сигнал живёт

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class SymbolSetupConfig:
    """
    Параметры по инструменту + сетапу.
    Эти значения можно подтягивать из БД (signal_ttd_config и др.).
    """
    symbol: str
    setup_type: str

    # Время жизни (в барах 1m)
    expiry_bars: int = 3  # дефолт, если из БД не пришло

    # Буфер для стопа
    min_stop_ticks: int = 5
    max_stop_R: float = 3.0  # не больше 3R, иначе сетап считаем неадекватным
    atr_buffer_ratio: float = 0.1  # 0.1 * ATR_1m

    # Entry zone в R от стопа (для long от support_level)
    entry_zone_min_R: float = 0.3
    entry_zone_max_R: float = 0.8

    # TP уровни в R (если нет явных HTF уровней)
    default_tp_R: tuple[float, float, float] = (1.0, 2.0, 3.0)

    # Risk sizing по score (ступени)
    score_buckets: tuple[float, float, float] = (0.4, 0.7, 0.85)  # границы
    risk_multipliers: tuple[float, float, float, float] = (0.5, 1.0, 1.5, 2.0)

    # Глобальные лимиты
    max_risk_R_per_trade: float = 1.0
    max_portfolio_risk_pct: float = 5.0
