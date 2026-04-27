"""
Execution Plan and Performance models for signal processing.
Extended data structures for TTD, risk management, and performance tracking.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Tuple
from datetime import datetime


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"


@dataclass
class SwingPoint:
    """
    Локальный экстремум + микроструктура.
    Используется для поиска уровня для стопа.
    """
    ts: datetime
    price: float
    type: str  # "high" или "low"
    volume: float = 0.0
    delta: float = 0.0  # агрессивная дельта в районе экстремума


@dataclass
class HTFLevel:
    """
    Старший уровень: дневные/часовые high/low, VWAP-бэнды, etc.
    """
    ts: datetime
    price: float
    kind: str  # "D_high", "D_low", "H1_high", "VWAP", "VWAP_band", ...
    strength: float = 1.0  # субъективная "важность" уровня 0..1


@dataclass
class OrderBookSnapshot:
    """
    Упрощённый L2, на момент сигнала/входа.
    В реале можно хранить ссылку на отдельный сервис L2.
    """
    ts: datetime
    best_bid: float
    best_ask: float
    bids: List[float] = field(default_factory=list)  # цены bid, отсортированы по убыванию/возрастанию — как заведено у вас
    asks: List[float] = field(default_factory=list)  # цены ask


@dataclass
class AccountState:
    """
    Состояние счёта при генерации плана: нужно для risk sizing.
    """
    equity_usd: float
    open_risk_usd: float  # суммарный риск по открытым позициям
    max_risk_per_trade_pct: float  # например 0.5 (% от equity)
    max_portfolio_risk_pct: float  # например 5.0 (% от equity)


@dataclass
class ExtendedSignalContext:
    """
    Расширенный контекст сигнала, который заходит в ExecutionPlanner.
    Часть полей у вас уже наверняка есть — просто примерьте по именам.
    """
    signal_id: str
    symbol: str
    side: Side
    setup_type: str  # "vol_spike", "breakout", "mean_reversion" и т.п.

    ts_signal: datetime
    price_at_signal: float  # mid/last, как у вас заведено

    # ATR на разных ТФ
    atr_1m: float
    atr_5m: float

    # Финальный скор модели (0..1 или нормированный)
    final_score: float

    # Микроструктурные данные
    l2_snapshot: Optional[OrderBookSnapshot] = None
    local_swings: List[SwingPoint] = field(default_factory=list)
    htf_levels: List[HTFLevel] = field(default_factory=list)

    # Параметры инструмента
    tick_size: float = 0.01
    contract_size: float = 1.0  # для фьючей/CFD

    # Состояние счёта
    account_state: Optional[AccountState] = None

    # Конфиг по TTD, если уже посчитан (можно подтягивать из БД)
    ttd_expiry_bars: Optional[int] = None


@dataclass
class ExecutionPlan:
    """
    План исполнения сигнала с уровнями входа, выхода и риска.
    """
    signal_id: str
    symbol: str
    side: Side

    # Зона входа
    entry_zone_low: float
    entry_zone_high: float

    # Планируемый стоп
    stop_price: float

    # Риск и размер (обязательные поля без дефолтов)
    pos_risk_R: float  # риск в R (сколько "R" закладываем)
    risk_usd: float
    position_size: float  # лоты/контракты

    # Время жизни сигнала
    expiry_bars: int  # через сколько 1m-свеч сигнал протухает

    # Планируемые тейки (0..N) - с дефолтами
    tp_levels: List[float] = field(default_factory=list)
    partials: List[float] = field(default_factory=list)  # доли позиции на каждом TP

    # Служебное
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Bar1m:
    """
    1-минутный бар для расчета TTD и MFE/MAE.
    """
    ts: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class SignalPerformance:
    """
    Результаты выполнения сигнала: TTD, MFE/MAE, realized R и т.д.
    """
    signal_id: str
    symbol: str
    side: Side
    setup_type: str

    ts_signal: datetime
    ts_entry: Optional[datetime]
    ts_exit: Optional[datetime]

    price_at_signal: float
    entry_price: Optional[float]
    exit_price: Optional[float]
    stop_price: Optional[float]

    # Результат в R
    realized_R: Optional[float]  # итоговый результат сделки в R
    mfe_R: Optional[float]       # max favorable excursion в R
    mae_R: Optional[float]       # max adverse excursion в R

    # Time-to-decay в барах и секундах
    ttd_bars: Optional[int]
    ttd_seconds: Optional[float]

    # Статус: "realized", "stopped", "expired", "no_entry"
    outcome: str

    # Доп. инфа для анализа
    bars_to_entry: Optional[int] = None
    bars_to_exit: Optional[int] = None
    notes: Optional[str] = None


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
    default_tp_R: Tuple[float, float, float] = (1.0, 2.0, 3.0)

    # Risk sizing по score (ступени)
    score_buckets: Tuple[float, float, float] = (0.4, 0.7, 0.85)  # границы
    risk_multipliers: Tuple[float, float, float, float] = (0.5, 1.0, 1.5, 2.0)

    # Глобальные лимиты
    max_risk_R_per_trade: float = 1.0
    max_portfolio_risk_pct: float = 5.0
