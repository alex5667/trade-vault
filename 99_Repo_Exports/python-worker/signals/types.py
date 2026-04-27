"""
Общие типы для модуля сигналов.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum


class SignalQualityLabel(Enum):
    A = "A"
    B = "B"
    C = "C"
    REJECT = "REJECT"  # технический label для «заваленных» сигналов


@dataclass
class OrderflowContext:
    """
    Raw orderflow context со всеми метриками, собранными из тиков/баров.
    Это входные данные для пайплайна.
    """
    # Базовые поля
    ts: int
    price: float
    symbol: str
    family: str = "orderflow"
    venue: str = ""
    timeframe: str = ""

    # Основные метрики
    z_delta: float = 0.0
    weak_progress: bool = False
    obi: float = 0.0
    obi_avg: float = 0.0
    obi_sustained: bool = False
    atr: float = 0.0
    atr_ts_ms: Optional[int] = None  # Timestamp when ATR was last updated
    pivots: Optional[Dict[str, float]] = None
    current_delta: float = 0.0
    delta_bucket: float = 0.0

    # Расширенные поля (опционально)
    last_price: float = 0.0
    vwap: Optional[float] = None
    daily_open: Optional[float] = None
    daily_open_dist_bps: Optional[float] = None
    cum_delta_slope: Optional[float] = None
    atr_q_14: Optional[float] = None

    # L2/L3 метрики
    spread_bps: float = 0.0
    obi_20: float = 0.0
    depth_bid_5: float = 0.0
    depth_ask_5: float = 0.0
    microprice_shift_bps_20: float = 0.0

    # Burst метрики
    burst_ratio: float = 0.0
    burst_cv_dt: float = 0.0
    burst_fano_counts: float = 0.0
    burst_flip_ratio: float = 0.0

    # Дополнительные поля для совместимости
    ts_utc: Optional[float] = None
    daily_atr_bps: Optional[float] = None
    weak_progress_ratio: Optional[float] = None
    htf_level_dist_bps: Optional[float] = None

    # Поля для экспериментов и фильтров
    experiment_id: Optional[str] = None
    experiment_variant: Optional[str] = None
    experiment_config: Optional[Dict[str, Any]] = None
    filter_flags: Optional[Dict[str, bool]] = None

    # Confidence и breakdown (для обратной совместимости)
    confidence: Optional[float] = None
    confidence_breakdown: Optional[Dict[str, Any]] = None

    # Режим рынка
    regime: str = ""
    regime_trend_score: float = 0.0
    regime_range_score: float = 0.0

    # Новые поля скоринга / quality:
    score_raw: float = 0.0          # базовый score до golden / pattern_weight
    score_final: float = 0.0        # итоговый score после всех множителей
    quality_label: Optional[SignalQualityLabel] = None
    quality_reasons: List[str] = field(default_factory=list)

    # Уже существующие поля (если нет — добавить):
    pattern_weight: float = 1.0      # вес паттерна (ICT, SMT и т.п.)
    is_golden_pattern: bool = False  # флаг «золотого» сигнала (GOLDEN_MULT)


@dataclass
class SignalContext:
    """
    Контекст для скоринга и принятия решений.
    Создается из OrderflowContext и обогащается на каждом шаге пайплайна.
    """
    # Идентификация
    symbol: str
    ts_event_ms: int

    # Ссылка на сырой контекст
    of: OrderflowContext

    # Рынок и сессия
    regime: Optional["RegimeInfo"] = None
    session: str = ""

    # Скоринг
    base_score: float = 0.0
    final_score: float = 0.0

    # Golden pattern
    tags: List[str] = None
    is_golden_pattern: bool = False
    golden_pattern_label: Optional[str] = None

    # Качество сигнала (опционально)
    quality_offline: Optional[float] = None
    quality_online: Optional[float] = None
    quality_combined: Optional[float] = None
    quality_status: Optional[str] = None
    is_disabled_by_quality: bool = False

    # Новые поля скоринга / quality (дублирование для удобства)
    score_raw: float = 0.0          # базовый score до golden / pattern_weight
    score_final: float = 0.0        # итоговый score после всех множителей
    quality_label: Optional[SignalQualityLabel] = None
    quality_reasons: List[str] = field(default_factory=list)

    # pattern_weight: вес паттерна (ICT, SMT и т.п.)
    pattern_weight: float = 1.0

    # ============================================================
    # Торговые уровни (нужны для cost gate / expected_move_bps)
    # ============================================================
    # Эти поля НЕ обязательны для всего пайплайна, но крайне важны
    # для анти-churn gate: EDGE_EXPECTED_MOVE_MODE=tp1/rr/atr.
    # Заполняются там, где вы рассчитываете SL/TP (для crypto — добавим).
    entry_price: Optional[float] = None
    sl_price: Optional[float] = None
    tp1_price: Optional[float] = None
    tp_levels: Optional[List[float]] = None
    stop_dist: Optional[float] = None
    rr_list: Optional[List[float]] = None
    stop_mode: str = ""
    tp_mode: str = ""

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
