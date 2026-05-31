"""
Общие типы для модуля сигналов.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


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
    atr_ts_ms: int | None = None  # Timestamp when ATR was last updated
    pivots: dict[str, float] | None = None
    current_delta: float = 0.0
    delta_bucket: float = 0.0

    # Расширенные поля (опционально)
    last_price: float = 0.0
    vwap: float | None = None
    daily_open: float | None = None
    daily_open_dist_bps: float | None = None
    cum_delta_slope: float | None = None
    atr_q_14: float | None = None

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
    ts_utc: float | None = None
    daily_atr_bps: float | None = None
    weak_progress_ratio: float | None = None
    htf_level_dist_bps: float | None = None

    # Поля для экспериментов и фильтров
    experiment_id: str | None = None
    experiment_variant: str | None = None
    experiment_config: dict[str, Any] | None = None
    filter_flags: dict[str, bool] | None = None

    # Confidence и breakdown (для обратной совместимости)
    confidence: float | None = None
    confidence_breakdown: dict[str, Any] | None = None

    # Режим рынка
    regime: str = ""
    regime_trend_score: float = 0.0
    regime_range_score: float = 0.0

    # Новые поля скоринга / quality:
    score_raw: float = 0.0          # базовый score до golden / pattern_weight
    score_final: float = 0.0        # итоговый score после всех множителей
    quality_label: SignalQualityLabel | None = None
    quality_reasons: list[str] = field(default_factory=list)

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
    tags: list[str] = None
    is_golden_pattern: bool = False
    golden_pattern_label: str | None = None

    # Качество сигнала (опционально)
    quality_offline: float | None = None
    quality_online: float | None = None
    quality_combined: float | None = None
    quality_status: str | None = None
    is_disabled_by_quality: bool = False

    # Новые поля скоринга / quality (дублирование для удобства)
    score_raw: float = 0.0          # базовый score до golden / pattern_weight
    score_final: float = 0.0        # итоговый score после всех множителей
    quality_label: SignalQualityLabel | None = None
    quality_reasons: list[str] = field(default_factory=list)

    # pattern_weight: вес паттерна (ICT, SMT и т.п.)
    pattern_weight: float = 1.0

    # ============================================================
    # Торговые уровни (нужны для cost gate / expected_move_bps)
    # ============================================================
    # Эти поля НЕ обязательны для всего пайплайна, но крайне важны
    # для анти-churn gate: EDGE_EXPECTED_MOVE_MODE=tp1/rr/atr.
    # Заполняются там, где вы рассчитываете SL/TP (для crypto — добавим).
    entry_price: float | None = None
    sl_price: float | None = None
    tp1_price: float | None = None
    tp_levels: list[float] | None = None
    stop_dist: float | None = None
    rr_list: list[float] | None = None
    stop_mode: str = ""
    tp_mode: str = ""

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
