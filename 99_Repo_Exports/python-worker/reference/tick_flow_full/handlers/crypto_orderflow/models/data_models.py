from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal
from collections import deque


@dataclass
class RegimeConfig:
    # размер окна истории режима (в сэмплах/бакетах), чтобы deque(maxlen=...) был стабильным
    regime_window_size: int = 240

    # веса фич в финальном score
    atr_weight: float = 1.0
    delta_weight: float = 1.0
    vwap_dev_weight: float = 1.0
    daily_open_dev_weight: float = 0.75
    daily_open_cross_weight: float = 0.75
    htf_level_weight: float = 0.5
    weak_progress_weight: float = 0.75
    session_weight: float = 0.25

    # пороги для финального score
    regime_trend_threshold: float = 0.35
    regime_range_threshold: float = -0.35

    # --- динамические пороги по daily_open, завязанные на daily ATR ---
    # daily_open_range ≈ «мы ещё в окрестности open → больше похоже на рендж»
    # daily_open_trend ≈ «мы уже далеко от open → больше похоже на тренд»
    daily_open_range_atr_mult: float = 0.25     # 0.25 * daily_atr_bps
    daily_open_trend_atr_mult: float = 0.75     # 0.75 * daily_atr_bps

    # запасные абсолютные пороги, если daily_atr_bps нет
    daily_open_range_bps_fallback: float = 7.0
    daily_open_trend_bps_fallback: float = 25.0

    # окно по барам для частоты пересечений daily_open
    daily_open_cross_window: int = 60           # N последних баров

    # weakProgress (|range|/ATR) как фильтр грязного тренда
    weak_progress_range_threshold: float = 0.3  # ниже → рендж-поведение
    weak_progress_trend_threshold: float = 0.6  # выше → более «чистый» тренд

    # HTF proximity: насколько близко к сильному уровню считаем «range bias»
    htf_near_mult: float = 0.2   # если dist_bps <= 0.2 * daily_atr_bps → сильно около уровня
    htf_far_mult: float = 0.8    # если dist_bps >= 0.8 * daily_atr_bps → далеко от уровня

    htf_near_bps_fallback: float = 10.0
    htf_far_bps_fallback: float = 40.0

    # session regime: мапа session_label -> bias (по умолчанию)
    session_bias_default: Dict[str, float] | None = None

    def __post_init__(self) -> None:
        if self.session_bias_default is None:
            # базовая евродоллар/золото логика:
            # Азия — больше рендж, Лондон/NY — более трендовые
            self.session_bias_default = {
                "asia": -0.4
                "london": +0.4
                "ny": +0.6
                "late_us": 0.0
                "other": 0.0
            }


@dataclass
class RegimeSample:
    ts: float
    price: float
    vwap_side: int        # -1 / 0 / +1 (ниже / на / выше VWAP)
    daily_open_side: int  # -1 / 0 / + 1 (ниже / на / выше open)
    bar_index: int | None = None


@dataclass
class RegimeFeatures:
    # raw метрики (по желанию для логов)
    vwap_dev_bps: float | None = None
    daily_open_dev_bps: float | None = None
    daily_open_cross_freq: float | None = None
    htf_level_dist_bps: float | None = None

    # bias в диапазоне [-1; +1]
    atr_bias: float | None = None
    delta_dir_bias: float | None = None
    vwap_dev_bias: float | None = None
    daily_open_dev_bias: float | None = None
    daily_open_cross_bias: float | None = None
    htf_prox_bias: float | None = None
    weak_progress_bias: float | None = None
    session_bias: float | None = None


@dataclass
class HTFLevels:
    pdh: float
    pdl: float
    pdm: float
    week_hi: float
    week_lo: float
    asia_open: float
    europe_open: float
    us_open: float
    ob_zones: list[dict]  # [{"low":..., "high":..., "strength":...}, ...]
    fvg_zones: list[dict]  # [{"low":..., "high":..., "strength":...}, ...]


@dataclass
class GeometryConfig:
    # зона интереса в долях ATR_HTF
    near_mult: float = 0.25    # 0.25 * ATR_HTF
    far_mult: float = 1.0      # дальше ATR_HTF — уже "не уровень"

    new_extreme_bonus: float = 0.3   # сколько добавить, если реально новый экстремум
    max_score: float = 1.0
    min_score: float = 0.0


@dataclass
class LiquidityConfig:
    min_notional_for_high_liq: float = 250_000.0   # порог "толстого" best bid/ask
    dense_cluster_bps: float = 5.0                 # окно вокруг цены в б.п. для поиска плотного кластера
    dense_cluster_min_levels: int = 3
    dense_cluster_min_share: float = 0.25          # какая доля объёма в кластере считается "плотной"


@dataclass
class ConfScoreConfig:
    regime_weight: float = 0.4
    geometry_weight: float = 0.3
    liquidity_weight: float = 0.3

    # жёсткие фильтры
    min_geometry_for_signal: float = 0.25
    min_liquidity_for_signal: float = 0.2


@dataclass
class SignalTypeConf:
    name: SignalKind

    # веса компонентов в conf_factor
    regime_weight: float
    geometry_weight: float
    liquidity_weight: float

    # минимальные пороги
    min_conf_factor: float          # минимальный conf_factor (после смешивания)
    min_final_score: float          # минимальный итоговый |score|
    min_raw_score: float            # минимальный |raw_score| (например, z-score сигнала)

    # режимы рынка
    allowed_regimes: Tuple[MarketRegime, ...]
    prefer_trend: bool = False
    prefer_range: bool = False
    forbid_strong_trend: bool = False
    forbid_strong_range: bool = False

    # "золотой стандарт" (пороговые значения)
    golden_regime_min: float = 0.7
    golden_geometry_min: float = 0.7
    golden_liquidity_min: float = 0.7


@dataclass
class GoldenThresholds:
    regime_min: float
    geometry_min: float
    liquidity_min: float


@dataclass
class _PendingMid:
    ts: int
    mid_at_trade: float
    side: int  # +1 taker-buy, -1 taker-sell
