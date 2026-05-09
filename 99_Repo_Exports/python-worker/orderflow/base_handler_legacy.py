from __future__ import annotations

import json
import math
import os
import statistics
import time

from core.redis_keys import RedisStreams as RS

# orderflow/base_handler_legacy.py
# DEPRECATED: This is the legacy monolithic version of BaseOrderFlowHandler.
# New code should use handlers.base_orderflow_handler.BaseOrderFlowHandler instead.
from utils.time_utils import get_ny_time_millis
import contextlib


# ======================================================================================
# Pure helper for deterministic tests:
# - Parses hmget("atr","lastCloseTime")
# - Applies staleness filter identical to legacy logic
# - Returns (atr_value, atr_ts_ms)
# ======================================================================================
def load_tracker_atr_from_redis_hmget(
    *,
    redis_client: any,
    key: str,
    timeframe: str,
    current_ts: int,
    timeframe_to_ms_fn: any,
    logger: any,
    warning_logged: bool,
) -> tuple[float | None, int | None, bool]:
    """
    Fail-open + testable parser for Redis ATR tracker.
    Returns:
      (atr_value, atr_ts_ms, warning_logged_updated)
    """
    try:
        atr_str, last_close_str = redis_client.hmget(key, "atr", "lastCloseTime")
    except Exception as exc:
        if not warning_logged:
            with contextlib.suppress(Exception):
                logger.warning("ATR read failed %s: %s", key, exc)
        return None, None, True

    if not atr_str:
        return None, None, warning_logged

    try:
        val = float(atr_str)
    except Exception:
        return None, None, warning_logged
    if val <= 0.0 or not math.isfinite(val):
        return None, None, warning_logged

    atr_ts_ms: int | None = None
    if last_close_str:
        try:
            atr_ts_ms = int(last_close_str)
        except Exception:
            atr_ts_ms = None

    # staleness filter (same behavior as your current _load_tracker_atr_from_redis)
    if atr_ts_ms and current_ts:
        try:
            stale_mult = int(os.getenv("ATR_REDIS_STALENESS_MULT", "3"))
        except Exception:
            stale_mult = 3
        max_age = int(timeframe_to_ms_fn(timeframe)) * max(int(stale_mult), 1)
        if current_ts - int(atr_ts_ms) > max_age:
            return None, None, warning_logged

    return val, atr_ts_ms, warning_logged

# ======================================================================================
# Pure helper for deterministic tests:
# - Parses hmget("atr","lastCloseTime")
# - Applies staleness filter identical to legacy logic
# - Returns (atr_value, atr_ts_ms)
# ======================================================================================
def load_tracker_atr_from_redis_hmget(
    *,
    redis_client: any,
    key: str,
    timeframe: str,
    current_ts: int,
    timeframe_to_ms_fn: any,
    logger: any,
    warning_logged: bool,
) -> tuple[float | None, int | None, bool]:
    """
    Fail-open + testable parser for Redis ATR tracker.
    Returns:
      (atr_value, atr_ts_ms, warning_logged_updated)
    """
    try:
        atr_str, last_close_str = redis_client.hmget(key, "atr", "lastCloseTime")
    except Exception as exc:
        if not warning_logged:
            with contextlib.suppress(Exception):
                logger.warning("ATR read failed %s: %s", key, exc)
        return None, None, True

    if not atr_str:
        return None, None, warning_logged

    try:
        val = float(atr_str)
    except Exception:
        return None, None, warning_logged
    if val <= 0.0 or not math.isfinite(val):
        return None, None, warning_logged

    atr_ts_ms: int | None = None
    if last_close_str:
        try:
            atr_ts_ms = int(last_close_str)
        except Exception:
            atr_ts_ms = None

    # staleness filter (same behavior as your current _load_tracker_atr_from_redis)
    if atr_ts_ms and current_ts:
        try:
            stale_mult = int(os.getenv("ATR_REDIS_STALENESS_MULT", "3"))
        except Exception:
            stale_mult = 3
        max_age = int(timeframe_to_ms_fn(timeframe)) * max(int(stale_mult), 1)
        if current_ts - int(atr_ts_ms) > max_age:
            return None, None, warning_logged

    return val, atr_ts_ms, warning_logged

# Детерминированный парсер/классификатор для 6.1 (юнит-тестируемый)
from handlers.tick_parser import Tick as ParsedTick
from handlers.tick_parser import classify_delta as _classify_delta_pure
from handlers.tick_parser import parse_tick as _parse_tick_pure

# Record & Replay (6.2)
try:
    from replay.recorder import ReplayRecorder
except Exception:
    ReplayRecorder = None  # type: ignore
import threading

from common.deque_utils import ensure_bounded_deque
from common.steady_clock import SteadyClock
from common.tick_time import SanitizeResult, TickTimeGuard, TickTimePolicy
from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy

# ---- Minimal metrics instrumentation (fail-open) ----
# Важно: этот импорт НЕ должен ломать рантайм.
try:
    from common.metrics2 import (
        EventRateTracker,
        LagTracker,
        MissingRateTracker,
        NoopMetrics,
        normalize_ts_ms,
        safe_float,
        should_drop_by_watermark,
    )
    from common.signal_metrics import SignalMetrics
except Exception:  # pragma: no cover
    NoopMetrics = None  # type: ignore
    LagTracker = None  # type: ignore
    MissingRateTracker = None  # type: ignore
    EventRateTracker = None  # type: ignore
    safe_float = None  # type: ignore
    normalize_ts_ms = None  # type: ignore
    should_drop_by_watermark = None  # type: ignore
    SignalMetrics = None
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC
from typing import Any, Literal

from calibration.local_calibration_service import LocalCalibrationService
from common.backoff import Backoff, sleep_s
from common.dlq_sanitize import sanitize_for_dlq
from common.gpu_service import get_gpu_service
from common.log import setup_logger
from common.redis_errors import is_transient_error as is_transient_redis_error
from common.robust_stats import RobustZscoreMADRolling
from common.time_norm import normalize_epoch_ms
from config.gpu_config import GPU_MIN_N
from core.dependency_policy import ensure_dependency_defaults
from core.dual_redis_client import get_dual_signals_redis
from core.htf_levels import HTFLevels, HTFLevelsProvider  # type: ignore
from core.instrument_config import OrderFlowConfig, SymbolSpecs, get_config
from core.performance_optimizer import (
    ATRCache,
    PivotPointsCache,
    get_optimized_redis_client,
)
from core.redis_stream_consumer import SyncRedisStreamHelper
from core.signal_context import SignalContext
from core.signal_outbox import OutboxSettings, SignalOutboxPublisher
from core.unified_signal_formatter import Signal
from geometry.htf_levels import HTFLevelsService
from handlers.signal_types import MarketRegime
from local_calibration.store import LocalCalibrationStore as LCStoreV2
from local_calibration.store import eval_local_quantile
from regime import BlackZoneScheduler, RegimeRuntimeState, RegimeSample

# Lazy import to avoid circular dependency: from signal_scoring import SignalScoringEngine, ScoringConfig, SignalContext as ScoringSignalContext
# Import new modular services
from regime.market_regime_service import MarketRegimeService
from services.burstiness_tracker import BurstinessTracker, BurstStats
from services.l3_lite_tracker import L3LiteTracker
from services.l3_queue_events_proxy import L3BucketStats, L3QueueEventsProxy
from services.pnl_math import calculate_position_size
from services.queue_eta_estimator import QueueETAEvaluator
from services.touch_level_tracker import TouchLevelTracker, TouchSnapshot
from signals.atr import ATR
from signals.detectors import weak_progress as check_weak_progress
from signals.orderbook_l2_tracker import L2BookTracker, L2Snapshot
from signals.pivots import compute_daily_pivots
from signals.risk_levels import compute_levels

# Import unified pipeline (now local to orderflow package)
from .unified_pipeline import OrderflowContext, SignalContext, UnifiedSignalPipeline
import contextlib

ZoneType = Literal[
    "PDH", "PDL", "PDM",         # previous day high/low/mid
    "WEEK_HI", "WEEK_LO",
    "ASIA_OPEN", "EUROPE_OPEN", "US_OPEN",
    "HTF_OB", "HTF_FVG"
]


@dataclass
class LiquidityContext:
    # L2 / кластер вокруг цены
    near_wall_side: Literal["bid", "ask"] | None = None
    near_wall_price: float | None = None
    near_wall_size: float | None = None        # абсолютный размер
    near_wall_size_z: float | None = None      # z-score к "нормальной" глубине

    depth_5_vol: float | None = None           # суммарный объём в 5-ти уровнях
    depth_5_z: float | None = None

    # связь агрессии с ликвидностью
    aggr_vol_at_wall: float | None = None      # объём ударов по стене
    aggr_to_rest_ratio: float | None = None    # aggr_vol_at_wall / near_wall_size

    # примитивная классификация: разбор / поглощение / ничего
    pattern: Literal["absorption", "break", "none"] | None = None

    # итоговый score [0..1]
    liquidity_context_score: float | None = None


@dataclass
class GeoZoneHit:
    zone_type: ZoneType
    zone_price: float
    dist_bps: float         # расстояние |price - zone_price| в б.п.
    atr_htf_bps: float      # ATR HTF в б.п. на момент
    dist_rel_atr: float     # dist_bps / atr_htf_bps
    strength: float         # "сила" уровня [0..1] (PDH < weekly < OB и т.п.)




@dataclass
class BarSample:
    ts: float
    high: float
    low: float
    volume: float


@dataclass
class L2Level:
    price: float
    size: float


@dataclass
class SimpleL2Snapshot:
    bids: list[L2Level]  # отсортированы по цене убыванию
    asks: list[L2Level]  # по возрастанию


@dataclass
class ClusterVol:
    # суммарные агрессивные объёмы по ценам в окрестности текущего бара
    buy_vol_by_price: dict[float, float]
    sell_vol_by_price: dict[float, float]


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


SignalKind = Literal["breakout", "sweep", "reclaim", "absorption"]


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
    allowed_regimes: tuple[MarketRegime, ...]
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
class RegimeFeatures:
    """Фичи, на основе которых решаем TREND / RANGE / MIXED."""
    atr_intraday_bps: float = 0.0          # ATR(14) 1m/5m в б.п. от цены
    atr_quantile_1d: float = 0.5          # квантили дневной ATR по инструменту (0..1)
    weak_progress: float = 0.0            # |range| / ATR по текущей свече
    vwap_distance_bps: float = 0.0        # дистанция до VWAP в б.п.
    vwap_trend_bps: float = 0.0           # тренд "цена - VWAP" в б.п. за окно
    daily_open_range_bps: float = 0.0     # дистанция до daily open в б.п.
    daily_open_cross_freq: float = 0.0    # частота пересечений daily open за последнее окно


@dataclass
class RegimeState:
    label: str  # "trending", "ranging", "mixed", "unknown"
    trend_score: float  # [-1, +1] raw trend score
    range_score: float  # [-1, +1] raw range score
    session_bias: float = 0.0
    daily_open_cross_freq: float = 0.0
    ts: float = 0.0  # timestamp
    symbol=""  # symbol identifier
    last_update_ts: float = 0.0  # timestamp of last regime update


@dataclass(frozen=True)
class RegimeConfig:
    # базовые окна/пороги
    atr_period: int = 14
    regime_window_size: int = 100  # размер окна для истории режима
    trend_score_trend: float = 0.6
    trend_score_range: float = 0.4
    range_score_range: float = 0.6
    range_score_trend: float = 0.4

    # ATR квантили
    atr_quantile_trend_thr: float = 0.7
    atr_quantile_range_thr: float = 0.3

    # weakProgress = |range| / ATR
    weak_progress_trend_min: float = 0.3
    weak_progress_range_max: float = 0.2

    # daily open: рендж — часто пересекаем, тренд — редко
    daily_open_cross_freq_range_min: float = 0.3
    daily_open_cross_freq_trend_max: float = 0.15

    # дистанция до daily open в б.п.
    daily_open_range_bps_max_for_range: float = 40.0
    daily_open_range_bps_min_for_trend: float = 60.0

    # bias по сессиям
    session_bias_default: dict[str, float] = field(default_factory=lambda: {
        "asia": 0.0,
        "london": 0.1,
        "ny": 0.05,
    })

    # частота пробоя daily open
    daily_open_cross_fast: float = 0.6
    daily_open_cross_slow: float = 0.3
    daily_open_cross_window: int = 20  # размер окна для расчета частоты пересечений

    # Weights for regime scoring
    atr_weight: float = 1.0
    delta_weight: float = 0.8
    vwap_dev_weight: float = 0.6
    daily_open_dev_weight: float = 0.7
    daily_open_cross_weight: float = 0.5
    htf_level_weight: float = 0.4
    weak_progress_weight: float = 0.9
    session_weight: float = 0.3

    # Regime score thresholds
    regime_trend_threshold: float = 0.35
    regime_range_threshold: float = -0.35

    @classmethod
    def from_env(cls) -> RegimeConfig:
        import os
        return cls(
            atr_period=int(os.getenv("REGIME_ATR_PERIOD", "14")),
            regime_window_size=int(os.getenv("REGIME_WINDOW_SIZE", "100")),
            trend_score_trend=float(os.getenv("REGIME_TREND_SCORE_TREND", "0.6")),
            trend_score_range=float(os.getenv("REGIME_TREND_SCORE_RANGE", "0.4")),
            range_score_range=float(os.getenv("REGIME_RANGE_SCORE_RANGE", "0.6")),
            range_score_trend=float(os.getenv("REGIME_RANGE_SCORE_TREND", "0.4")),
            atr_quantile_trend_thr=float(os.getenv("REGIME_ATR_QUANTILE_TREND_THR", "0.7")),
            atr_quantile_range_thr=float(os.getenv("REGIME_ATR_QUANTILE_RANGE_THR", "0.3")),
            weak_progress_trend_min=float(os.getenv("REGIME_WEAK_PROGRESS_TREND_MIN", "0.3")),
            weak_progress_range_max=float(os.getenv("REGIME_WEAK_PROGRESS_RANGE_MAX", "0.2")),
            daily_open_cross_freq_range_min=float(os.getenv("REGIME_DAILY_OPEN_CROSS_FREQ_RANGE_MIN", "0.3")),
            daily_open_cross_freq_trend_max=float(os.getenv("REGIME_DAILY_OPEN_CROSS_FREQ_TREND_MAX", "0.15")),
            daily_open_range_bps_max_for_range=float(os.getenv("REGIME_DAILY_OPEN_RANGE_BPS_MAX_FOR_RANGE", "40.0")),
            daily_open_range_bps_min_for_trend=float(os.getenv("REGIME_DAILY_OPEN_RANGE_BPS_MIN_FOR_TREND", "60.0")),
            daily_open_cross_fast=float(os.getenv("REGIME_DAILY_OPEN_CROSS_FAST", "0.6")),
            daily_open_cross_slow=float(os.getenv("REGIME_DAILY_OPEN_CROSS_SLOW", "0.3")),
            daily_open_cross_window=int(os.getenv("REGIME_DAILY_OPEN_CROSS_WINDOW", "20")),
        )


class MarketRegimeService:
    """
    Отдельный сервис, который по фичам + истории (crossings daily_open) даёт:
      - regime ∈ {TREND, RANGE, MIXED, UNKNOWN}
      - regime_score ∈ [-1, +1]
      - RegimeFeatures (для логов/визуализации)
    """

    def __init__(self, cfg: RegimeConfig | None = None, logger=None) -> None:
        self._cfg = cfg or RegimeConfig.from_env()
        self._log = logger

        # history[symbol] = deque[(ts, close, daily_open)]
        self._history: dict[str, deque[tuple[float, float, float]]] = defaultdict(
            lambda: deque(maxlen=240)  # ~4 часа по 1m, настраивается
        )
        self._last_state: dict[str, RegimeState] = {}

        # история режима по инструментам
        self._regime_history: dict[str, deque[RegimeSample]] = defaultdict(
            lambda: deque(maxlen=self._cfg.regime_window_size)
        )

        # bar_history MUST be bounded (memory & latency stability)
        self._bar_history_maxlen = int(os.getenv("BAR_HISTORY_MAXLEN", "512"))
        self.bar_history = deque(maxlen=max(self._bar_history_maxlen, 1))
        self._regime_history = {}

    def _get_bar_history(self) -> deque:
        d = getattr(self, "bar_history", None)
        d2 = ensure_bounded_deque(d, int(getattr(self, "_bar_history_maxlen", 512) or 512))
        if d2 is not d:
            self.bar_history = d2
        return d2

    # --- daily_open: range & crossings ---

    def _compute_daily_open_metrics(
        self,
        hist: deque[tuple[float, float, float]],
    ) -> tuple[float, float]:
        """
        hist: deque[(ts, close, daily_open)]
        return: (current_range_bps, crossings_freq)
        """
        if not hist:
            return 0.0, 0.0

        # текущая дистанция до daily_open
        _, last_close, last_do = hist[-1]
        dist_rel = abs(last_close - last_do) / max(last_do, 1e-6)
        dist_bps = dist_rel * 10_000.0

        if len(hist) < 2:
            return dist_bps, 0.0

        hist_list = list(hist)
        crossings = 0
        for i in range(len(hist_list) - 1):
            _, c_prev, d_prev = hist_list[i]
            _, c_cur, d_cur = hist_list[i + 1]
            do = d_prev  # считаем, что daily_open фиксирован в течение дня
            above_prev = c_prev >= do
            above_cur = c_cur >= do
            if above_prev != above_cur:
                crossings += 1

        cross_freq = crossings / max(len(hist_list) - 1, 1)
        return dist_bps, cross_freq

    # --- scoring regime TREND / RANGE ---

    def _decide_regime(self, f: RegimeFeatures) -> tuple[MarketRegime, float]:
        cfg = self._cfg

        # 1) TREND score ∈ [0,1]
        trend_parts: list[float] = []

        # ATR в верхних квантилях
        if f.atr_quantile_1d > cfg.atr_quantile_trend_thr:
            trend_parts.append(
                min(
                    1.0,
                    (f.atr_quantile_1d - cfg.atr_quantile_trend_thr)
                    / max(1.0 - cfg.atr_quantile_trend_thr, 1e-6),
                )
            )
        else:
            trend_parts.append(0.0)

        # weakProgress высокий → есть направленность
        if f.weak_progress > cfg.weak_progress_trend_min:
            trend_parts.append(
                min(
                    1.0,
                    (f.weak_progress - cfg.weak_progress_trend_min)
                    / max(1.0 - cfg.weak_progress_trend_min, 1e-6),
                )
            )
        else:
            trend_parts.append(0.0)

        # далеко от daily_open
        if f.daily_open_range_bps > cfg.daily_open_range_bps_min_for_trend:
            trend_parts.append(
                min(
                    1.0,
                    (f.daily_open_range_bps - cfg.daily_open_range_bps_min_for_trend)
                    / 100.0,  # нормировка
                )
            )
        else:
            trend_parts.append(0.0)

        # мало пересечений daily_open
        if f.daily_open_cross_freq < cfg.daily_open_cross_freq_trend_max:
            trend_parts.append(
                min(
                    1.0,
                    (cfg.daily_open_cross_freq_trend_max - f.daily_open_cross_freq)
                    / max(cfg.daily_open_cross_freq_trend_max, 1e-6),
                )
            )
        else:
            trend_parts.append(0.0)

        trend_score = sum(trend_parts) / len(trend_parts)

        # 2) RANGE score ∈ [0,1]
        range_parts: list[float] = []

        # ATR в нижних квантилях
        if f.atr_quantile_1d < cfg.atr_quantile_range_thr:
            range_parts.append(
                min(
                    1.0,
                    (cfg.atr_quantile_range_thr - f.atr_quantile_1d)
                    / max(cfg.atr_quantile_range_thr, 1e-6),
                )
            )
        else:
            range_parts.append(0.0)

        # weakProgress низкий → ping-pong
        if f.weak_progress < cfg.weak_progress_range_max:
            range_parts.append(
                min(
                    1.0,
                    (cfg.weak_progress_range_max - f.weak_progress)
                    / max(cfg.weak_progress_range_max, 1e-6),
                )
            )
        else:
            range_parts.append(0.0)

        # недалеко от daily_open
        if f.daily_open_range_bps < cfg.daily_open_range_bps_max_for_range:
            range_parts.append(
                min(
                    1.0,
                    (cfg.daily_open_range_bps_max_for_range - f.daily_open_range_bps)
                    / max(cfg.daily_open_range_bps_max_for_range, 1e-6),
                )
            )
        else:
            range_parts.append(0.0)

        # много пересечений daily_open
        if f.daily_open_cross_freq > cfg.daily_open_cross_freq_range_min:
            range_parts.append(
                min(
                    1.0,
                    (f.daily_open_cross_freq - cfg.daily_open_cross_freq_range_min)
                    / max(1.0 - cfg.daily_open_cross_freq_range_min, 1e-6),
                )
            )
        else:
            range_parts.append(0.0)

        range_score = sum(range_parts) / len(range_parts)

        # 3) итоговая оценка ∈ [-1,1]
        raw_score = trend_score - range_score
        score = max(-1.0, min(1.0, raw_score))

        if trend_score >= 0.6 and trend_score > range_score + 0.2:
            regime = MarketRegime.TREND
        elif range_score >= 0.6 and range_score > trend_score + 0.2:
            regime = MarketRegime.RANGE
        elif max(trend_score, range_score) < 0.4:
            regime = MarketRegime.UNKNOWN
        else:
            regime = MarketRegime.MIXED

        return regime, score

    # --- публичные методы ---

    def _detect_regime_from_snapshot(self, snapshot) -> str:
        """
        Определяет строковый лейбл режима по snapshot.
        """
        if hasattr(snapshot, 'features') and snapshot.features:
            # Используем существующие features для определения
            regime, score = self._decide_regime(snapshot.features)
            if regime == MarketRegime.TREND:
                return "trending"
            elif regime == MarketRegime.RANGE:
                return "ranging"
            elif regime == MarketRegime.MIXED:
                return "mixed"
        return "unknown"

    def detect(self, snapshot, session: str, daily_stats) -> RegimeState:
        """
        Определяет режим рынка по snapshot данным.
        """
        label = self._detect_regime_from_snapshot(snapshot)
        # Для простоты возвращаем базовые значения, можно расширить логику
        trend_score = 0.0
        range_score = 0.0
        session_bias = self._cfg.session_bias_default.get(session, 0.0)
        daily_open_cross_freq = daily_stats.get('cross_freq', 0.0) if daily_stats else 0.0

        return RegimeState(
            label=label,
            trend_score=trend_score,
            range_score=range_score,
            session_bias=session_bias,
            daily_open_cross_freq=daily_open_cross_freq,
            ts=time.time(),
            symbol=getattr(snapshot, 'symbol', ''),
        )

    def update(
        self,
        symbol: str,
        ts: float,
        close_price: float,
        daily_open: float,
        atr_intraday_bps: float,
        atr_quantile_1d: float,
        weak_progress: float,
        vwap_distance_bps: float,
        vwap_trend_bps: float,
        session: str = "",
    ) -> RegimeState:
        """
        Обновляет режим и возвращает новое состояние.
        daily_open_range_bps / daily_open_cross_freq считаются на истории здесь.
        """
        hist = self._history[symbol]
        hist.append((ts, close_price, daily_open))
        dopen_range_bps, cross_freq = self._compute_daily_open_metrics(hist)

        # Создаем snapshot-like объект для detect
        snapshot = type('Snapshot', (), {
            'symbol': symbol,
            'features': RegimeFeatures(
                atr_intraday_bps=atr_intraday_bps,
                atr_quantile_1d=atr_quantile_1d,
                weak_progress=weak_progress,
                vwap_distance_bps=vwap_distance_bps,
                vwap_trend_bps=vwap_trend_bps,
                daily_open_range_bps=dopen_range_bps,
                daily_open_cross_freq=cross_freq,
            )
        })()

        daily_stats = {'cross_freq': cross_freq}
        state = self.detect(snapshot, session, daily_stats)
        self._last_state[symbol] = state
        return state

    def last_state(self, symbol: str) -> RegimeState | None:
        """Можно использовать для логов / визуализации по времени."""
        return self._last_state.get(symbol)

    def _update_regime_history(self, ctx: OrderflowContext, bar_index: int | None = None) -> None:
        if ctx.symbol is None or ctx.last_price is None:
            return

        now = ctx.ts_utc or time.time()

        # сторон VWAP
        vwap_side = 0
        if ctx.vwap is not None:
            diff_v = ctx.last_price - ctx.vwap
            if diff_v > 0.0:
                vwap_side = 1
            elif diff_v < 0.0:
                vwap_side = -1

        # сторона daily_open
        daily_open_side = 0
        if ctx.daily_open is not None:
            diff_o = ctx.last_price - ctx.daily_open
            if diff_o > 0.0:
                daily_open_side = 1
            elif diff_o < 0.0:
                daily_open_side = -1

        hist = self._regime_history[ctx.symbol]
        hist.append(
            RegimeSample(
                ts=now,
                price=ctx.last_price,
                vwap_side=vwap_side,
                daily_open_side=daily_open_side,
                bar_index=bar_index,
            )
        )

    # ---------- HTF levels and geometry ----------
    def _get_htf_levels(self, symbol: str) -> HTFLevels | None:
        """
        Базовый способ получить HTF уровни.
        Конкретный источник данных инкапсулирован в self._htf_provider.
        """
        if self._htf_provider is None:
            return None
        return self._htf_provider.get_levels(symbol)

    def _build_geo_zone_hits(
        self,
        ctx: OrderflowContext,
        htf: HTFLevels,
    ) -> list[GeoZoneHit]:
        hits: list[GeoZoneHit] = []
        price = ctx.last_price
        if price is None:
            return hits

        atr_htf_bps = ctx.atr_htf_bps or 0.0
        # fallback если ATR нет — просто используем 50 б.п.
        if atr_htf_bps <= 0.0:
            atr_htf_bps = 50.0

        def add_level(level_price: float, zone_type: ZoneType, strength: float) -> None:
            dist_rel = abs(price - level_price) / max(level_price, 1e-6)
            dist_bps = dist_rel * 10_000.0
            hits.append(
                GeoZoneHit(
                    zone_type=zone_type,
                    zone_price=level_price,
                    dist_bps=dist_bps,
                    atr_htf_bps=atr_htf_bps,
                    dist_rel_atr=dist_bps / atr_htf_bps,
                    strength=strength,
                )
            )

        # day levels
        add_level(htf.pdh, "PDH", strength=0.7)
        add_level(htf.pdl, "PDL", strength=0.7)
        add_level(htf.pdm, "PDM", strength=0.4)

        # week levels
        add_level(htf.week_hi, "WEEK_HI", strength=0.9)
        add_level(htf.week_lo, "WEEK_LO", strength=0.9)

        # session opens
        add_level(htf.asia_open, "ASIA_OPEN", strength=0.5)
        add_level(htf.europe_open, "EUROPE_OPEN", strength=0.6)
        add_level(htf.us_open, "US_OPEN", strength=0.7)

        # OB / FVG как зоны (если цена внутри — dist_rel_atr=0)
        for z in htf.ob_zones:
            low, high = z["low"], z["high"]
            if low <= price <= high:
                center = 0.5 * (low + high)
                dist_rel = abs(price - center) / max(center, 1e-6)
                dist_bps = dist_rel * 10_000.0
                hits.append(
                    GeoZoneHit(
                        zone_type="HTF_OB",
                        zone_price=center,
                        dist_bps=dist_bps,
                        atr_htf_bps=atr_htf_bps,
                        dist_rel_atr=dist_bps / atr_htf_bps,
                        strength=z.get("strength", 1.0),
                    )
                )

        for z in htf.fvg_zones:
            low, high = z["low"], z["high"]
            if low <= price <= high:
                center = 0.5 * (low + high)
                dist_rel = abs(price - center) / max(center, 1e-6)
                dist_bps = dist_rel * 10_000.0
                hits.append(
                    GeoZoneHit(
                        zone_type="HTF_FVG",
                        zone_price=center,
                        dist_bps=dist_bps,
                        atr_htf_bps=atr_htf_bps,
                        dist_rel_atr=dist_bps / atr_htf_bps,
                        strength=z.get("strength", 0.8),
                    )
                )

        return hits

    def _compute_cross_bias(self, symbol: str) -> float | None:
        hist = self._regime_history.get(symbol)
        if not hist or len(hist) < 3:
            return None

        vwap_crosses = 0
        open_crosses = 0
        pairs = 0

        prev = hist[0]
        for cur in list(hist)[1:]:
            # пересечения VWAP
            if prev.vwap_side != 0 and cur.vwap_side != 0 and prev.vwap_side != cur.vwap_side:
                vwap_crosses += 1
            # пересечения daily_open
            if prev.daily_open_side != 0 and cur.daily_open_side != 0 and prev.daily_open_side != cur.daily_open_side:
                open_crosses += 1

            pairs += 1
            prev = cur

        if pairs == 0:
            return None

        cross_rate_vwap = vwap_crosses / pairs          # [0..1]
        cross_rate_open = open_crosses / pairs          # [0..1]
        cross_rate = 0.5 * (cross_rate_vwap + cross_rate_open)

        # интерпретация:
        # cross_rate ≈ 0 → редко пересекаем → тренд → bias ~ +1
        # cross_rate ≈ 1 → постоянно пересекаем → рендж → bias ~ -1
        bias = 1.0 - 2.0 * max(0.0, min(1.0, cross_rate))  # [0..1] → [+1..-1]
        return bias

    def _detect_regime_from_ctx(self, ctx: OrderflowContext) -> MarketRegime:
        # 1) обновляем историю (для daily_open_cross_freq)
        self._update_regime_history(ctx)

        cfg = self._cfg
        feats = self._compute_regime_features(ctx)

        score = 0.0
        weight_sum = 0.0

        def acc(val: float | None, w: float) -> None:
            nonlocal score, weight_sum
            if val is None or w <= 0.0:
                return
            score += w * val
            weight_sum += w

        acc(feats.atr_bias, cfg.atr_weight)
        acc(feats.delta_dir_bias, cfg.delta_weight)
        acc(feats.vwap_dev_bias, cfg.vwap_dev_weight)
        acc(feats.daily_open_dev_bias, cfg.daily_open_dev_weight)
        acc(feats.daily_open_cross_bias, cfg.daily_open_cross_weight)
        acc(feats.htf_prox_bias, cfg.htf_level_weight)
        acc(feats.weak_progress_bias, cfg.weak_progress_weight)
        acc(feats.session_bias, cfg.session_weight)

        if weight_sum <= 0.0:
            ctx.market_regime_score = 0.0
            ctx.market_regime = MarketRegime.MIXED
            return ctx.market_regime

        regime_score = score / weight_sum  # [-1..+1]
        ctx.market_regime_score = regime_score

        if regime_score >= cfg.regime_trend_threshold:
            regime = MarketRegime.TREND
        elif regime_score <= cfg.regime_range_threshold:
            regime = MarketRegime.RANGE
        else:
            regime = MarketRegime.MIXED

        ctx.market_regime = regime
        return regime


@dataclass
class Tick:
    ts: int
    bid: float
    ask: float
    last: float
    volume: float
    flags: int
    is_buyer_maker: bool | None = None


@dataclass
class OrderflowContext:
    ts: int
    price: float
    z_delta: float
    weak_progress: bool
    obi: float
    obi_avg: float
    obi_sustained: bool
    atr: float
    pivots: dict[str, float] | None
    delta_window: deque
    current_delta: float
    delta_bucket: float
    # Microstructure fields for crypto
    spread_bps: float = 0.0
    realized_bps: float = 0.0
    realized_ema_bps: float = 0.0
    adverse_ratio_ema: float = 0.0
    market_mode: str = "mixed"

    # L2 metrics (from book snapshots)
    obi_20: float = 0.0
    obi_avg_20: float = 0.0
    obi_sustained_20: bool = False

    depth_bid_5: float = 0.0
    depth_ask_5: float = 0.0
    depth_bid_20: float = 0.0
    depth_ask_20: float = 0.0

    slope_bid_20: float = 0.0
    slope_ask_20: float = 0.0

    microprice_shift_bps_20: float = 0.0

    wall_bid: bool = False
    wall_ask: bool = False
    wall_bid_dist_bps: float = 0.0
    wall_ask_dist_bps: float = 0.0

    # Refill/Depletion proxy (direction-specific will be computed at bucket-boundary)
    bid_top5_ratio: float = 0.0
    ask_top5_ratio: float = 0.0
    bid_top3_ratio: float = 0.0
    ask_top3_ratio: float = 0.0

    refill_score: float = 0.0
    depletion_score: float = 0.0

    impact_proxy: float = 0.0

    # L2 staleness/debug
    l2_ts: int = 0
    l2_age_ms: int = 0
    l2_is_stale: bool = True
    l2_age_ms_raw: int = 0
    l2_skew_ms: int = 0
    l2_skew_flag: bool = False
    # optional diag relative to tick timestamp
    l2_age_ms_tick_raw: int = 0
    l2_age_ms_tick: int = 0
    l2_skew_tick_ms: int = 0
    l2_skew_tick_flag: bool = False

    # L3-lite (trade vs cancel decomposition, ETA)
    taker_buy_rate_ema: float = 0.0
    taker_sell_rate_ema: float = 0.0
    cancel_bid_rate_ema: float = 0.0
    cancel_ask_rate_ema: float = 0.0
    cancel_to_trade_bid: float = 0.0
    cancel_to_trade_ask: float = 0.0
    eta_fill_bid_sec: float = 0.0
    eta_fill_ask_sec: float = 0.0

    # L3-lite (queue-events proxy) - дополнительные метрики
    taker_buy_qty_bucket: float = 0.0
    taker_sell_qty_bucket: float = 0.0
    pull_ask_qty_proxy: float = 0.0
    pull_bid_qty_proxy: float = 0.0

    # Burstiness metrics
    burst_trade_count_bucket: int = 0
    burst_rate_short: float = 0.0
    burst_rate_long: float = 0.0
    burst_ratio: float = 0.0
    burst_cv_dt: float = 0.0
    burst_fano_counts: float = 0.0
    burst_flip_ratio: float = 0.0

    # Touch-level (depletion/refill) mini-tracker (v2 simplified)
    touch_bid_tag: str = "none"
    touch_ask_tag: str = "none"
    touch_bid_rho: float = 0.0
    touch_ask_rho: float = 0.0
    touch_bid_traded_w: float = 0.0
    touch_ask_traded_w: float = 0.0
    touch_bid_drop_w: float = 0.0
    touch_ask_drop_w: float = 0.0
    touch_is_stale: bool = True

    # Confidence model (optional)
    confidence: float = 0.0
    confidence_breakdown: dict[str, float] = field(default_factory=dict)

    # Market regime
    regime: MarketRegime = MarketRegime.UNKNOWN
    regime_trend_score: float = 0.0    # ∈ [-1, +1]
    regime_range_score: float = 0.0    # ∈ [-1, +1]

    # Additional price context
    last_price: float | None = None
    vwap: float | None = None
    daily_open: float | None = None
    daily_open_dist_bps: float | None = None  # расстояние от daily_open в б. п.

    # Regime features
    cum_delta_slope: float | None = None  # наклон cumulative delta
    atr_q_14: float | None = None  # ATR quantile

    # Market regime
    market_regime: MarketRegime = MarketRegime.UNKNOWN

    # Symbol identifier
    symbol: str = "unknown"

    # Extended regime fields
    ts_utc: float | None = None           # timestamp текущего бара/тик-бакета

    daily_atr_bps: float | None = None    # средний дневной ATR (в б.п.) по инструменту

    weak_progress_ratio: float | None = None  # |range| / ATR (по текущему бару/сигналу)

    htf_level_dist_bps: float | None = None   # расстояние до ближайшего HTF уровня (H1/H4/D1) в б.п.

    session_label: str | None = None          # "asia"/"london"/"ny"/"late_us" и т.п.

    # HTF ATR в б.п. (например дневной или H4)
    atr_htf_bps: float | None = None

    # --- NEW: режим ---
    market_regime_score: float = 0.0
    regime_features: RegimeFeatures | None = None

    # геометрия
    geo_zone_hits: list[GeoZoneHit] | None = None
    is_new_local_extreme: bool | None = None     # строим новый важный high/low
    geometry_score: float | None = None          # 0..1

    # ликвидность
    liquidity_ctx: LiquidityContext | None = None

    # Дополнительно: вес паттерна (для скоринга/агрегации)
    pattern_weight: float = 1.0

    # скоринг
    base_score: float = 0.0
    final_score: float = 0.0

    # локальная калибровка
    session: str = ""  # asia / europe / us
    regime_label: str = ""   # trend / range / mixed

    # локальные метрики (квантили и пороги)
    delta_spike_z_local_q: float = float("nan")
    delta_spike_z_local_thr: float = float("nan")
    obi_local_q: float = float("nan")
    obi_local_thr: float = float("nan")
    weak_progress_local_q: float = float("nan")
    weak_progress_local_thr: float = float("nan")
    atr_quantile_local_q: float = float("nan")
    atr_quantile_local_thr: float = float("nan")

    # Golden pattern flags
    is_golden_pattern: bool = False
    golden_pattern_label: str | None = None

    # Signal family для regime guard (контроль качества)
    family: str = "unknown"        # "volatilitySpike", "weakProgress", "deltaSpikeZ_OBI", ...
    venue: str = "unknown"         # "binance", "mt5", "deribit"
    timeframe: str = "unknown"     # "1m", "5m"

    # Направление сигнала: +1 = long, -1 = short, 0 = neutral
    direction: int = 0
    side: str = ""  # "long" / "short"
    pattern_name: str = ""  # имя паттерна для сигнала

    # L3-Lite метрики
    cancel_to_trade_bid_5s: float = 0.0
    cancel_to_trade_ask_5s: float = 0.0
    cancel_to_trade_bid_20s: float = 0.0
    cancel_to_trade_ask_20s: float = 0.0

    # Experiment layer fields
    experiment_id: str | None = None           # ID активного эксперимента
    experiment_variant: str | None = None      # "control" | "treatment"
    experiment_config: dict[str, Any] = field(default_factory=dict)  # конфиг эксперимента

    obi_5: float = 0.0
    obi_50: float = 0.0
    obi_persistence_score: float = 0.0

    # Дополнительные L3-метрики
    microprice_velocity_bps: float = 0.0
    queue_pressure_bid: float = 0.0
    queue_pressure_ask: float = 0.0
    market_depth_imbalance: float = 0.0

    # L3 scoring results (для отладки)
    _l3_score: float = 0.0
    _l3_terms: dict[str, float] | None = None
    _l3_profile: dict[str, float] | None = None

    # 4.1: Dependency policies (fail-open/fail-closed)
    data_quality_flags: list[str] = field(default_factory=list)
    l3_score01: float = 0.5         # нейтраль по умолчанию
    l3_missing_rate: float = 0.0
    l2_score01: float = 0.5         # нейтраль по умолчанию (если L2 не прикреплён/нет провайдера)
    l2_missing_rate: float = 0.0
    geometry_score: float = 0.1     # нейтраль по умолчанию (HTF missing)


@dataclass(frozen=True)
class PublishResult:
    sent: bool   # реально поставили в outbox
    dedup: bool  # дедуп сработал (или публикация отключена/подавлена)


def _parse_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = _to_str(v).strip().lower()
    if s in {"true", "1", "yes", "y", "on"}:
        return True
    if s in {"false", "0", "no", "n", "off"}:
        return False
    return None


def _to_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def robust_zscore_mad(
    values: list[float],
    last_value: float,
    eps: float = 1e-12,
    gpu_service: Any | None = None
) -> float:
    """
    Robust Z-score через median/MAD.
    z = 0.6745 * (x - median) / MAD
    
    ✅ ОПТИМИЗИРОВАНО: Использует GPU метод compute_robust_zscore_mad() для полного GPU ускорения.
    """
    if len(values) < 30:
        return 0.0

    # ✅ ОПТИМИЗАЦИЯ: Используем новый GPU метод если доступен и окно большое
    gpu_min_n = int(os.getenv("GPU_MIN_N", "2048"))
    if gpu_service and len(values) >= gpu_min_n and hasattr(gpu_service, 'compute_robust_zscore_mad'):
        try:
            import numpy as np
            arr = np.array(values, dtype=np.float32)
            # Используем оптимизированный GPU метод
            return gpu_service.compute_robust_zscore_mad(arr, last_value, eps)
        except Exception:
            pass  # fallback на CPU

    # GPU acceleration (старый способ, fallback)
    if gpu_service and hasattr(gpu_service, 'use_gpu') and gpu_service.use_gpu:
        try:
            import numpy as np
            arr = np.array(values, dtype=np.float32)

            # Используем GPU для median
            median = float(np.median(arr))  # NumPy median, GPU service может оптимизировать
            abs_dev = np.abs(arr - median)
            mad = float(np.median(abs_dev))

            if mad <= eps:
                return 0.0
            return 0.6745 * (last_value - median) / mad
        except Exception:
            pass  # fallback на CPU

    # CPU fallback
    xs = sorted(values)
    n = len(xs)
    median = xs[n // 2] if (n % 2 == 1) else 0.5 * (xs[n // 2 - 1] + xs[n // 2])

    abs_dev = [abs(v - median) for v in xs]
    abs_dev.sort()
    mad = abs_dev[n // 2] if (n % 2 == 1) else 0.5 * (abs_dev[n // 2 - 1] + abs_dev[n // 2])

    if mad <= eps:
        return 0.0
    return 0.6745 * (last_value - median) / mad


class BaseOrderFlowHandler(ABC):
    """
    Надежный и эффективный базовый handler.

    Улучшения/принципы:
    - pending recovery: claim + обработка, ACK только на успех
    - различение transient infra ошибок vs poison сообщений (DLQ только для poison)
    - снижение нагрузки: robust-zscore/сигналы считаются только на границе delta-bucket
    - OBI только из book-stream; при stale book сбрасываем sustained/state
    - корректная статистика: published_signals увеличивается только при реальном publish в outbox
    - anti-chatter: cooldown по (kind, level_key) поверх min_signal_interval
    """

    DLQ_STREAM_ENV = "ORDERFLOW_DLQ_STREAM"
    DLQ_DEFAULT = RS.DLQ_ORDERFLOW

    def _get_source_name(self) -> str:
        return "OrderFlow"

    def _get_strategy_key(self) -> str:
        return "orderflow"

    def _get_signal_stream(self) -> str:
        return os.getenv("ORDERFLOW_SIGNAL_STREAM") or f"signals:{self._get_strategy_key()}:{self.symbol}"

    def __init__(
        self,
        symbol: str,
        config: OrderFlowConfig | None = None,
        *,
        source_name: str = "OrderFlow",
        signal_stream_prefix: str = "signals:orderflow",
        htf_provider: HTFLevelsProvider | None = None,
        local_calibration: LCStoreV2 | None = None,
        unified_pipeline: UnifiedSignalPipeline | None = None,
        # New modular services
        regime_service: MarketRegimeService | None = None,
        geometry_service: HTFLevelsService | None = None,
        calibration_service: LocalCalibrationService | None = None,
    ):
        self.symbol = symbol
        self.config = config or get_config(symbol, use_env=True)
        self.specs = self._get_symbol_specs()

        self._steady_clock = SteadyClock()

        # bad-time quarantine (prevents corrupt signal generation when ts is broken)
        self._bad_time_quarantine = BadTimeQuarantine(BadTimeQuarantinePolicy(), inc=self._inc_metric)

        self.source_name = source_name
        self.signal_stream_prefix = signal_stream_prefix

        # Поля для regime guard
        self.venue = "mt5"  # по умолчанию для базового orderflow
        self.timeframe = "1m"  # по умолчанию
        self.family = "orderflow"  # тип сигнала для контроля качества

        self.logger = setup_logger(f"{self.__class__.__name__}:{symbol}")

        # Redis разделение: ticks vs state
        redis_url_main = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        redis_url_ticks = os.getenv("REDIS_TICKS_URL") or redis_url_main
        self.redis_ticks = get_optimized_redis_client(redis_url_ticks)
        self.redis = get_optimized_redis_client(redis_url_main)
        self.dual_redis = get_dual_signals_redis()

        # HTF levels provider
        self._htf_provider = htf_provider

        # Local calibration store
        # caches (state Redis)
        self._pivot_cache = PivotPointsCache(self.redis)
        self._atr_cache = ATRCache(self.redis, ttl=15)
        self._redis_atr_warning_logged = False
        # quality markers (переносятся в ctx на bucket boundary)
        self._quality_flags_bucket: set[str] = set()
        self._quality_gate = None  # lazy

        # Streams
        self.tick_stream = os.getenv(f"{symbol}_TICK_STREAM") or f"stream:tick_{symbol}"
        self.book_stream = os.getenv(f"{symbol}_BOOK_STREAM") or f"stream:book_{symbol}"
        self.l3_stream = os.getenv(f"{symbol}_L3_STREAM") or f"stream:l3_{symbol}"
        self.notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
        self.audit_signal_stream = os.getenv("SIGNAL_AUDIT_STREAM") or f"signals:audit:{symbol}"
        self.signal_stream = self._get_signal_stream()

        # notify throttling
        self.notify_signal_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", RS.NOTIFY_SIGNAL_COUNTER)
        try:
            self.notify_signal_every_n = max(1, int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1")))
        except ValueError:
            self.notify_signal_every_n = 1

        # Consumer group
        self.group = os.getenv(f"{symbol}_GROUP") or f"{symbol.lower()}-signal-group"
        self.consumer_name_prefix = os.getenv(f"{symbol}_CONSUMER") or f"{symbol.lower()}-handler"

        # Outbox (signals redis)
        self.outbox_enabled = os.getenv("USE_SIGNAL_OUTBOX", "true").lower() == "true"
        signals_redis_url = os.getenv("REDIS_SIGNALS_URL", redis_url_main)
        self.outbox = SignalOutboxPublisher(redis_url=signals_redis_url, settings=OutboxSettings())
        self.dedup_ttl_ms = int(os.getenv(
            "SIGNAL_DEDUP_TTL_MS",
            str(int(self.config.min_signal_interval_sec * 1000))
        ))

        # GPU acceleration service (singleton)
        self.gpu_service = get_gpu_service()

        # Modular services
        self._regime_service = regime_service or MarketRegimeService()
        self._geometry_service = geometry_service or HTFLevelsService(htf_provider=htf_provider)
        self._calibration_service = calibration_service or LocalCalibrationService(local_calibration or LCStoreV2())

        # Legacy regime guard services (for compatibility)
        self.regime_runtime = RegimeRuntimeState(redis_url_main)
        self.black_zone_scheduler = BlackZoneScheduler(os.getenv("DATABASE_URL", "postgresql://user:password@localhost/db"))

        # NEW: signal scoring engine
        try:
            from signal_quality import SignalQualityEstimator

            pg_dsn = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/trade")
            calib_store = LCStoreV2()
            calib_store.load_from_db(pg_dsn)
            scoring_cfg = ScoringConfig.from_env()

            # Initialize quality estimator
            quality_estimator = SignalQualityEstimator(pg_dsn)
            self._scoring_engine = SignalScoringEngine(calib_store, scoring_cfg, quality_estimator)
        except Exception as e:
            print(f"⚠️ Failed to initialize signal scoring engine: {e}")
            self._scoring_engine = None

        # NEW: Unified Signal Execution System
        try:
            from signal_exec import (
                ExecutionPlanner,
                SignalBus,
                SignalPerformanceTracker,
                SignalRepository,
                SignalService,
            )

            # Initialize components
            self._execution_repo = SignalRepository(pg_dsn)
            setup_configs = self._execution_repo.load_setup_configs()
            self._execution_planner = ExecutionPlanner(setup_configs)
            self._performance_tracker = SignalPerformanceTracker(
                repo=self._execution_repo,
                ttd_target_R=1.0,
                max_ttd_bars=30
            )

            # Redis bus for signal distribution
            redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            self._signal_bus = SignalBus(redis_url)

            # Unified service
            self._signal_service = SignalService(
                repo=self._execution_repo,
                planner=self._execution_planner,
                tracker=self._performance_tracker,
                bus=self._signal_bus,
            )

            self.logger.info(f"✅ Unified signal execution initialized with {len(setup_configs)} configs")
        except ImportError:
            self.logger.warning("⚠️ signal_exec module not available, signal execution disabled")
            self._signal_service = None
        except Exception as e:
            self.logger.exception(f"Failed to initialize signal execution: {e}")
            self._signal_service = None

        # reliability settings
        self.max_fail_retries = int(os.getenv("STREAM_MSG_MAX_RETRIES", "3"))
        self.claim_interval_ms = int(os.getenv("STREAM_CLAIM_INTERVAL_MS", "5000"))
        self.claim_min_idle_ms = int(os.getenv("STREAM_CLAIM_MIN_IDLE_MS", "15000"))
        self.claim_count = int(os.getenv("STREAM_CLAIM_COUNT", "50"))
        self.dlq_stream = os.getenv(self.DLQ_STREAM_ENV, self.DLQ_DEFAULT)

        # signal chatter controls
        self.level_cooldown_ms = int(os.getenv("LEVEL_SIGNAL_COOLDOWN_MS", "15000"))
        self._last_level_signal_ts: dict[tuple[str, str], int] = {}
        self.breakout_min_dist_atr = float(os.getenv("BREAKOUT_MIN_DIST_ATR", "0.0"))

        # breakout: strict OBI confirmation (по умолчанию строго)
        self.breakout_require_obi = os.getenv("BREAKOUT_REQUIRE_OBI", "true").lower() == "true"

        # per-signal Z thresholds (optional; defaults preserve previous behavior)
        self.breakout_z_threshold = float(os.getenv("BREAKOUT_Z_THRESHOLD", str(self.config.delta_z_threshold)))
        self.absorption_z_threshold = float(os.getenv("ABSORPTION_Z_THRESHOLD", str(self.config.delta_z_threshold)))
        self.extreme_z_mult = float(os.getenv("EXTREME_Z_MULT", "1.6"))
        self.extreme_z_threshold = float(os.getenv(
            "EXTREME_Z_THRESHOLD",
            str(self.config.delta_z_threshold * self.extreme_z_mult),
        ))
        # gate for main logic: allow breakout/absorption if any of their thresholds is met
        self.main_z_threshold = float(os.getenv(
            "MAIN_Z_THRESHOLD",
            str(min(self.config.delta_z_threshold, self.breakout_z_threshold, self.absorption_z_threshold)),
        ))

        # absorption gating (optional: allow disabling weak_progress hard requirement)
        self.absorption_require_weak_progress = os.getenv("ABSORPTION_REQUIRE_WEAK_PROGRESS", "true").lower() == "true"

        # analysis state
        self.is_running = False
        self.last_signal_ts = 0

        # Unified Signal Pipeline
        if unified_pipeline is not None:
            # Use provided pipeline (for testing/customization)
            self._unified_pipeline = unified_pipeline
        else:
            # Create default pipeline with all services
            try:
                from signals.calibration_service import CalibrationService
                from signals.exec_filters import ExecFiltersGroup
                from signals.golden_pattern_service import GoldenPatternService
                from signals.signal_publisher import SignalPublisher
                from signals.unified_pipeline import UnifiedSignalPipeline

                # Create services
                golden_logic = GoldenPatternService()
                calibrator = CalibrationService(calibration_store=self.local_calibration)
                exec_filters = ExecFiltersGroup()
                publisher = SignalPublisher(redis_client=self.redis, outbox=self.outbox)

                # Create unified pipeline
                self._unified_pipeline = UnifiedSignalPipeline(
                    scoring_engine=self._scoring_engine,
                    regime_service=self._regime_service,
                    golden_logic=golden_logic,
                    exec_filters=exec_filters,
                    publisher=publisher,
                    calibrator=calibrator,
                )

                self.logger.info("✅ UnifiedSignalPipeline initialized successfully")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to initialize UnifiedSignalPipeline: {e}")
                self._unified_pipeline = None

        # Use legacy path only if pipeline creation failed
        # After testing: fully switch to unified pipeline
        self._use_legacy_path = False

        # regime detection
        self.regime_state = RegimeState(label="unknown", trend_score=0.0, range_score=0.0)
        self._regime_lookback_minutes = int(os.getenv("REGIME_LOOKBACK_MINUTES", "60"))
        self._regime_window = deque(maxlen=self._regime_lookback_minutes)

        # local calibration - main store for orderflow metrics
        if local_calibration is not None:
            self.local_calibration = local_calibration
        else:
            self.local_calibration = LCStoreV2()
            try:
                pg_dsn = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/trade")
                self.local_calibration.load_from_db(pg_dsn)
                self.logger.info("Loaded local calibration from database")
            except Exception as e:
                self.logger.warning("Failed to load local calibration from DB: %s", e)

        # legacy calibration store for signal context (if needed)
        self._legacy_calibration_store = None

        # delta bucketization
        self.robust_backend = os.getenv("ROBUST_Z_BACKEND", "auto")
        self.delta_window = deque(maxlen=self.config.delta_window_ticks)
        self.delta_bucket_ms = int(os.getenv("DELTA_BUCKET_MS", "1000"))
        self._bucket_id: int | None = None
        self._bucket_sum = 0.0
        self._last_bucket_value = 0.0
        self.max_zero_buckets = int(os.getenv("DELTA_BUCKET_MAX_ZERO_FILL", "3"))
        self._last_z_delta = 0.0

        # ----- L2 tracker (book -> full metrics + refill/depletion proxies)
        self.l2_k_small = int(os.getenv("L2_K_SMALL", "5"))
        self.l2_k_large = int(os.getenv("L2_K_LARGE", "20"))
        self.l2_wall_mult = float(os.getenv("L2_WALL_MULT", "3.0"))
        self.l2_wall_max_dist_bps = float(os.getenv("L2_WALL_MAX_DIST_BPS", "15.0"))
        self.l2 = L2BookTracker(
            k_small=self.l2_k_small,
            k_large=self.l2_k_large,
            wall_mult=self.l2_wall_mult,
            wall_max_dist_bps=self.l2_wall_max_dist_bps,
        )
        self._l2_last: L2Snapshot | None = None

        # ----- Touch-level tracker (drop@touch, traded@touch, refill/depletion tags)
        tick_size = float(getattr(self.specs, "tick_size", 0.0) or 0.0)
        self.touch = TouchLevelTracker(
            window_ms=int(os.getenv("TOUCH_WINDOW_MS", "500")),
            tau_refill_ms=int(os.getenv("TOUCH_TAU_REFILL_MS", "250")),
            recover_frac=float(os.getenv("TOUCH_RECOVER_FRAC", "0.90")),
            rho_refill_min=float(os.getenv("TOUCH_RHO_REFILL_MIN", "1.5")),
            rho_depletion_max=float(os.getenv("TOUCH_RHO_DEPLETION_MAX", "1.5")),
            tick_size=tick_size,
            max_touch_ticks=int(os.getenv("TOUCH_MAX_TOUCH_TICKS", "1")),
            book_fresh_ms=int(os.getenv("TOUCH_BOOK_FRESH_MS", "250")),
        )

        # ✅ GPU Batch Buffering: буфер для батч обработки книг
        self._book_buffer: list[dict[str, Any]] = []
        self._book_buffer_max = int(os.getenv("L2_BATCH_SIZE", "10"))
        self._book_buffer_timeout_ms = int(os.getenv("L2_BATCH_TIMEOUT_MS", "100"))
        # book batch buffering timestamps
        self._book_buffer_last_flush_ts: int = 0    # когда мы реально флашили батч (важно!)
        self._l2_last_ts: int = 0

        # ----- L3-lite tracker (Binance L2+trades decomposition)
        self.l3_lite_enabled = os.getenv("L3_LITE_ENABLED", "true").lower() == "true"
        self.l3 = L3LiteTracker(
            alpha=float(os.getenv("L3_LITE_EMA_ALPHA", "0.08")),
            min_dt_ms=int(os.getenv("L3_LITE_MIN_DT_MS", "80")),
            enabled=self.l3_lite_enabled,
        )
        self._l3_last_stats: L3BucketStats | None = None
        # rolling robust z (GPU-aware ring buffer)
        self._delta_rr = RobustZscoreMADRolling(
            window_size=self.config.delta_window_ticks,
            threshold=3.0,
        )

        # L2 warn throttling
        self._last_l2_warn_ms = 0
        self._l2_warn_every_ms = int(os.getenv("L2_WARN_EVERY_MS", "15000"))

        # ----- L3-lite queue-events proxy (дополнительные метрики)
        l3_alpha = float(os.getenv("L3_TAKER_RATE_EMA_ALPHA", "0.12"))
        self.l3_queue = L3QueueEventsProxy(bucket_ms=self.delta_bucket_ms, alpha=l3_alpha)
        self.l3_eps = float(os.getenv("L3_EPS", "1e-9"))

        # ETA evaluator (depth / taker_rate -> time-to-fill proxy)
        self.eta_eval: QueueETAEvaluator | None = None
        try:
            self.eta_eval = QueueETAEvaluator(eps=self.l3_eps)
        except Exception:
            self.eta_eval = None

        # ----- Burstiness tracker
        burst_half_life_short_ms = int(os.getenv("BURST_HALF_LIFE_SHORT_MS", "250"))
        burst_half_life_long_ms = int(os.getenv("BURST_HALF_LIFE_LONG_MS", "2000"))
        burst_fano_window_buckets = int(os.getenv("BURST_FANO_WINDOW_BUCKETS", "60"))
        burst_dt_alpha = float(os.getenv("BURST_DT_ALPHA", "0.05"))
        self.burst = BurstinessTracker(
            bucket_ms=self.delta_bucket_ms,
            half_life_short_ms=burst_half_life_short_ms,
            half_life_long_ms=burst_half_life_long_ms,
            fano_window_buckets=burst_fano_window_buckets,
            dt_alpha=burst_dt_alpha,
        )
        self._burst_last_stats: BurstStats | None = None

        # ---- Burst/quality gates (bucket-level) ----
        self.min_trades_breakout = int(os.getenv("MIN_TRADES_BREAKOUT", "20"))
        self.burst_ratio_min = float(os.getenv("BURST_RATIO_MIN", "1.6"))
        self.fano_min = float(os.getenv("FANO_MIN", "1.5"))
        self.flip_ratio_max = float(os.getenv("FLIP_RATIO_MAX", "0.70"))
        self.imbalance_min = float(os.getenv("IMBALANCE_MIN", "0.20"))  # OBI proxy

        # Execution-quality gating (burstiness + OBI + ETA)
        self.exec_filters_enabled = os.getenv("EXEC_FILTERS_ENABLED", "true").lower() == "true"
        # optional ETA gates (seconds)
        self.eta_max_sec = float(os.getenv("ETA_MAX_SEC", "2.5"))

        # ----- OBI sustained quality
        self.obi_use_fraction = os.getenv("OBI_SUSTAINED_USE_FRACTION", "true").lower() == "true"
        self.obi_min_samples = int(os.getenv("OBI_SUSTAINED_MIN_SAMPLES", "3"))
        self.obi_min_fraction = float(os.getenv("OBI_SUSTAINED_MIN_FRACTION", "0.6"))

        # OBI state (legacy 5-depth)
        self._last_obi = 0.0
        self._last_obi_ts = 0

        # separate OBI deques for 5 and 20 (avg+sustained stability)
        self._obi_state_5 = deque()
        self._obi_state_20 = deque()
        self._last_obi_20 = 0.0
        self._last_obi_20_ts = 0

        # ATR
        self.atr_calculator = ATR(period=14)

        # pivots
        self.daily_pivots: dict[str, float] | None = None
        self.last_pivot_date = None

        # bar range for weak progress (minute bar)
        self.bar_high = -1e9
        self.bar_low = 1e9
        self.bar_start_ts = 0

        # breakout cross - use previous evaluation price (bucket boundary), not every tick
        self._prev_eval_price: float | None = None

        # snapshot
        self.snap_prefix = os.getenv("SNAP_PREFIX", "signal:snap:")
        self.snap_ttl = int(os.getenv("SNAP_TTL", "21600"))

        # counters
        self.processed_ticks = 0
        self.processed_books = 0
        self.published_signals = 0
        self.signal_count_long = 0
        self.signal_count_short = 0

        self.max_tick_lag_ms = int(os.getenv("MAX_TICK_LAG_MS", "5000"))

        # ---- Signal metrics hook (candidates/veto histograms) ----
        # Создаём единый объект, чтобы:
        #  - candidates_total считался ДО emitter (в handler),
        #  - veto имел реальную причину из ConfirmationsEngine,
        #  - conf_factor_hist/final_score_hist логировались в одном месте.
        try:
            self._sigm = SignalMetrics(getattr(self, "_m2", None)) if SignalMetrics else None
        except Exception:
            self._sigm = None

        # ------------------------------------------------------------------
        # 6.2 Record & Replay hooks (optional, env-driven)
        #  - REPLAY_RECORD=1 enables writing ctx/tick/signal to JSONL
        #  - used for integration tests + debugging behaviour regressions
        # ------------------------------------------------------------------
        self._replay_recorder = None
        if ReplayRecorder is not None:
            try:
                rr = ReplayRecorder()
                if rr.enabled:
                    self._replay_recorder = rr
            except Exception:
                # fail-open: handler must not depend on recorder
                self._replay_recorder = None

    def __del__(self) -> None:
        # best-effort close, do not raise in GC
        try:
            rr = getattr(self, "_replay_recorder", None)
            if rr is not None:
                rr.close()
        except Exception:
            pass

        self.logger.info(
            "Init %s for %s | source=%s | tick=%s book=%s | "
            "Z: main=%.2f breakout=%.2f absorption=%.2f extreme=%.2f | OBI_thr=%.3f | bucket=%dms | "
            "breakout_strict_obi=%s | OBI_sustained: use_frac=%s min_samples=%d min_frac=%.2f | absorption_req_weak=%s | "
            "L2: k_small=%d k_large=%d wall_mult=%.1f wall_max_dist_bps=%.1f",
            self.__class__.__name__, symbol, self.source_name,
            self.tick_stream, self.book_stream,
            self.main_z_threshold, self.breakout_z_threshold,
            self.absorption_z_threshold, self.extreme_z_threshold,
            self.config.obi_threshold,
            self.delta_bucket_ms,
            self.breakout_require_obi,
            self.obi_use_fraction, self.obi_min_samples,
            self.obi_min_fraction, self.absorption_require_weak_progress,
            self.l2_k_small, self.l2_k_large, self.l2_wall_mult, self.l2_wall_max_dist_bps,
        )

    def _now_ms(self) -> int:
        return get_ny_time_millis()

    def _parse_tick(self, raw: Any) -> ParsedTick | None:
        """
        Wrapper над чистой функцией parse_tick.
        Важно:
          - в проде now_ms берём из _now_ms()
          - в тестах дергаем handlers.tick_parser.parse_tick напрямую (детерминизм)
        """
        return _parse_tick_pure(raw, now_ms=self._now_ms())

    def _classify_delta(self, tick: ParsedTick) -> float:
        """
        Wrapper над чистой функцией classify_delta.
        """
        return float(_classify_delta_pure(tick))

    def _replay_record_tick(self, tick_payload: dict[str, Any]) -> None:
        rr = getattr(self, "_replay_recorder", None)
        if rr is not None:
            with contextlib.suppress(Exception):
                rr.record_tick(tick_payload)

    def _replay_record_ctx(self, ctx: Any) -> None:
        rr = getattr(self, "_replay_recorder", None)
        if rr is not None:
            with contextlib.suppress(Exception):
                rr.record_ctx(ctx)

    @abstractmethod
    def _get_symbol_specs(self) -> SymbolSpecs:
        raise NotImplementedError

    def _get_min_confidence_for_symbol(self, symbol: str | None) -> float:
        """
        Возвращает минимальный порог confidence для символа.

        Базовый порог из env CRYPTO_SIGNAL_MIN_CONF (по умолчанию 30.0, но у пользователя 80).
        Для золота (XAU*) временно пониженный порог 20.

        Можно расширить для других символов по необходимости.
        """
        base_min_conf = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "30.0"))

        if not symbol:
            return base_min_conf

        sym = symbol.upper()

        # Все варианты золота
        if sym.startswith("XAU"):
            return 20.0

        return base_min_conf

    # -------------------- lifecycle --------------------

    def start(self) -> None:
        if self.is_running:
            return
        self.is_running = True
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

    def stop(self) -> None:
        self.is_running = False

    # -------------------- error classification --------------------

    def _is_transient_error(self, e: Exception) -> bool:
        """
        Transient (infrastructure) errors should NOT lead to DLQ and should NOT be ACKed.
        They must remain pending and be retried after Redis recovers.
        """
        if is_transient_redis_error(e):
            return True
        name = (e.__class__.__name__ or "").lower()
        msg = (str(e) or "").lower()
        tokens = (
            "timeout",
            "timed out",
            "connection",
            "conn",
            "broken pipe",
            "busy loading",
            "try again",
            "temporarily",
            "reset by peer",
        )
        return any(t in name for t in tokens) or any(t in msg for t in tokens)

    def _sanitize_dlq_payload(self, payload: dict[str, Any]) -> dict[str, str]:
        return sanitize_for_dlq(
            payload,
            max_depth=int(os.getenv("DLQ_MAX_DEPTH", "3")),
            max_length=int(os.getenv("DLQ_MAX_LENGTH", "1000")),
        )

    def _try_add_dlq_or_backoff(
        self,
        consumer: SyncRedisStreamHelper,
        dlq_payload: dict[str, Any],
        *,
        backoff: Backoff,
        where: str,
    ) -> bool:
        """
        Пишем DLQ. Если transient — НЕ ACK, даём backoff и возвращаем False
        (сообщение останется pending и будет переобработано).
        """
        try:
            consumer.add_dlq(self.dlq_stream, self._sanitize_dlq_payload(dlq_payload))
            return True
        except Exception as e:
            if self._is_transient_error(e):
                delay = backoff.next_sleep()
                self.logger.warning(
                    "Transient error while writing DLQ (%s, no ACK): %s (backoff=%.2fs)",
                    where,
                    e,
                    delay,
                )
                sleep_s(delay)
                return False
            raise

    def _ts_to_ms(self, ts: object, *, label: str = "ts") -> int:
        """
        Нормализует timestamp к миллисекундам (seconds/us/ns -> ms).
        """
        if ts is None:
            return 0
        try:
            v = int(ts)
        except Exception:
            return 0
        if v <= 0:
            return 0
        if v < 1_000_000_000_000:  # seconds
            return v * 1000
        if v > 10_000_000_000_000_000:  # ns
            return v // 1_000_000
        if v > 1_000_000_000_000_000:  # us
            return v // 1000
        return v

    def _l2_warn_allowed(self, now_ms: int) -> bool:
        if now_ms - getattr(self, "_last_l2_warn_ms", 0) < getattr(self, "_l2_warn_every_ms", 15000):
            return False
        self._last_l2_warn_ms = now_ms
        return True

    def _calc_l2_age_ms(self, *, tick_ts: object, book_ts: object) -> tuple[int, int]:
        """
        Возвращает (tick_ts_ms, l2_age_ms) с нормализацией единиц.
        l2_age_ms считается как tick_ts_ms - book_ts_ms (может быть отрицательным при skew).
        """
        t_ms = self._ts_to_ms(tick_ts, label="tick.ts")
        b_ms = self._ts_to_ms(book_ts, label="book.ts")
        if b_ms > 0 and t_ms > 0:
            l2_age_ms = int(t_ms - b_ms)
        else:
            l2_age_ms = 10**9
        return t_ms, l2_age_ms

    # -------------------- regime detection --------------------

    def _update_regime_on_bucket_close(self, ts: int, close_price: float) -> None:
        """
        Simplified candle update for regime tracking.
        """
        # Create a simplified candle object
        candle = type('Candle', (), {
            'ts': ts,
            'close': close_price,
            'vwap': getattr(self, "current_vwap", close_price),
            'daily_open': getattr(self, "daily_open", close_price),
        })()
        self._update_regime_window(candle)

    def _update_regime_window(self, candle) -> None:
        """
        candle: объект с полями close, ts, vwap (если есть), daily_open (если есть)
        Если нет vwap/daily_open на свечке — берём из state.
        """

        ts = getattr(candle, 'ts', get_ny_time_millis())
        close = float(getattr(candle, 'close', 0.0))

        # HTF-VWAP / дневной open ты где-то считаешь, возьми оттуда
        vwap = getattr(candle, "vwap", None) or getattr(self, "current_vwap", close)
        daily_open = getattr(candle, "daily_open", None) or getattr(self, "daily_open", close)

        # cumulative delta и OBI_windowLevels — через твои трекеры
        cum_delta = float(getattr(self, "cum_delta_1m", 0.0))
        obi_level = float(getattr(self, "obi_window_level", 0.0))  # твой OBI_windowLevels агрегат

        self._regime_window.append({
            "ts": ts,
            "close": close,
            "vwap": vwap,
            "daily_open": daily_open,
            "cum_delta": cum_delta,
            "obi_level": obi_level,
        })

    def _recompute_regime_if_needed(self) -> None:
        if len(self._regime_window) < 10:
            # мало данных
            return

        now_ts = self._regime_window[-1]["ts"]
        # можно пересчитывать не на каждом тике, а раз в N сек (например, раз в 60с)
        if now_ts - self.regime_state.last_update_ts < 60:
            return

        features = self._compute_regime_features()
        regime_state = self._classify_regime(features)
        regime_state.last_update_ts = now_ts
        self.regime_state = regime_state

    def _compute_regime_features(self) -> dict:
        window = list(self._regime_window)
        closes = [w["close"] for w in window]
        vwap_vals = [w["vwap"] for w in window]
        daily_open_vals = [w["daily_open"] for w in window]
        cum_deltas = [w["cum_delta"] for w in window]
        obi_levels = [w["obi_level"] for w in window]

        # 1) ATR-нормировка и квантиль
        atr_1m = float(getattr(self.atr_calculator, "value", lambda: 0.0)() or 0.0)
        # Для нормализации берём rolling-медиану за окно
        atr_history = [atr_1m]  # адаптируй под свой интерфейс
        atr_median = statistics.median(atr_history) if atr_history else max(atr_1m, 1e-8)
        atr_rel = atr_1m / max(atr_median, 1e-8)      # >1 => волатильность выше обычной

        # ATR quantile (0-1): где текущий ATR находится относительно исторического распределения
        # Простая оценка: нормируем относительно типичного диапазона
        if atr_1m <= 0.0001:  # очень низкая волатильность
            atr_q = 0.0
        elif atr_1m >= 0.01:   # очень высокая волатильность
            atr_q = 1.0
        else:
            # Предполагаем нормальное распределение ATR
            # Типичный ATR для крипты: 0.001-0.005
            atr_q = min(1.0, max(0.0, (atr_1m - 0.0005) / (0.005 - 0.0005)))

        # 2) Направленность cumulative delta
        # Считаем наклон линейной регрессии cum_delta по времени в окне (упрощённо).
        n = len(cum_deltas)
        if n > 1:
            x = list(range(n))
            x_mean = sum(x) / n
            y_mean = sum(cum_deltas) / n
            num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, cum_deltas))
            den = sum((xi - x_mean) ** 2 for xi in x) or 1.0
            delta_slope = num / den
        else:
            delta_slope = 0.0

        # Нормируем наклон в [-1;1] (грубая нормировка)
        delta_dir = max(-1.0, min(1.0, delta_slope / 1e6))

        # 3) Цена относительно VWAP и daily_open
        above_vwap = 0
        below_vwap = 0
        cross_vwap = 0

        prev_side = None
        for c, v in zip(closes, vwap_vals):
            side = 1 if c >= v else -1
            if side > 0:
                above_vwap += 1
            else:
                below_vwap += 1
            if prev_side is not None and side != prev_side:
                cross_vwap += 1
            prev_side = side

        total = max(1, len(closes))
        frac_above_vwap = above_vwap / total
        frac_below_vwap = below_vwap / total
        frac_side_persistence = max(frac_above_vwap, frac_below_vwap)
        cross_rate_vwap = cross_vwap / total  # 0 – редко пересекаем, 1 – постоянно

        # 4) OBI sign persistence
        pos_obi = sum(1 for o in obi_levels if o > 0)
        neg_obi = sum(1 for o in obi_levels if o < 0)
        if pos_obi + neg_obi > 0:
            obi_sign_persistence = abs(pos_obi - neg_obi) / (pos_obi + neg_obi)
            # 1 => один знак доминирует, 0 => пополам
        else:
            obi_sign_persistence = 0.0

        return {
            "atr_rel": atr_rel,
            "atr_q": atr_q,
            "delta_dir": delta_dir,
            "frac_side_persistence": frac_side_persistence,
            "cross_rate_vwap": cross_rate_vwap,
            "obi_sign_persistence": obi_sign_persistence,
        }

    def _classify_regime(self, feat: dict) -> RegimeState:
        # Для обратной совместимости, если передают feat dict
        if "atr_rel" in feat:
            return self._classify_regime_from_features(feat)

        # Новый метод для SignalContext - будет реализован ниже
        return RegimeState(label="unknown", trend_score=0.0, range_score=0.0)

    def _classify_regime_from_features(self, feat: dict) -> RegimeState:
        atr_rel = feat["atr_rel"]
        delta_dir = feat["delta_dir"]
        frac_side_persistence = feat["frac_side_persistence"]
        cross_rate_vwap = feat["cross_rate_vwap"]
        obi_sign_persistence = feat["obi_sign_persistence"]

        # clamp helper
        def clamp01(x: float) -> float:
            return max(0.0, min(1.0, x))

        # Нормируем atr_rel: >1 => трендовая волатильность
        atr_trend = clamp01(atr_rel - 1.0)     # 0 при atr_rel<=1, растёт до 1
        atr_range = clamp01(1.5 - atr_rel)     # 1 при очень низкой волатильности, падает при росте

        # Trend score: высокая ATR, направленный delta, устойчивое положение по одну сторону VWAP,
        # мало пересечений, устойчивый OBI
        trend_score_raw = (
            0.4 * atr_trend +
            0.3 * delta_dir +
            0.2 * (frac_side_persistence - 0.5) * 2.0 +   # 0 → 0, 1 → +1
            -0.2 * cross_rate_vwap +                      # много пересечений → минус
            0.2 * obi_sign_persistence
        )

        # Range score: низкая ATR, мало направленности delta, много пересечений VWAP,
        # низкая устойчивость OBI
        range_score_raw = (
            0.4 * atr_range +
            0.2 * (1.0 - abs(delta_dir)) +
            0.3 * cross_rate_vwap +
            0.1 * (1.0 - obi_sign_persistence)
        )

        # Приводим к [-1;+1] (грубая нормировка)
        def norm(x: float) -> float:
            return max(-1.0, min(1.0, x))

        trend_score = norm(trend_score_raw)
        range_score = norm(range_score_raw)

        # Решение
        # Если трендовый скор сильно выше — TREND, если ренджевый — RANGE, иначе MIXED
        diff = trend_score - range_score
        if diff > 0.3:
            label = MarketRegime.TREND
        elif diff < -0.3:
            label = MarketRegime.RANGE
        else:
            label = MarketRegime.MIXED

        # Конвертируем enum в строку для нового формата
        label_str = {
            MarketRegime.TREND: "trending",
            MarketRegime.RANGE: "ranging",
            MarketRegime.MIXED: "mixed",
            MarketRegime.UNKNOWN: "unknown"
        }.get(label, "unknown")

        return RegimeState(
            label=label_str,
            trend_score=trend_score,
            range_score=range_score,
            session_bias=0.0,  # будет установлено выше
            daily_open_cross_freq=0.0,  # будет установлено выше
            ts=time.time(),
            symbol="",  # будет установлено выше
        )

    def _detect_regime_from_context(self, ctx: OrderflowContext) -> MarketRegime:
        """
        Определение режима на основе SignalContext с daily_open
        """
        # Получаем конфиг (можно вынести в атрибут класса)
        cfg = {
            "atr_weight": 1.0,
            "delta_weight": 1.0,
            "vwap_weight": 1.0,
            "regime_trend_threshold": 0.35,
            "regime_range_threshold": -0.35,
            "daily_open_weight": 0.75,
            "daily_open_range_bps": 7.0,
            "daily_open_trend_bps": 25.0,
        }

        score = 0.0
        weight_sum = 0.0

        # 1) ATR-квантиль (предполагаем что есть в контексте)
        if hasattr(ctx, 'atr_q_14') and ctx.atr_q_14 is not None:
            atr_bias = 2.0 * ctx.atr_q_14 - 1.0  # [0..1] -> [-1..+1]
            score += cfg["atr_weight"] * atr_bias
            weight_sum += cfg["atr_weight"]

        # 2) Направленность cumulative delta
        if hasattr(ctx, 'cum_delta_slope') and ctx.cum_delta_slope is not None:
            norm = max(abs(ctx.cum_delta_slope), 1e-6)
            delta_dir = max(-1.0, min(1.0, ctx.cum_delta_slope / norm))
            score += cfg["delta_weight"] * delta_dir
            weight_sum += cfg["delta_weight"]

        # 3) Позиция относительно VWAP
        if ctx.vwap is not None and ctx.last_price is not None:
            rel = (ctx.last_price - ctx.vwap) / ctx.vwap
            vwap_dev_bps = abs(rel) * 10_000.0

            if vwap_dev_bps >= 20.0:
                vwap_bias = +1.0
            elif vwap_dev_bps <= 5.0:
                vwap_bias = -1.0
            else:
                frac = (vwap_dev_bps - 5.0) / max(20.0 - 5.0, 1e-6)
                vwap_bias = -1.0 + 2.0 * frac

            score += cfg["vwap_weight"] * vwap_bias
            weight_sum += cfg["vwap_weight"]

        # 4) Аналогично для daily_open
        if ctx.daily_open is not None and ctx.last_price is not None:
            if ctx.daily_open_dist_bps is not None:
                dist_bps = ctx.daily_open_dist_bps
            else:
                rel = abs(ctx.last_price - ctx.daily_open) / max(ctx.daily_open, 1e-6)
                dist_bps = rel * 10_000.0

            lo = cfg["daily_open_range_bps"]
            hi = cfg["daily_open_trend_bps"]

            if dist_bps <= lo:
                open_bias = -1.0     # range
            elif dist_bps >= hi:
                open_bias = +1.0     # trend
            else:
                frac = (dist_bps - lo) / max(hi - lo, 1e-6)
                open_bias = -1.0 + 2.0 * frac

            score += cfg["daily_open_weight"] * open_bias
            weight_sum += cfg["daily_open_weight"]

        # нормировка и выбор режима
        if weight_sum <= 0.0:
            return MarketRegime.MIXED

        regime_score = score / weight_sum  # [-1..+1]

        if regime_score >= cfg["regime_trend_threshold"]:
            return MarketRegime.TREND
        elif regime_score <= cfg["regime_range_threshold"]:
            return MarketRegime.RANGE
        return MarketRegime.MIXED

    # ---------- session regime ----------
    def _infer_session_label(self, ctx: OrderflowContext) -> str:
        """Грубая, но практичная реализация на UTC"""
        if ctx.ts_utc is None:
            return "other"

        h = time.gmtime(ctx.ts_utc).tm_hour

        # Базовая евродоллар/золото логика:
        # Азия — больше рендж, Лондон/NY — более трендовые
        if 0 <= h < 7:
            return "asia"
        if 7 <= h < 12:
            return "london"
        if 12 <= h < 21:
            return "ny"
        if 21 <= h < 24:
            return "late_us"
        return "other"

    def _session_bias(self, ctx: OrderflowContext) -> float | None:
        session = self._infer_session_label(ctx)
        ctx.session_label = session
        return self._cfg.session_bias_default.get(session, 0.0)

    # ---------- daily_open crossing frequency ----------
    def _compute_daily_open_cross_freq(self, symbol: str) -> float | None:
        cfg = self._cfg
        hist = self._regime_history.get(symbol)
        if not hist or len(hist) < 3:
            return None

        tail = list(hist)[-cfg.daily_open_cross_window:]
        if len(tail) < 3:
            return None

        crosses = 0
        pairs = 0

        prev = tail[0]
        for cur in tail[1:]:
            if prev.daily_open_side != 0 and cur.daily_open_side != 0:
                if prev.daily_open_side != cur.daily_open_side:
                    crosses += 1
                pairs += 1
            prev = cur

        if pairs == 0:
            return None
        return crosses / pairs  # [0..1]

    def _daily_open_cross_bias_from_freq(self, cross_freq: float) -> float:
        # clamp на всякий
        cf = max(0.0, min(1.0, cross_freq))
        return 1.0 - 2.0 * cf  # 0 -> +1, 1 -> -1

    # ---------- bar history and new extreme detection ----------
    def _update_bar_history(self, symbol: str, bar: BarSample) -> None:
        self._bar_history[symbol].append(bar)

    def _is_new_local_extreme(
        self,
        symbol: str,
        bar: BarSample,
        atr_intraday: float,
        k_atr: float = 0.25,     # 0.25 ATR сверху/снизу
        vol_z_thr: float = 1.5,  # z-score по объёму
    ) -> bool:
        hist = self._bar_history.get(symbol)
        if not hist or len(hist) < 20:
            return False

        highs = [b.high for b in hist]
        lows = [b.low for b in hist]
        vols = [b.volume for b in hist]

        prev_high = max(highs[:-1])
        prev_low = min(lows[:-1])

        mu_vol = sum(vols[:-1]) / (len(vols) - 1)
        var_vol = sum((v - mu_vol) ** 2 for v in vols[:-1]) / max(len(vols) - 2, 1)
        std_vol = math.sqrt(max(var_vol, 1e-9))
        vol_z = (bar.volume - mu_vol) / max(std_vol, 1e-6)

        is_new_high = (
            bar.high > prev_high + k_atr * atr_intraday and vol_z >= vol_z_thr
        )
        is_new_low = (
            bar.low < prev_low - k_atr * atr_intraday and vol_z >= vol_z_thr
        )

        return is_new_high or is_new_low

    def _update_market_regime(self, ctx: OrderflowContext) -> None:
        """
        Вызывается перед конф-скорером: считает фичи и кладёт в ctx.market_regime / market_regime_score.
        """
        symbol = ctx.symbol
        price = ctx.last_price
        if not symbol or price is None:
            return

        # ATR в б.п. от цены — для логов, но прямо в решении режима не используется, только как фича
        atr = getattr(ctx, "atr_14_1m", 0.0) or 0.0
        if atr > 0 and price > 0:
            atr_intraday_bps = abs(atr / price) * 10_000.0
        else:
            atr_intraday_bps = 0.0

        atr_q = getattr(ctx, "atr_quantile_1d", 0.5) or 0.5
        weak = getattr(ctx, "weakProgress", 0.0) or 0.0
        vwap_dist_bps = getattr(ctx, "vwap_distance_bps", 0.0) or 0.0
        vwap_trend_bps = getattr(ctx, "vwap_trend_bps", 0.0) or 0.0

        daily_open = getattr(ctx, "daily_open", None)
        if daily_open is None or daily_open <= 0:
            daily_open = price  # fallback, чтобы не делить на 0

        ts = getattr(ctx, "ts_utc", None) or time.time()

        state = self._regime_service.update(
            symbol=symbol,
            ts=ts,
            close_price=price,
            daily_open=daily_open,
            atr_intraday_bps=atr_intraday_bps,
            atr_quantile_1d=atr_q,
            weak_progress=weak,
            vwap_distance_bps=vwap_dist_bps,
            vwap_trend_bps=vwap_trend_bps,
            session=ctx.session,
        )

        # Конвертируем новый формат обратно в старый для совместимости
        if state.label == "trending":
            ctx.market_regime = MarketRegime.TREND
            ctx.market_regime_score = state.trend_score
        elif state.label == "ranging":
            ctx.market_regime = MarketRegime.RANGE
            ctx.market_regime_score = -state.range_score
        else:
            ctx.market_regime = MarketRegime.MIXED
            ctx.market_regime_score = state.trend_score - state.range_score

    def _apply_golden_logic(self, ctx: OrderflowContext) -> None:
        """
        1) Определяем порог для этого паттерна (из ENV или дефолтный).
        2) Ставим флаги is_golden_pattern / golden_pattern_label.
        3) Подтягиваем вес паттерна (для последующей агрегации, если нужно).
        """
        from core.config import (
            get_pattern_conf_threshold,
            get_pattern_weight,
        )

        label = getattr(ctx, "pattern_label", None) or getattr(ctx, "golden_pattern_label", None)
        threshold = get_pattern_conf_threshold(label)

        confidence = getattr(ctx, "confidence", 0.0)
        ctx.is_golden_pattern = confidence >= threshold
        ctx.golden_pattern_label = label if ctx.is_golden_pattern else None

        ctx.pattern_weight = get_pattern_weight(label)

    def _apply_scoring(self, ctx: OrderflowContext) -> None:
        """
        final_score = (confidence_scaled) * pattern_weight * golden_mult
        всё ограничиваем FINAL_SCORE_MAX.
        """
        from core.config import (
            CONFIDENCE_SCALE,
            FINAL_SCORE_MAX,
            GOLDEN_SCORE_MULTIPLIER,
        )

        # 1) нормализуем confidence
        base_score = ctx.confidence * CONFIDENCE_SCALE  # 80 → 0.8 при 0.01
        # можно ещё зажать: base_score = min(max(base_score, 0.0), 1.0)

        # 2) умножаем на вес паттерна
        score = base_score * ctx.pattern_weight

        # 3) бустим golden
        if ctx.is_golden_pattern:
            score *= GOLDEN_SCORE_MULTIPLIER

        # 4) защита от разлёта
        score = min(score, FINAL_SCORE_MAX)

        ctx.base_score = base_score
        ctx.final_score = score



    def _create_execution_plan(self, ctx: OrderflowContext) -> ExecutionPlan | None:
        """
        Создает план исполнения для сигнала.
        Использует ExecutionPlanner из signal_execution модуля.
        """
        try:
            from signal_exec import ExecutionPlanner
            from signal_execution import ExtendedSignalContext, SymbolSetupConfig

            # Конвертируем SignalContext в ExtendedSignalContext
            extended_ctx = ExtendedSignalContext(
                signal_id=getattr(ctx, 'signal_id', str(uuid.uuid4())),
                symbol=ctx.symbol,
                side=getattr(ctx, 'side', 'long'),  # Используем enum Side
                setup_type=getattr(ctx, 'pattern_name', 'unknown'),
                ts_signal=getattr(ctx, 'ts_utc', ctx.ts),
                price_at_signal=getattr(ctx, 'last_price', 0.0),
                atr_1m=getattr(ctx, 'atr_14_1m', 0.0),
                atr_5m=getattr(ctx, 'atr_14_1m', 0.0),  # TODO: добавить расчет ATR_5m
                final_score=getattr(ctx, 'confidence', 0.0),
                # Расширенные поля - заполняем из ctx где возможно
                tick_size=getattr(ctx, 'tick_size', 0.01),
                contract_size=getattr(ctx, 'contract_size', 1.0),
                # TODO: добавить account_state из состояния счета
                # TODO: добавить swing points из микроструктуры
                # TODO: добавить HTF levels из анализа
            )

            # Создаем базовые конфиги по символам/сетапам
            # В проде это можно загружать из базы данных
            setup_configs = {
                ("breakout_R1"): SymbolSetupConfig(
                    symbol="",
                    setup_type="breakout_R1",
                    expiry_bars=5,
                    score_buckets=(0.4, 0.7, 0.85),
                    risk_multipliers=(0.5, 1.0, 1.5, 2.0),
                ),
                "fade_PDH": SymbolSetupConfig(
                    symbol="",
                    setup_type="fade_PDH",
                    expiry_bars=3,
                    score_buckets=(0.4, 0.7, 0.85),
                    risk_multipliers=(0.5, 1.0, 1.5, 2.0),
                ),
                ("BTCUSDT", "breakout_R1"): SymbolSetupConfig(
                    symbol="BTCUSDT",
                    setup_type="breakout_R1",
                    expiry_bars=4,
                    score_buckets=(0.4, 0.7, 0.85),
                    risk_multipliers=(0.5, 1.0, 1.5, 2.0),
                ),
                # Дефолтные конфиги для неизвестных комбинаций
                ("default", "default"): SymbolSetupConfig(
                    symbol="default",
                    setup_type="default",
                    expiry_bars=3,
                ),
            }

            planner = ExecutionPlanner(setup_configs)
            plan = planner.build_plan(extended_ctx)

            return plan

        except Exception:
            self.logger.exception("Failed to create execution plan")
            return None

    def _save_execution_plan(self, plan: ExecutionPlan) -> None:
        """
        Сохраняет план исполнения в базу данных.
        """
        try:
            self._execution_repo.insert_execution_plan(plan)
        except Exception:
            self.logger.exception("Failed to save execution plan")

    def _publish_entry_candidate(self, side: str, signal_type: str, signal_settings: dict) -> None:
        """Публикует entry_candidate в stream:trade:entry_candidate для SMT entry policy."""
        try:
            import json

            from utils.time_utils import get_ny_time_millis
            _ts_now = get_ny_time_millis()
            entry_payload = {
                "schema_version": 1,
                "type": "entry_candidate",
                "ts_ms": _ts_now,
                "setup_ts_ms": _ts_now,
                "symbol": self.symbol,
                "side": side,
                "bundle": signal_settings.get("bundle_id", ""),
                "kind": signal_type,
                "leader": self._get_strategy_key(),
                "ab_arm": signal_settings.get("ab_arm", "A"),
                "ab_group": signal_settings.get("ab_group", "default"),
                "regime": signal_settings.get("regime", "na"),
            }
            self.redis.xadd(
                "stream:trade:entry_candidate",
                {"payload": json.dumps(entry_payload, ensure_ascii=False)},
                maxlen=20000,
                approximate=True
            )
        except Exception as e:
            self.logger.exception(f"Failed to publish signal to entry_candidate: {e}")

    def _execution_plan_to_dict(self, plan: ExecutionPlan | None) -> dict | None:
        """
        Конвертирует ExecutionPlan в словарь для payload.
        """
        if plan is None:
            return None

        return {
            "signalId": plan.signal_id,
            "symbol": plan.symbol,
            "side": plan.side,
            "entryZoneLow": plan.entry_zone_low,
            "entryZoneHigh": plan.entry_zone_high,
            "stopPrice": plan.stop_price,
            "tpLevels": plan.tp_levels,
            "partials": plan.partials,
            "posRiskR": plan.pos_risk_R,
            "riskUsd": plan.risk_usd,
            "positionSize": plan.position_size,
            "expiryBars": plan.expiry_bars,
            "createdAt": plan.created_at.isoformat() if plan.created_at else None,
        }

    def _should_emit(self, ctx: OrderflowContext) -> bool:
        """
        DEPRECATED: Legacy method for backward compatibility.
        Now uses UnifiedSignalPipeline to determine if signal should be emitted.
        """
        if self._unified_pipeline is None:
            # Fallback to basic confidence check
            min_conf = self._get_min_confidence_for_symbol(ctx.symbol)
            return getattr(ctx, 'confidence', 0.0) >= min_conf

        try:
            # Use unified pipeline to build signal context and check emission
            sig_ctx = self._unified_pipeline.build_ctx(ctx)
            self._unified_pipeline.attach_regime(sig_ctx)
            self._unified_pipeline.apply_scoring(sig_ctx)
            self._unified_pipeline.apply_golden_logic(sig_ctx)
            return self._unified_pipeline.should_emit(sig_ctx)

        except Exception:
            self.logger.exception("Failed to check should_emit via UnifiedSignalPipeline, falling back to basic check")
            # Fallback: проверяем только confidence
            min_conf = self._get_min_confidence_for_symbol(ctx.symbol)
            return getattr(ctx, 'confidence', 0.0) >= min_conf

    def _update_geometry_liquidity_context(self, ctx: OrderflowContext, price: float, ts: float) -> None:
        """
        Обновление геометрии и ликвидности перед скорингом.
        Дефолтная реализация: просто дергаем attach_хукы.
        Наследники могут переопределить, если нужен более сложный сценарий.
        """
        # Create a simple BarSample from current price data
        bar = BarSample(
            ts=ts,
            high=price,
            low=price,
            volume=0.0  # Not used in geometry calculation
        )
        self._attach_geometry_context(ctx, bar)
        self._attach_liquidity_context(ctx)

    # ---------- geometry scoring ----------
    def _score_geometry(self, ctx: OrderflowContext) -> float:
        hits = ctx.geo_zone_hits or []
        if not hits and not ctx.is_new_local_extreme:
            return 0.0

        base_scores: list[float] = []

        for h in hits:
            nr = 0.25  # near_mult
            fr = 1.0   # far_mult
            d = h.dist_rel_atr

            if d <= nr:
                proximity = 1.0
            elif d >= fr:
                proximity = 0.0
            else:
                proximity = max(0.0, 1.0 - (d - nr) / max(fr - nr, 1e-6))

            # усиливаем PDH/PDL/weekly/OB против mid/session
            base_scores.append(proximity * h.strength)

        geom = max(base_scores) if base_scores else 0.0

        # новый экстремум + спайк по range/volume — усиливаем
        if ctx.is_new_local_extreme:
            geom += 0.3  # new_extreme_bonus

        geom = max(0.0, min(1.0, geom))  # clamp to [0,1]
        return geom

    # ---------- liquidity analysis ----------
    def _find_near_liquidity_wall(
        self,
        ctx: OrderflowContext,
        l2: SimpleL2Snapshot,
        max_levels: int = 10,
        max_dist_bps: float = 15.0,
        size_z_thr: float = 1.5,
    ) -> tuple[str | None, L2Level | None, float | None]:
        price = ctx.last_price
        if price is None:
            return None, None, None

        # соберём размеры для оценки "нормального" уровня
        sizes = [lvl.size for lvl in l2.bids[:max_levels]] + [lvl.size for lvl in l2.asks[:max_levels]]
        if not sizes:
            return None, None, None

        mu = sum(sizes) / len(sizes)
        var = sum((s - mu) ** 2 for s in sizes) / max(len(sizes) - 1, 1)
        std = math.sqrt(max(var, 1e-9))

        best_side = None
        best_level = None
        best_size_z = None

        def process_side(levels: list[L2Level], side: str):
            nonlocal best_side, best_level, best_size_z
            for lvl in levels[:max_levels]:
                dist_rel = abs(lvl.price - price) / max(price, 1e-6)
                dist_bps = dist_rel * 10_000.0
                if dist_bps > max_dist_bps:
                    continue

                size_z = (lvl.size - mu) / max(std, 1e-6)
                if size_z < size_z_thr:
                    continue

                if best_level is None or size_z > (best_size_z or -1e9):
                    best_side = side
                    best_level = lvl
                    best_size_z = size_z

        process_side(l2.bids, "bid")
        process_side(l2.asks, "ask")

        return best_side, best_level, best_size_z

    def _build_liquidity_context(
        self,
        ctx: OrderflowContext,
        l2: SimpleL2Snapshot,
        cluster: ClusterVol,
    ) -> LiquidityContext:
        lc = LiquidityContext()
        side, lvl, size_z = self._find_near_liquidity_wall(ctx, l2)
        if side is None or lvl is None or size_z is None:
            return lc

        lc.near_wall_side = side
        lc.near_wall_price = lvl.price
        lc.near_wall_size = lvl.size
        lc.near_wall_size_z = size_z

        # глубина в 5 уровнях (можно хранить медианы по инструменту и считать z-score)
        depth_5 = sum(x.size for x in (l2.bids[:5] if side == "bid" else l2.asks[:5]))
        lc.depth_5_vol = depth_5
        # std/median по depth_5 заранее считаются в статистическом сервисе
        # здесь можно просто stub: depth_5_z = depth_5 / avg_depth_5
        # lc.depth_5_z = ...

        # агрессивный объём по цене стенки (плюс/минус 1 тик)
        price = lvl.price
        tick = getattr(ctx, "tick_size", None) or 0.01

        def agg_at(p: float) -> tuple[float, float]:
            buys = 0.0
            sells = 0.0
            for k, v in cluster.buy_vol_by_price.items():
                if abs(k - p) <= tick:
                    buys += v
            for k, v in cluster.sell_vol_by_price.items():
                if abs(k - p) <= tick:
                    sells += v
            return buys, sells

        buys, sells = agg_at(price)
        if side == "ask":
            aggr = buys
        else:
            aggr = sells

        lc.aggr_vol_at_wall = aggr
        lc.aggr_to_rest_ratio = aggr / max(lvl.size, 1e-6)

        # классификация: break vs absorption
        # very rough:
        if lc.aggr_to_rest_ratio > 1.0:
            lc.pattern = "break"
        elif lc.aggr_to_rest_ratio > 0.2:
            lc.pattern = "absorption"
        else:
            lc.pattern = "none"

        lc.liquidity_context_score = self._score_liquidity(lc)
        return lc

    def _score_liquidity(self, lc: LiquidityContext) -> float:
        base_no_wall_score = 0.1
        max_score = 1.0

        if lc.near_wall_side is None:
            return base_no_wall_score

        # приводим к [0..1]
        wall_score = 0.0
        if lc.near_wall_size_z is not None:
            wall_score = max(0.0, min(1.0, (lc.near_wall_size_z - 1.0) / 3.0))  # z 1..4 → 0..1

        depth_score = 0.0
        if lc.depth_5_z is not None:
            depth_score = max(0.0, min(1.0, (lc.depth_5_z - 1.0) / 3.0))

        aggr_score = 0.0
        if lc.aggr_to_rest_ratio is not None:
            r = lc.aggr_to_rest_ratio
            if r <= 0.1:
                aggr_score = 0.0
            elif r >= 1.0:
                aggr_score = 1.0
            else:
                aggr_score = (r - 0.1) / 0.9

        score = (
            0.4 * wall_score +      # wall_size_weight
            0.2 * depth_score +     # depth_weight
            0.4 * aggr_score        # aggr_ratio_weight
        )

        score = max(0.0, min(max_score, score))
        return score

    # ---------- context attachment methods ----------
    def _attach_geometry_context(self, ctx: OrderflowContext, bar: BarSample) -> None:
        """
        Наполнить контекст геометрией: HTF уровни, попадания в зоны, локальные экстремумы.
        """
        htf = self._get_htf_levels(ctx.symbol)
        if htf is None:
            ctx.geo_zone_hits = []
            ctx.is_new_local_extreme = False
            ctx.geometry_score = 0.0
            return

        # GeoZoneHit[] из HTF уровней + текущей цены
        geo_hits = self._build_geo_zone_hits(ctx, htf)
        geometry_score = self._score_geometry(ctx)

        # Check for local extreme (simplified - use current ATR)
        atr_val = getattr(ctx, 'atr_q_14', 1.0) or 1.0
        is_new_extreme = self._is_new_local_extreme(ctx.symbol, bar, atr_val)

        ctx.geo_zone_hits = geo_hits
        ctx.geometry_score = geometry_score
        ctx.is_new_local_extreme = is_new_extreme

    def _attach_liquidity_context(
        self,
        ctx: OrderflowContext,
        l2: SimpleL2Snapshot | None = None,
        cluster: ClusterVol | None = None,
    ) -> None:
        """Attach liquidity context. Default implementation does nothing."""
        pass

    # ---------- local calibration ----------

    def _apply_metric_calibration(
        self,
        ctx: SignalContext,
        metric_name: str,
        *,
        default_extreme_z: float = 2.0,
    ) -> None:
        """
        Calibrate a single metric using local calibration data.
        """
        if self.local_calibration is None:
            return

        raw_value = ctx.metrics.get(metric_name)
        if raw_value is None:
            return

        # Use LCStoreV2 interface
        cfg = self.local_calibration.get_metric_cfg(
            ctx.symbol, ctx.session or "mixed", ctx.regime_label or "mixed", metric_name
        )
        if cfg:
            quantile = eval_local_quantile(cfg.cdf_points, raw_value)
            is_extreme = abs(raw_value) >= cfg.threshold
        else:
            quantile = 0.5  # neutral
            is_extreme = abs(raw_value) >= default_extreme_z
            cfg = None

        ctx.calibrated[metric_name] = {
            "value": raw_value,
            "is_extreme": is_extreme,
            "threshold": cfg.threshold if cfg else default_extreme_z,
            "quantile": quantile,
            "p50": cfg.q90 if cfg else None,  # Using q90 as approximation for p50
            "p75": cfg.q95 if cfg else None,  # Using q95 as approximation for p75
            "p90": cfg.q98 if cfg else None,  # Using q98 as approximation for p90
        }

    def _apply_local_calibration(self, ctx: SignalContext) -> None:
        """
        Apply local calibration to key metrics.
        """
        if self.local_calibration is None:
            return

        # List of metrics to calibrate
        metrics_to_calibrate = [
            "deltaSpike_z",
            "obi",
            "absorption_score",
            "liquidity_score",
            "weak_progress",
            "atr_quantile",
        ]

        for metric_name in metrics_to_calibrate:
            self._apply_metric_calibration(ctx, metric_name)

    # ---------- unified signal generation ----------

    def _build_signal_context(self, bar: BarSample) -> SignalContext:
        """
        Build unified SignalContext from bar data.
        This is a base implementation - subclasses should override to add specific metrics.
        """
        # This is a placeholder - real implementation should be in subclasses
        # that have access to orderflow-specific data
        return SignalContext(
            symbol=self.symbol,
            side="long",  # placeholder
            ts_ms=int(bar.ts * 1000),
            ts=None,  # TODO: convert from bar.ts
        )

    def _generate_signals_unified(self, bar: BarSample) -> list[Signal]:
        """
        DEPRECATED: Legacy adapter for unified signal generation.
        Now converts BarSample to OrderflowContext and delegates to UnifiedSignalPipeline.
        """
        if self._unified_pipeline is None:
            self.logger.warning("No unified pipeline configured, skipping signal generation")
            return []

        # Convert BarSample to OrderflowContext (simplified adapter)
        of_ctx = self._build_orderflow_context_from_bar(bar)

        # Delegate to unified pipeline
        signal = self._unified_pipeline.process(of_ctx)

        return [signal] if signal is not None else []

    def _build_orderflow_context_from_bar(self, bar: BarSample) -> OrderflowContext:
        """
        Adapter: Convert BarSample to OrderflowContext for unified pipeline.
        This is a simplified conversion - in full implementation,
        more fields should be populated from current state.
        """
        # Use current state to build a minimal OrderflowContext
        # In practice, this should aggregate data from the current processing context

        # Compute ATR and preserve its candle timestamp (if Redis-backed).
        # This is critical for hard staleness veto gates (DATA_ATR_STALE_MAX_MS).
        # _get_atr() stays backward-compatible (returns float), but updates:
        #   self._last_atr_ts_ms
        _atr_val = self._get_atr(bar.close, bar.close_ts_ms)
        _atr_ts_ms = getattr(self, "_last_atr_ts_ms", None)

        of = OrderflowContext(
            ts=int(bar.close_ts_ms),
            price=bar.close,
            symbol=self.symbol,
            family=self.family,
            venue=self.venue,
            timeframe=self.timeframe,
            z_delta=getattr(self, '_last_z_delta', 0.0),
            weak_progress=getattr(self, '_last_weak_progress', False),
            obi=getattr(self, '_last_obi', 0.0),
            obi_avg=getattr(self, '_last_obi_avg', 0.0),
            obi_sustained=getattr(self, '_last_obi_sustained', False),
            atr=_atr_val,
            atr_ts_ms=_atr_ts_ms,
            pivots=self.daily_pivots,
            current_delta=getattr(self, '_last_bucket_value', 0.0),
            delta_bucket=getattr(self, '_last_bucket_value', 0.0),
            regime=self.regime_state.label,
            regime_trend_score=self.regime_state.trend_score,
            regime_range_score=self.regime_state.range_score,
            last_price=bar.close,
            # Add other fields as needed for compatibility
        )
        # Fail-open propagation if OrderflowContext doesn't yet declare atr_ts_ms
        with contextlib.suppress(Exception):
            of.atr_ts_ms = _atr_ts_ms
        return of

    def _should_emit_signal(self, ctx: SignalContext) -> bool:
        """
        DEPRECATED: Legacy method for backward compatibility.
        Now delegates to UnifiedSignalPipeline.should_emit().
        """
        if self._unified_pipeline is None:
            return True  # Fallback

        return self._unified_pipeline.should_emit(ctx)

    def _build_signal_from_context(self, ctx: SignalContext, bar: BarSample) -> Signal:
        """
        Build Signal object from unified context.
        Base implementation - subclasses should override.
        """
        # Placeholder - real implementation should create proper Signal
        raise NotImplementedError("Subclasses must implement _build_signal_from_context")

    # -------------------- stream run loop --------------------

    def _run_loop(self) -> None:
        consumer_name = f"{self.consumer_name_prefix}-{os.getpid()}-{int(time.time())}"  # type: ignore[name-defined]
        consumer = SyncRedisStreamHelper(self.redis_ticks, self.group, consumer_name)

        backoff_new = Backoff(
            base_delay=float(os.getenv("REDIS_BACKOFF_BASE", "0.25")),
            multiplier=float(os.getenv("REDIS_BACKOFF_FACTOR", "2.0")),
            max_delay=float(os.getenv("REDIS_BACKOFF_CAP", "5.0")),
            jitter=bool(int(os.getenv("REDIS_BACKOFF_JITTER_ENABLED", "1"))),
        )
        backoff_pending = Backoff(
            base_delay=float(os.getenv("REDIS_BACKOFF_BASE_PENDING", os.getenv("REDIS_BACKOFF_BASE", "0.25"))),
            multiplier=float(os.getenv("REDIS_BACKOFF_FACTOR_PENDING", os.getenv("REDIS_BACKOFF_FACTOR", "2.0"))),
            max_delay=float(os.getenv("REDIS_BACKOFF_CAP_PENDING", os.getenv("REDIS_BACKOFF_CAP", "5.0"))),
            jitter=bool(int(os.getenv("REDIS_BACKOFF_JITTER_ENABLED", "1"))),
        )
        idle_sleep = float(os.getenv("REDIS_IDLE_SLEEP_SEC", "0.05"))

        streams = [self.tick_stream, self.book_stream, self.l3_stream]
        consumer.ensure_groups(streams)

        fail_counts: dict[tuple[str, str], int] = {}
        next_claim_at = 0
        claim_start_ids = {self.tick_stream: "0-0", self.book_stream: "0-0", self.l3_stream: "0-0"}

        self.logger.info("Run loop started (consumer=%s group=%s)", consumer_name, self.group)

        # First: recover pending
        self._claim_and_process_pending(consumer, streams, claim_start_ids, fail_counts, backoff_pending)

        # Stats
        last_stat = time.time()
        tick_cnt = 0
        book_cnt = 0
        prev_total_published = self.published_signals

        while self.is_running:
            now_ms = get_ny_time_millis()

            if now_ms >= next_claim_at:
                self._claim_and_process_pending(consumer, streams, claim_start_ids, fail_counts, backoff_pending)
                next_claim_at = now_ms + self.claim_interval_ms

            try:
                msgs = consumer.read_new(streams, count=self.config.read_count, block_ms=self.config.read_block_ms)
            except Exception as e:
                if self._is_transient_error(e):
                    delay = backoff_new.next_sleep()
                    self.logger.warning("Transient error on read_new: %s (backoff=%.2fs)", e, delay)
                    sleep_s(delay)
                    continue
                raise

            if not msgs:
                backoff_new.reset()
                if idle_sleep > 0:
                    sleep_s(idle_sleep)
                continue

            batch_had_transient = False
            acked_in_batch = 0

            for m in msgs:
                ok = False
                key = (m.stream, m.msg_id)

                try:
                    if m.stream == self.tick_stream:
                        tick = self._parse_tick(m.fields)
                        if tick:
                            self._process_tick(tick)
                            tick_cnt += 1
                        ok = True
                    elif m.stream == self.book_stream:
                        book = self._parse_book(m.fields)
                        if book:
                            self._process_book(book)
                            book_cnt += 1
                        ok = True
                    elif m.stream == self.l3_stream:
                        l3_event = self._parse_l3_event(m.fields)
                        if l3_event:
                            self._process_l3_event(l3_event)
                        ok = True
                    else:
                        ok = True

                except Exception as e:
                    if self._is_transient_error(e):
                        # no ACK, no DLQ, no retries counter increment
                        delay = backoff_new.next_sleep()
                        self.logger.warning("Transient error (no ACK, will retry via pending): %s (backoff=%.2fs)", e, delay)
                        ok = False
                        sleep_s(delay)
                        batch_had_transient = True
                        break
                    else:
                        fail_counts[key] = fail_counts.get(key, 0) + 1
                        self.logger.warning(
                            "Process failed %s %s (try=%d): %s",
                            m.stream, m.msg_id, fail_counts[key], e
                        )

                        if fail_counts[key] >= self.max_fail_retries:
                            dlq_payload = {
                                    "ts": get_ny_time_millis(),
                                    "symbol": self.symbol,
                                    "handler": self.__class__.__name__,
                                    "stream": m.stream,
                                    "msg_id": m.msg_id,
                                    "fields": {k: _to_str(v) for k, v in (m.fields or {}).items()},
                                    "error": str(e),
                            }
                            ok = self._try_add_dlq_or_backoff(
                                consumer,
                                dlq_payload,
                                backoff=backoff_new,
                                where="new",
                            )

                if ok:
                    fail_counts.pop(key, None)
                    try:
                        consumer.ack(m.stream, m.msg_id)
                        acked_in_batch += 1
                    except Exception as ack_e:
                        self.logger.error("ACK failed %s %s: %s", m.stream, m.msg_id, ack_e)

                # prevent unbounded growth
                if len(fail_counts) > 20000:
                    fail_counts.clear()

            if (not batch_had_transient) and acked_in_batch > 0:
                backoff_new.reset()
            if time.time() - last_stat >= 60:
                sig_60 = self.published_signals - prev_total_published
                prev_total_published = self.published_signals

                self.logger.info(
                    "60s stats | ticks=%d books=%d signals=%d (total=%d)",
                    tick_cnt, book_cnt, sig_60, self.published_signals
                )
                tick_cnt = 0
                book_cnt = 0
                last_stat = time.time()

        # --- metrics sink (best-effort)
        self._local_metrics: dict[str, int] = {}

        def _inc_metric(name: str, delta: int = 1) -> None:
            # HealthMetrics-style
            hm = getattr(self, "health_metrics", None) or getattr(self, "_health_metrics", None)
            if hm is not None:
                for meth in ("inc", "incr", "inc_counter", "counter_inc", "add"):
                    fn = getattr(hm, meth, None)
                    if fn is None:
                        continue
                    try:
                        fn(name, delta)
                        return
                    except Exception:
                        pass
            # fallback local counters
            self._local_metrics[name] = int(self._local_metrics.get(name, 0)) + int(delta)

        # --- time policy: tie "past" guard to max_tick_lag_ms if present (stronger default)
        max_past = int(getattr(self, "max_tick_lag_ms", 0) or 0)
        pol = TickTimePolicy(
            max_past_ms=max(max_past, int(os.getenv("TICK_MAX_PAST_MS", "5000"))),
            max_future_ms=int(os.getenv("TICK_MAX_FUTURE_MS", "500")),
            max_reorder_ms=int(os.getenv("TICK_MAX_REORDER_MS", "1500")),
            seconds_threshold=int(float(os.getenv("TICK_SECONDS_THRESHOLD", "1e12"))),
        )
        self._tick_time = TickTimeGuard(pol, inc=_inc_metric, now_provider=self._steady_clock.now_ms)

        # allow runtime toggles
        self._drop_bad_time = os.getenv("DROP_BAD_TICK_TIME", "1").lower() not in {"0", "false", "no"}

    def _claim_and_process_pending(
        self,
        consumer: SyncRedisStreamHelper,
        streams: list[str],
        start_ids: dict[str, str],
        fail_counts: dict[tuple[str, str], int],
        backoff: Backoff,
    ) -> bool:
        any_msgs = False
        all_ok = True
        for s in streams:
            start_id = start_ids.get(s, "0-0")
            try:
                next_id, msgs = consumer.claim_pending(
                    s,
                    min_idle_ms=self.claim_min_idle_ms,
                    start_id=start_id,
                    count=self.claim_count,
                )
                start_ids[s] = next_id
            except Exception as e:
                if self._is_transient_error(e):
                    delay = backoff.next_sleep()
                    self.logger.warning("Transient error on pending claim: %s (backoff=%.2fs)", e, delay)
                    sleep_s(delay)
                    return False
                raise

            if not msgs:
                continue
            any_msgs = True

            for m in msgs:
                ok = False
                key = (m.stream, m.msg_id)
                try:
                    if m.stream == self.tick_stream:
                        tick = self._parse_tick(m.fields)
                        if tick:
                            self._process_tick(tick)
                        ok = True
                    elif m.stream == self.book_stream:
                        book = self._parse_book(m.fields)
                        if book:
                            self._process_book(book)
                        ok = True
                    else:
                        ok = True

                except Exception as e:
                    if self._is_transient_error(e):
                        delay = backoff.next_sleep()
                        self.logger.warning("Transient error on pending (no ACK, retry later): %s (backoff=%.2fs)", e, delay)
                        sleep_s(delay)
                        all_ok = False
                        return False
                    else:
                        fail_counts[key] = fail_counts.get(key, 0) + 1
                        if fail_counts[key] >= self.max_fail_retries:
                            dlq_payload = {
                                    "ts": get_ny_time_millis(),
                                    "symbol": self.symbol,
                                    "handler": self.__class__.__name__,
                                    "stream": m.stream,
                                    "msg_id": m.msg_id,
                                    "fields": {k: _to_str(v) for k, v in (m.fields or {}).items()},
                                    "error": str(e),
                                    "from_pending": True,
                            }
                            ok = self._try_add_dlq_or_backoff(
                                consumer,
                                dlq_payload,
                                backoff=backoff,
                                where="pending",
                            )
                            if not ok:
                                all_ok = False
                                return False
                        else:
                            all_ok = False

                if ok:
                    fail_counts.pop(key, None)
                    with contextlib.suppress(Exception):
                        consumer.ack(m.stream, m.msg_id)
                else:
                    all_ok = False

                if len(fail_counts) > 20000:
                    fail_counts.clear()

        if (not any_msgs) or all_ok:
            backoff.reset()
        return all_ok

    # -------------------- parsing --------------------

    def _parse_tick(self, fields: dict[str, Any]) -> Tick | None:
        if not fields:
            return None

        is_buyer_maker: bool | None = None
        tick_json = None

        if "data" in fields:
            raw_s = _to_str(fields.get("data"))
            try:
                tick_json = json.loads(raw_s) if raw_s else {}
            except Exception:
                return None
            try:
                ts_raw = tick_json.get("ts", 0)
                ts = normalize_epoch_ms(ts_raw)
                bid = float(tick_json.get("bid", 0))
                ask = float(tick_json.get("ask", 0))
                last = float(tick_json.get("last", 0))
                volume = float(tick_json.get("volume", 0))
                flags = int(tick_json.get("flags", 0))
            except Exception:
                return None
        else:
            try:
                ts = normalize_epoch_ms(fields.get("ts", 0))
                bid = float(fields.get("bid", 0))
                ask = float(fields.get("ask", 0))
                last = float(fields.get("last", 0))
                volume = float(fields.get("volume", 0))
                flags = int(fields.get("flags", 0))
            except Exception:
                return None

        if ts is None or ts <= 0 or bid <= 0 or ask <= 0:
            return None

        # Extract is_buyer_maker (fast path: flat field in stream)
        ibm = fields.get("is_buyer_maker")
        if ibm is not None:
            is_buyer_maker = _parse_bool(ibm)
        # fallback: from parsed JSON data
        elif tick_json and isinstance(tick_json, dict) and "is_buyer_maker" in tick_json:
            is_buyer_maker = _parse_bool(tick_json.get("is_buyer_maker"))

        return Tick(
            ts=int(ts),
            bid=bid,
            ask=ask,
            last=last,
            volume=volume,
            flags=flags,
            is_buyer_maker=is_buyer_maker
        )

    def _parse_book(self, fields: dict[str, Any]) -> dict[str, Any] | None:
        if not fields:
            return None
        raw = fields.get("data") or fields.get("payload")
        if raw is None:
            return None
        raw_s = _to_str(raw)
        try:
            return json.loads(raw_s) if isinstance(raw_s, str) else None
        except Exception:
            return None

    # -------------------- L3-lite trade hook --------------------


    # -------------------- core processing --------------------

    def _l3_on_tick_trade(self, tick: Tick, signed_delta: float) -> None:
        """
        Hook: обновление L3LiteTracker на trade-тиках.
        По умолчанию no-op, crypto-обработчик переопределяет.
        """
        return

    def _is_trade_tick(self, tick: Tick) -> bool:
        """
        Единая эвристика "это сделка", чтобы не плодить разные условия.
        """
        try:
            return bool(tick.flags & 1) or bool(tick.volume and float(tick.volume) > 0)
        except Exception:
            return bool(tick.flags & 1)

    def _before_signal_generation(self, ctx: Any) -> None:
        """
        Хук финализации контекста перед генерацией сигналов.
        Fail-open: ошибки enrichment не должны ломать генерацию/пайплайн.
        """
        try:
            # CryptoOrderFlowHandler может переопределять это и заполнять:
            # ctx.geo_zone_hits/ctx.geo_zone_hit/ctx.geometry_score/ctx.liquidity_ctx и т.д.
            self._update_geometry_liquidity_context(ctx)
        except Exception:
            # Логируем, но не ломаем обработку бакета
            with contextlib.suppress(Exception):
                self.logger.exception("update_geometry_liquidity_context failed (fail-open)")

    def _track_dependency_metrics(self, ctx: Any) -> None:
        """
        Лёгкие метрики качества данных (без внешних зависимостей).
        Сейчас: l3_missing_rate.
        """
        try:
            flags = getattr(ctx, "data_quality_flags", []) or []
            total = int(getattr(self, "_dq_l3_total", 0) or 0) + 1
            missing = int(getattr(self, "_dq_l3_missing", 0) or 0) + (1 if ("l3_missing" in flags) else 0)
            self._dq_l3_total = total
            self._dq_l3_missing = missing
            # сохраняем в ctx для аудит/логов/эмиттера
            ctx.l3_missing_rate = float(missing) / float(max(1, total))
        except Exception:
            # метрики не должны ломать обработку
            pass

    def _run_signal_generation(self, ctx: Any) -> None:
        """
        Единая точка генерации сигналов: unified pipeline или legacy fallback.
        Выделено в отдельный метод для поведенческих тестов.
        """
        # Signal generation: unified pipeline approach
        # If quarantined due to broken tick time: FAIL-CLOSED for signal emission
        # (state updates still happen above; we just skip emission).
        now_ms2 = int(self._steady_clock.now_ms())
        if self._bad_time_quarantine.is_quarantined(now_ms2):
            with contextlib.suppress(Exception):
                self._inc_metric("signals.skipped.bad_time_quarantine", 1)
        else:
            use_legacy = bool(getattr(self, "_use_legacy_path", False))
            pipe = getattr(self, "_unified_pipeline", None)

            if pipe is not None and not use_legacy:
                try:
                    pipe.process(ctx)
                    return
                except Exception as e:
                    with contextlib.suppress(Exception):
                        self.logger.exception(f"UnifiedSignalPipeline failed, falling back to legacy: {e}")

            # ------------------------------------------------------------------
            # 6.2 Record ctx at bucket boundary (after all attach/fallback)
            # Этот момент максимально важен: ctx уже полностью сформирован, и replay
            # не будет зависеть от Redis/L2/L3/ATR/HTF провайдеров во время теста.
            # ------------------------------------------------------------------
            self._replay_record_ctx(ctx)

            # ---------------------------------------------------------------------
            # 9.5 Geometry/HTF: finalize ctx before signal generation
            # ---------------------------------------------------------------------
            # We do it at bucket boundary because:
            #   - ctx.price/atr/vwap/daily_open are already set
            #   - modular services already attached (regime/geometry snapshots)
            #   - we want stable "one ctx -> one score" semantics for scoring/gating
            #
            # CryptoOrderFlowHandler implements _update_geometry_liquidity_context(ctx).
            if hasattr(self, "_update_geometry_liquidity_context"):
                try:
                    self._update_geometry_liquidity_context(ctx)  # type: ignore[attr-defined]
                except Exception:
                    # fail-open: HTF missing must not break pipeline
                    pass

            self._generate_signals(ctx)

    def _handle_bucket_boundary(self, ctx: Any, mid: float) -> None:
        """
        Вызывается на bucket boundary, когда ctx полностью сформирован.
        """
        self._before_signal_generation(ctx)
        self._track_dependency_metrics(ctx)
        self._run_signal_generation(ctx)
        # update prev evaluation price at bucket boundary
        self._prev_eval_price = mid

    def _process_tick(self, tick: Tick) -> None:
        # -----------------------------
        # 5.1 ticks: ticks_in / parsed_ok / dropped_bad + tick_lag_ms p50/p95
        # 4.2 bad time: normalize + watermark drop
        # -----------------------------
        self._m_inc("ticks_in", 1, self._metric_tags_base())

        now_ms = get_ny_time_millis()

        # normalize tick.ts (sec -> ms)
        try:
            raw_ts = getattr(tick, "ts", None)
        except Exception:
            raw_ts = None
        ts_ms = None
        if normalize_ts_ms is not None:
            try:
                ts_ms = normalize_ts_ms(raw_ts)
            except Exception:
                ts_ms = None

        if ts_ms is None:
            # невозможно интерпретировать время => bad tick
            self._m_inc("ticks_dropped_bad", 1, {**self._metric_tags_base(), "reason": "bad_ts"})
            return

        # watermark (drop too future/past)
        if should_drop_by_watermark is not None:
            max_future = int(os.getenv("MAX_TICK_FUTURE_MS", "500"))
            max_past = int(os.getenv("MAX_TICK_PAST_MS", "5000"))
            try:
                drop, reason = should_drop_by_watermark(now_ms=now_ms, ts_ms=ts_ms, max_future_ms=max_future, max_past_ms=max_past)
            except Exception:
                drop, reason = False, ""
            if drop:
                self._m_inc("ticks_dropped_bad", 1, {**self._metric_tags_base(), "reason": reason})
                return

        # перезаписываем tick.ts нормализованным значением (важно для bucket boundary и всех downstream вычислений)
        with contextlib.suppress(Exception):
            tick.ts = int(ts_ms)

        # basic price sanity (минимально, без попыток "лечить" рынок)
        try:
            bid = float(getattr(tick, "bid", 0.0) or 0.0)
            ask = float(getattr(tick, "ask", 0.0) or 0.0)
            last = float(getattr(tick, "last", 0.0) or 0.0)
            if (bid <= 0.0 and ask <= 0.0 and last <= 0.0) or (ask > 0 and bid > 0 and ask < bid):
                self._m_inc("ticks_dropped_bad", 1, {**self._metric_tags_base(), "reason": "bad_px"})
                return
        except Exception:
            self._m_inc("ticks_dropped_bad", 1, {**self._metric_tags_base(), "reason": "bad_px_exc"})
            return

        self._m_inc("ticks_parsed_ok", 1, self._metric_tags_base())

        # lag percentiles
        try:
            if self._tick_lag is not None:
                self._tick_lag.feed(float(now_ms - int(ts_ms)))
                self._tick_lag.maybe_export(getattr(self, "_m2", None))
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Hard time protection "последняя гайка":
        # 1) normalize ts (sec->ms), watermark checks
        # 2) quarantine/state_freeze
        # 3) recovery gate: AFTER state_freeze ended, require N consecutive OK ticks
        #    before we allow state updates / signal generation.
        # ------------------------------------------------------------------

        # tick.ts может быть "грязным" (сек/мс, NaN/None). Здесь только метрики/валидность.
        ts_ms = None
        try:
            ts_v = safe_float(getattr(tick, "ts", None))
            if ts_v is not None:
                # Нормализация времени (часть защиты "плохого времени", но здесь только для лаг-метрик)
                ts_ms = int(ts_v)
                if ts_ms < 1_000_000_000_000:  # < 1e12 => секунды
                    ts_ms *= 1000
        except Exception:
            ts_ms = None

        if ts_ms is not None:
            now_ms = get_ny_time_millis()
            lag_ms = now_ms - ts_ms
            if self._tick_lag is not None:
                self._tick_lag.update(lag_ms)
                with contextlib.suppress(Exception):
                    self._tick_lag.maybe_export(m2)

        now_ms = int(get_ny_time_millis())

        # normalize + watermark
        ts_res = self._tick_time.sanitize_ts_ms(getattr(tick, "ts", None), now_ms=now_ms)
        if ts_res is None:
            # cannot parse ts at all -> hard drop
            self._bad_time_quarantine.on_hard_drop("bad_ts", now_ms)
            if self._drop_bad_time:
                return
        else:
            if ts_res.drop_reason:
                # hard drop (future/past/bad_ts etc)
                self._bad_time_quarantine.on_hard_drop(str(ts_res.drop_reason), now_ms)
                if self._drop_bad_time:
                    return
            else:
                # apply normalized ts back
                tick.ts = int(ts_res.ts_ms)
                if ts_res.flags:
                    for f in ts_res.flags:
                        self._bad_time_quarantine.on_soft_event(str(f))
                else:
                    self._bad_time_quarantine.on_ok_tick()

        # fail-fast on quarantine
        if self._bad_time_quarantine.is_quarantined(now_ms):
            return

        # recovery/state_freeze gate: blocks ALL downstream state updates/emit
        if self._bad_time_quarantine.should_suppress_processing(now_ms):
            return

        # сброс флага HLC fallback для текущего тика (если ваш ATR/HLC код его выставляет)
        self._last_hlc_fallback_used = False
        self.processed_ticks += 1

        # --- existing logic continues below ---
        mid = (tick.bid + tick.ask) / 2.0 if tick.bid and tick.ask else (tick.last or 0.0)

        # Optional: record raw tick snapshot (small, before any transforms)
        # NOTE: keep payload compact; do NOT dump huge objects here.
        with contextlib.suppress(Exception):
            self._replay_record_tick(
                {
                    "ts": int(getattr(tick, "ts", 0) or 0),
                    "bid": float(getattr(tick, "bid", 0.0) or 0.0),
                    "ask": float(getattr(tick, "ask", 0.0) or 0.0),
                    "last": float(getattr(tick, "last", 0.0) or 0.0),
                    "volume": float(getattr(tick, "volume", 0.0) or 0.0),
                    "flags": int(getattr(tick, "flags", 0) or 0),
                }
            )
        # Минимальная валидация "плохих тиков" для ticks_dropped_bad.
        # Важно: это НЕ заменяет вашу основную логику drop/watermark (она в другом блоке),
        # это только метрика качества входа.
        if mid is None or not isinstance(mid, (int, float)) or mid <= 0.0:
            try:
                if m2 is not None:
                    m2.inc("ticks_dropped_bad", 1)
            except Exception:
                pass
            return

        try:
            if m2 is not None:
                m2.inc("ticks_parsed_ok", 1)
        except Exception:
            pass

        # late feed protection: не генерим сигналы по сильно запоздавшим тикам
        now_ms = get_ny_time_millis()
        if tick.ts > 0 and (now_ms - tick.ts) > self.max_tick_lag_ms:
            # но бар/ATR можно всё равно обновлять (не критично)
            self._update_bar_range(mid, tick.ts)
            self.atr_calculator.feed_tick(mid, tick.ts)
            return

        # cheap per-tick updates
        self._update_bar_range(mid, tick.ts)
        self.atr_calculator.feed_tick(mid, tick.ts)

        delta = self._classify_delta(tick)

        # 1) сначала бакетизация (закрываем предыдущий, если нужно)
        closed_bucket_id = self._feed_delta_bucket(delta, tick.ts)

        if closed_bucket_id is not None:
            # advance предыдущего бакета
            if self.l3_queue is not None:
                self._l3_last_stats = self.l3_queue.on_bucket_advance(bucket_id=int(closed_bucket_id))
            self._burst_last_stats = self.burst.on_bucket_advance(bucket_id=int(closed_bucket_id))

            # Update new modular services with bar data
            bar_sample = BarSample(
                symbol=self.symbol,
                ts_event_ms=tick.ts,
                open=self._bar_open,
                high=self._bar_high,
                low=self._bar_low,
                close=mid,
                volume=getattr(self, '_bar_volume', 0.0)
            )
            self._regime_service.on_bar(bar_sample)
            self._geometry_service.on_bar(bar_sample)

            # update regime on bucket boundary (simplified candle)
            self._update_regime_on_bucket_close(tick.ts, mid)
            self._recompute_regime_if_needed()

        # 2) теперь учитываем trade текущего тика (он уже относится к текущему бакету)
        is_trade = self._is_trade_tick(tick)
        if is_trade:
            side = self._taker_side(tick)

            if self.l3_queue is not None:
                self.l3_queue.on_trade(side=side, qty=float(tick.volume or 0.0))

            self.burst.on_trade(ts=int(tick.ts), side=side)

            # L3LiteTracker hook (override in crypto)
            with contextlib.suppress(Exception):
                self._l3_on_tick_trade(tick, signed_delta=delta)

            # Touch-level tracker: кормим сделки
            with contextlib.suppress(Exception):
                self.touch.on_trade(
                    ts=int(tick.ts),
                    price=float(tick.last or 0.0),
                    qty=float(tick.volume or 0.0),
                    side=int(side),
                )

        # 3) тяжёлое считаем только на границе бакета
        if closed_bucket_id is None:
            return

        atr_val = self._get_atr(mid, tick.ts)
        self._update_pivots(tick.ts)

        # Robust Z only once per bucket (GPU-aware rolling, no per-bucket CPU->GPU transfer)
        if len(self.delta_window) >= 1:
            last_bucket = self.delta_window[-1]
        else:
            last_bucket = self._last_bucket_value
        res = self._delta_rr.update(last_bucket)
        z_delta = res.z
        self._last_z_delta = z_delta

        bar_range = abs(self.bar_high - self.bar_low)
        weak_progress = check_weak_progress(bar_range, atr_val, self.config.weak_progress_atr)

        m = self.data_processor.get_obi_metrics()
        obi = float(m["obi"])
        obi_avg = float(m["obi_avg"])
        obi_sustained = bool(m["obi_sustained"])

        obi20 = float(m["obi20"])
        obi20_avg = float(m["obi20_avg"])
        obi20_sustained = bool(m["obi20_sustained"])
        obi20_valid = bool(m["obi20_valid"])

        # Расчет дополнительных ценовых полей
        last_price = mid  # используем mid как last_price
        vwap = getattr(self, "current_vwap", None)

        # Daily open calculation
        daily_open: float | None = None
        daily_open_dist_bps: float | None = None

        # Пытаемся получить daily OHLC из различных источников
        daily_ohlc = getattr(self, "daily_ohlc", None)
        if daily_ohlc is None:
            # Попробуем из pivots или других источников
            hlc = self._load_yesterday_hlc()
            if hlc and "O" in hlc:
                daily_open = float(hlc["O"])
            elif hasattr(self, "daily_open"):
                daily_open = self.daily_open

        if daily_ohlc is not None:
            with contextlib.suppress(Exception):
                daily_open = float(getattr(daily_ohlc, "open", getattr(daily_ohlc, "O", None)))

        if daily_open is not None and last_price is not None and daily_open > 0.0:
            rel = abs(last_price - daily_open) / daily_open
            daily_open_dist_bps = rel * 10_000.0  # в базисных пунктах

        # Расчет cum_delta_slope (упрощенный - используем последние значения)
        cum_delta_slope = None
        if len(self._regime_window) >= 2:
            # Берем последние 5 значений для оценки тренда
            recent_cum_deltas = [w["cum_delta"] for w in list(self._regime_window)[-5:]]
            if len(recent_cum_deltas) >= 2:
                n = len(recent_cum_deltas)
                x = list(range(n))
                x_mean = sum(x) / n
                y_mean = sum(recent_cum_deltas) / n
                num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, recent_cum_deltas))
                den = sum((xi - x_mean) ** 2 for xi in x) or 1.0
                cum_delta_slope = num / den if den > 0 else 0.0

        # Расчет ATR quantile
        atr_q_14 = None
        if atr_val > 0:
            # Простая оценка quantile на основе типичного диапазона ATR
            if atr_val <= 0.0001:
                atr_q_14 = 0.0
            elif atr_val >= 0.01:
                atr_q_14 = 1.0
            else:
                atr_q_14 = min(1.0, max(0.0, (atr_val - 0.0005) / (0.005 - 0.0005)))

        ctx = OrderflowContext(
            ts=tick.ts,
            price=mid,
            z_delta=z_delta,
            weak_progress=weak_progress,
            obi=obi,
            obi_avg=obi_avg,
            obi_sustained=obi_sustained,
            atr=atr_val,
            pivots=self.daily_pivots,
            delta_window=self.delta_window,
            current_delta=delta,
            delta_bucket=self._last_bucket_value,
            regime=self.regime_state.label,
            regime_trend_score=self.regime_state.trend_score,
            regime_range_score=self.regime_state.range_score,
            last_price=last_price,
            vwap=vwap,
            daily_open=daily_open,
            daily_open_dist_bps=daily_open_dist_bps,
            cum_delta_slope=cum_delta_slope,
            atr_q_14=atr_q_14,
            # Extended regime fields
            ts_utc=tick.ts / 1000.0 if hasattr(tick, 'ts') else time.time(),
            daily_atr_bps=getattr(self, 'daily_atr_bps', None),
            weak_progress_ratio=bar_range / atr_val if atr_val > 0 else None,
            htf_level_dist_bps=getattr(self, 'htf_level_dist_bps', None),
            # Geometry and liquidity fields will be populated by CryptoOrderFlowHandler
            # atr_htf_bps=getattr(self, 'atr_htf_bps', None),
            # geo_zone_hits=None,
            # is_new_local_extreme=None,
            # geometry_score=None,
            # liquidity_ctx=None,

            # Regime guard fields
            family=self.family,
            venue=self.venue,
            symbol=self.symbol,
            timeframe=self.timeframe,
        )
        # 4.1: переносим flags, накопленные на bucket
        try:
            if getattr(ctx, "data_quality_flags", None) is None:
                ctx.data_quality_flags = []
            for f in sorted(self._quality_flags_bucket):
                if f not in ctx.data_quality_flags:
                    ctx.data_quality_flags.append(f)
        except Exception:
            pass

        # Candles/HLC fallback помечаем флагом (сам фоллбек — в вашем ATR/HLC коде).
        # Любой код, который использует HLC/ATR fallback, должен выставлять:
        #   self._last_hlc_fallback_used = True
        # Тогда здесь флаг попадёт в ctx и будет промаркирован как "hlc_fallback".
        with contextlib.suppress(Exception):
            ctx.hlc_fallback_used = bool(getattr(self, "_last_hlc_fallback_used", False))

        # Attach data from new modular services
        self._attach_modular_services_data(ctx)

        # L3-Lite метрики (для CryptoOrderFlowHandler)
        if hasattr(self, 'l3_agg'):
            l3_feat = self.l3_agg.build_features(tick.ts)
            if l3_feat is not None:
                # L3 метрики (минимальный набор):
                #   - l3_event_rate (gauge, events/sec, EMA)
                #   - l3_missing_rate (gauge, missing/total)
                # 5.1 L3: event_rate + missing_rate
                try:
                    miss = (l3_feat is None)
                    if getattr(self, "_l3_missing", None) is not None:
                        self._l3_missing.mark(miss=bool(miss))
                        self._l3_missing.maybe_export(getattr(self, "_m2", None))
                    if not miss and getattr(self, "_l3_rate", None) is not None:
                        self._l3_rate.mark_event(now_ms=get_ny_time_millis())
                        self._l3_rate.maybe_export(getattr(self, "_m2", None), now_ms=get_ny_time_millis())
                except Exception:
                    pass
            if l3_feat is not None:
                ctx.cancel_to_trade_bid_5s = l3_feat.cancel_to_trade_bid_5s
                ctx.cancel_to_trade_ask_5s = l3_feat.cancel_to_trade_ask_5s
                ctx.cancel_to_trade_bid_20s = l3_feat.cancel_to_trade_bid_20s
                ctx.cancel_to_trade_ask_20s = l3_feat.cancel_to_trade_ask_20s
                ctx.microprice_shift_bps_20 = l3_feat.microprice_shift_bps_20
                ctx.spread_bps = l3_feat.spread_bps
                ctx.obi_5 = l3_feat.obi_5
                ctx.obi_20 = l3_feat.obi_20
                ctx.obi_50 = l3_feat.obi_50
        else:
            # l3_agg отсутствует => missing
            if getattr(self, "_l3_missing", None) is not None:
                try:
                    self._l3_missing.mark(miss=True)
                    self._l3_missing.maybe_export(m2)
                except Exception:
                    pass

    def _attach_modular_services_data(self, ctx: OrderflowContext) -> None:
        """Attach data from new modular services to OrderflowContext"""
        # Get regime data
        regime_snapshot = self._regime_service.get_regime(self.symbol)
        if regime_snapshot:
            # Update regime fields in context
            ctx.regime = regime_snapshot.regime.value
            ctx.market_regime = regime_snapshot.regime.value
            ctx.market_regime_score = regime_snapshot.trend_score - regime_snapshot.range_score
            ctx.atr_quantile = regime_snapshot.atr_quantile
            ctx.is_trending = regime_snapshot.is_trending

        # Get geometry data
        geometry_snapshot = self._geometry_service.get_geometry(
            symbol=self.symbol,
            ts_event_ms=ctx.ts,
            price=ctx.price
        )
        if geometry_snapshot:
            ctx.geometry = geometry_snapshot
            # если snapshot содержит geometry_score — оставляем его, иначе сохраняем default
            try:
                if getattr(ctx, "geometry_score", None) is None and hasattr(geometry_snapshot, "geometry_score"):
                    ctx.geometry_score = float(geometry_snapshot.geometry_score)
            except Exception:
                pass
        else:
            # 4.1: HTF levels недоступны -> geometry_score = 0.1 (нейтраль), без veto
            with contextlib.suppress(Exception):
                ctx.geometry_score = float(getattr(ctx, "geometry_score", 0.1) or 0.1)
            flags = self._dq_flags(ctx)
            if "htf_missing" not in flags:
                flags.append("htf_missing")

        # Apply calibration if we have a SignalContext-like object
        if hasattr(ctx, 'metrics') and hasattr(ctx, 'calibrated'):
            self._calibration_service.apply_calibration(ctx)

        # L3-Lite additional metrics
        if hasattr(self, 'l3_agg') and l3_feat is not None:
            ctx.obi_persistence_score = l3_feat.obi_persistence_score
            ctx.microprice_velocity_bps = l3_feat.microprice_velocity_bps
            ctx.queue_pressure_bid = l3_feat.queue_pressure_bid
            ctx.queue_pressure_ask = l3_feat.queue_pressure_ask
            ctx.market_depth_imbalance = l3_feat.market_depth_imbalance

        # --- L2 staleness bookkeeping ---
        # staleness + skew diagnostics
        stale_ms = int(os.getenv("L2_STALE_MS", os.getenv("L2_MAX_STALE_MS", "1500")))
        neg_skew_stale_ms = int(os.getenv("L2_NEGATIVE_SKEW_STALE_MS", "5000"))
        l2_age_warn_ms = int(os.getenv("L2_MAX_AGE_MS", "60000"))
        tick_lag_warn_ms = int(os.getenv("TICK_LAG_WARN_MS", "5000"))

        now_ms = get_ny_time_millis()
        book_ts_ms = int(self._ts_to_ms(self._l2_last_ts, label="book.ts")) if self._l2_last_ts else 0
        tick_ts_ms = int(self._ts_to_ms(tick.ts, label="tick.ts"))
        age_now_raw = int(now_ms - int(book_ts_ms or 0))
        age_tick_raw = int(tick_ts_ms - int(book_ts_ms or 0)) if (book_ts_ms > 0 and tick_ts_ms > 0) else 0

        ctx.l2_ts = int(book_ts_ms)
        ctx.l2_age_ms_raw = int(age_now_raw)
        ctx.l2_age_ms = int(max(0, age_now_raw))
        ctx.l2_skew_ms = int(-age_now_raw) if age_now_raw < 0 else 0
        ctx.l2_skew_flag = bool(age_now_raw < -neg_skew_stale_ms)

        ctx.l2_age_ms_tick_raw = int(age_tick_raw)
        ctx.l2_age_ms_tick = int(max(0, age_tick_raw))
        ctx.l2_skew_tick_ms = int(-age_tick_raw) if age_tick_raw < 0 else 0
        ctx.l2_skew_tick_flag = bool(age_tick_raw < -neg_skew_stale_ms)

        no_snapshot = book_ts_ms <= 0
        ctx.l2_is_stale = bool(no_snapshot) or ctx.l2_skew_flag or ctx.l2_skew_tick_flag or (ctx.l2_age_ms_tick > stale_ms)
        ctx.l2_is_stale_now = bool(no_snapshot) or ctx.l2_skew_flag or (ctx.l2_age_ms > stale_ms)

        if book_ts_ms > 0 and age_now_raw > l2_age_warn_ms and self._l2_warn_allowed(now_ms):
            self.logger.warning(
                "Stale L2(now): age_now_raw=%s age_tick_raw=%s tick_ts_ms=%s book_ts_ms=%s now_ms=%s",
                age_now_raw, age_tick_raw, tick_ts_ms, book_ts_ms, now_ms,
            )
        if book_ts_ms > 0 and (ctx.l2_skew_flag or ctx.l2_skew_tick_flag) and self._l2_warn_allowed(now_ms):
            self.logger.warning(
                "Clock skew: age_now_raw=%s age_tick_raw=%s skew_now=%s skew_tick=%s tick_ts_ms=%s book_ts_ms=%s now_ms=%s",
                age_now_raw, age_tick_raw, ctx.l2_skew_ms, ctx.l2_skew_tick_ms, tick_ts_ms, book_ts_ms, now_ms,
            )
        tick_lag_ms = now_ms - tick_ts_ms if tick_ts_ms > 0 else 0
        if tick_ts_ms > 0 and tick_lag_ms > tick_lag_warn_ms and self._l2_warn_allowed(now_ms):
            self.logger.warning(
                "Tick lag: tick_lag_ms=%s tick_ts_ms=%s now_ms=%s",
                tick_lag_ms, tick_ts_ms, now_ms,
            )

        # --- Touch-level stats ---
        try:
            snap: TouchSnapshot = self.touch.snapshot(ts=int(tick.ts))
            ctx.touch_bid_tag = snap.bid_tag
            ctx.touch_ask_tag = snap.ask_tag
            ctx.touch_bid_rho = snap.bid_rho
            ctx.touch_ask_rho = snap.ask_rho
            ctx.touch_bid_traded_w = snap.bid_traded_w
            ctx.touch_ask_traded_w = snap.ask_traded_w
            ctx.touch_bid_drop_w = snap.bid_drop_w
            ctx.touch_ask_drop_w = snap.ask_drop_w
            ctx.touch_is_stale = snap.is_stale
        except Exception:
            pass

        # attach L2 metrics snapshot (only if fresh)
        if (
            self._l2_last
            and self._l2_last.m
            and self._l2_last.m.mid > 0
            and (not ctx.l2_is_stale)
        ):
            m = self._l2_last.m
            ch = self._l2_last.ch

            ctx.depth_bid_5 = m.depth_bid_5
            ctx.depth_ask_5 = m.depth_ask_5
            ctx.depth_bid_20 = m.depth_bid_20
            ctx.depth_ask_20 = m.depth_ask_20

            ctx.obi_20 = obi20
            ctx.obi_avg_20 = obi20_avg
            ctx.obi_sustained_20 = obi20_sustained

            ctx.slope_bid_20 = m.slope_bid_20
            ctx.slope_ask_20 = m.slope_ask_20
            ctx.microprice_shift_bps_20 = m.microprice_shift_bps_20

            ctx.wall_bid = m.wall_bid
            ctx.wall_ask = m.wall_ask
            ctx.wall_bid_dist_bps = m.wall_bid_dist_bps
            ctx.wall_ask_dist_bps = m.wall_ask_dist_bps

            ctx.bid_top3_ratio = ch.bid_top3_ratio
            ctx.ask_top3_ratio = ch.ask_top3_ratio
            ctx.bid_top5_ratio = ch.bid_top5_ratio
            ctx.ask_top5_ratio = ch.ask_top5_ratio

            # direction-specific refill/depletion + impact_proxy
            depth_near = max(1e-9, (m.depth_bid_5 + m.depth_ask_5))
            ctx.impact_proxy = abs(ctx.delta_bucket) / depth_near

            if ctx.delta_bucket > 0:
                r = ch.ask_top5_ratio
                ctx.refill_score = max(0.0, r)
                ctx.depletion_score = max(0.0, -r)
            elif ctx.delta_bucket < 0:
                r = ch.bid_top5_ratio
                ctx.refill_score = max(0.0, r)
                ctx.depletion_score = max(0.0, -r)
            else:
                ctx.refill_score = 0.0
                ctx.depletion_score = 0.0

        # attach L3-lite stats + derived proxies (safe defaults if absent)
        if self._l3_last_stats is not None:
            # attach L3-lite stats
            ctx.taker_buy_qty_bucket = float(self._l3_last_stats.taker_buy_qty)
            ctx.taker_sell_qty_bucket = float(self._l3_last_stats.taker_sell_qty)
            ctx.taker_buy_rate_ema = float(self._l3_last_stats.taker_buy_rate_ema)
            ctx.taker_sell_rate_ema = float(self._l3_last_stats.taker_sell_rate_ema)

            # pull/cancel proxies from L2 change ratios (if present)
            ctx.pull_ask_qty_proxy = max(0.0, -float(getattr(ctx, "ask_top5_ratio", 0.0) or 0.0))
            ctx.pull_bid_qty_proxy = max(0.0, -float(getattr(ctx, "bid_top5_ratio", 0.0) or 0.0))

            # cancel-to-trade proxies (нормируем "исчезнувшую" ликвидность на depth)
            pulled_ask_qty_proxy = max(0.0, -float(getattr(ctx, "ask_top5_ratio", 0.0) or 0.0)) * float(getattr(ctx, "depth_ask_5", 0.0) or 0.0)
            pulled_bid_qty_proxy = max(0.0, -float(getattr(ctx, "bid_top5_ratio", 0.0) or 0.0)) * float(getattr(ctx, "depth_bid_5", 0.0) or 0.0)

            buy_qty = max(self.l3_eps, ctx.taker_buy_qty_bucket)
            sell_qty = max(self.l3_eps, ctx.taker_sell_qty_bucket)
            ctx.cancel_to_trade_ask = pulled_ask_qty_proxy / buy_qty
            ctx.cancel_to_trade_bid = pulled_bid_qty_proxy / sell_qty

            # cancel-rate EMA (qty/sec) по pulled liquidity
            try:
                if self.l3_queue is not None and hasattr(self.l3_queue, "on_bucket_close"):
                    self.l3_queue.on_bucket_close(
                        pulled_bid_qty_proxy=pulled_bid_qty_proxy,
                        pulled_ask_qty_proxy=pulled_ask_qty_proxy,
                        bucket_ms=self.delta_bucket_ms,
                    )
                    ctx.cancel_bid_rate_ema = float(getattr(self.l3_queue, "cancel_bid_rate_ema", 0.0))
                    ctx.cancel_ask_rate_ema = float(getattr(self.l3_queue, "cancel_ask_rate_ema", 0.0))
            except Exception:
                pass

            # ETA proxies (depth / absorption speed)
            if self.eta_eval is not None:
                # ask side eaten by taker-buy
                ctx.eta_fill_ask_sec = self.eta_eval.eta(
                    depth_qty=float(getattr(ctx, "depth_ask_5", 0.0) or 0.0),
                    taker_rate_ema=float(ctx.taker_buy_rate_ema),
                ).eta_sec
                # bid side eaten by taker-sell
                ctx.eta_fill_bid_sec = self.eta_eval.eta(
                    depth_qty=float(getattr(ctx, "depth_bid_5", 0.0) or 0.0),
                    taker_rate_ema=float(ctx.taker_sell_rate_ema),
                ).eta_sec

        # Hook: allow subclasses to augment context with microstructure data
        if hasattr(self, "_augment_context_microstructure"):
            self._augment_context_microstructure(ctx, tick)

        # ---- attach Burstiness stats (за предыдущий бакет) ----
        if self._burst_last_stats is not None:
            bs = self._burst_last_stats
            ctx.burst_trade_count_bucket = int(bs.trade_count_bucket)
            ctx.burst_rate_short = float(bs.rate_short)
            ctx.burst_rate_long = float(bs.rate_long)
            ctx.burst_ratio = float(bs.burst_ratio)
            ctx.burst_cv_dt = float(bs.cv_dt)
            ctx.burst_fano_counts = float(bs.fano_counts)
            ctx.burst_flip_ratio = float(bs.flip_ratio)

        # 4.1: candles fallback marker (должен стоять прямо перед генерацией/скорингом)
        self._mark_hlc_fallback_flag(ctx)

        # Geometry/HTF enrichment hook:
        # At this point ctx is fully formed (price/atr/vwap/daily_open/L3/modular services),
        # so geo_zone_hits/top-hit/geometry_score can be computed once per bucket boundary.
        #
        # Implemented in CryptoOrderFlowHandler (and potentially other subclasses).
        # Fail-open: geometry must never break signal generation.
        if hasattr(self, "_update_geometry_liquidity_context"):
            try:
                self._update_geometry_liquidity_context(ctx)
            except Exception:
                # keep fail-open behavior; logging is optional (avoid spam at tick rate)
                pass

        # 3.4 + 4.1: геометрия/HTF финализируется здесь (ctx уже собран)
        if hasattr(self, "_update_geometry_liquidity_context"):
            try:
                self._update_geometry_liquidity_context(ctx)
            except Exception:
                # fail-open: не роняем генерацию сигналов из-за HTF/геометрии
                try:
                    flags = getattr(ctx, "data_quality_flags", None) or []
                    if isinstance(flags, list) and "geo_exception" not in flags:
                        flags.append("geo_exception")
                        ctx.data_quality_flags = flags
                except Exception:
                    pass

        # Geometry/liquidity finalization at bucket boundary (4.1 / 3.4)
        # ctx уже полностью сформирован и обогащён модульными сервисами
        try:
            if hasattr(self, "_update_geometry_liquidity_context"):
                self._update_geometry_liquidity_context(ctx)  # type: ignore[misc]
        except Exception:
            self.logger.exception("update_geometry_liquidity_context failed (fail-open)")

        # "совсем жёстко": единый global QualityGate ДО любых путей (legacy/unified)
        try:
            if self._quality_gate is None:
                from handlers.quality.quality_gate import QualityGate
                self._quality_gate = QualityGate()
            # global assess (L3/geo/ATR flags). kind-specific будет на кандидате.
            self._quality_gate.assess_global(ctx=ctx)
        except Exception:
            # fail-open
            try:
                flags = getattr(ctx, "data_quality_flags", None) or []
                if isinstance(flags, list) and "quality_gate_exception" not in flags:
                    flags.append("quality_gate_exception")
                    ctx.data_quality_flags = flags
            except Exception:
                pass

        # Политики зависимостей: дефолты + флаги качества данных
        # (HTF missing -> geometry_score=0.1, L3 missing -> l3_score=0.5, etc.)
        try:
            ensure_dependency_defaults(ctx)
        except Exception:
            with contextlib.suppress(Exception):
                self.logger.exception("ensure_dependency_defaults failed (fail-open)")

        # Bucket boundary финализация + генерация сигналов
        self._handle_bucket_boundary(ctx, mid)

    def _extract_top1(self, x: Any) -> tuple[float, float]:
        """Извлекает top1 (price, qty) из массива уровней."""
        if not x:
            return 0.0, 0.0
        lv = x[0]
        if isinstance(lv, (list, tuple)) and len(lv) >= 2:
            try:
                return float(lv[0]), float(lv[1])
            except Exception:
                return 0.0, 0.0
        if isinstance(lv, dict):
            try:
                p = float(lv.get("p") or lv.get("price") or 0.0)
                q = float(lv.get("q") or lv.get("qty") or lv.get("size") or 0.0)
                return p, q
            except Exception:
                return 0.0, 0.0
        return 0.0, 0.0

    def _extract_top_levels(self, book_data: dict[str, Any], side: str, n: int = 3) -> list[tuple[float, float]]:
        """Извлекает top N уровней из book_data для указанной стороны."""
        keys = [side]
        if side == "bids":
            keys += ["b", "bid", "BIDS"]
        else:
            keys += ["a", "ask", "ASKS"]

        arr = None
        for k in keys:
            if k in book_data:
                arr = book_data.get(k)
                break
        if not arr or not isinstance(arr, list):
            return []

        out: list[tuple[float, float]] = []
        for i in range(min(n, len(arr))):
            lvl = arr[i]
            try:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    p, q = float(lvl[0]), float(lvl[1])
                elif isinstance(lvl, dict):
                    p = float(lvl.get("p") or lvl.get("price") or 0)
                    q = float(lvl.get("q") or lvl.get("qty") or lvl.get("size") or 0)
                else:
                    continue
                if p > 0 and q >= 0:
                    out.append((p, q))
            except Exception:
                continue
        return out

    def _process_book(self, book_data: dict[str, Any]) -> None:
        """
        Обработка Order Book с полными L2-метриками и батч-оптимизацией.
        
        ✅ ОПТИМИЗИРОВАНО: Использует батч-буферизацию для GPU ускорения.
        """
        self.processed_books += 1
        ts_norm = normalize_epoch_ms(book_data.get("ts"))
        ts = int(ts_norm or get_ny_time_millis())
        book_data["ts"] = ts

        # init flush ts on first ever book
        if self._book_buffer_last_flush_ts <= 0:
            self._book_buffer_last_flush_ts = ts

        # Touch-level tracker: кормим сразу (не ждём flush) - до буферизации
        bids = book_data.get("bids") or book_data.get("b") or []
        asks = book_data.get("asks") or book_data.get("a") or []
        bid_p, bid_q = self._extract_top1(bids)
        ask_p, ask_q = self._extract_top1(asks)
        try:
            if bid_p > 0 and ask_p > 0:
                self.touch.on_book(ts=ts, bid_p=bid_p, bid_q=bid_q, ask_p=ask_p, ask_q=ask_q)
        except Exception:
            pass

        # append
        self._book_buffer.append(book_data)

        # ✅ timeout считается от last_flush
        should_process = (
            len(self._book_buffer) >= self._book_buffer_max or
            (ts - self._book_buffer_last_flush_ts) >= self._book_buffer_timeout_ms
        )

        if not should_process:
            return

        if not self._book_buffer:
            return

        batch_min = int(os.getenv("L2_BATCH_MIN", "3"))
        # process buffer
        if len(self._book_buffer) >= max(1, batch_min):
            try:
                snapshots = self.l2.feed_batch(self._book_buffer)
                for snap in snapshots:
                    if snap:
                        self._process_l2_snapshot(snap, int(snap.m.ts or ts))
            except Exception as e:
                self.logger.warning(f"⚠️ Batch processing failed: {e}, falling back to single")
                for book in self._book_buffer:
                    snap = self.l2.feed(book)
                    if snap:
                        self._process_l2_snapshot(snap, int(snap.m.ts or ts))
        else:
            for book in self._book_buffer:
                snap = self.l2.feed(book)
                if snap:
                    self._process_l2_snapshot(snap, int(snap.m.ts or ts))

        self._book_buffer.clear()
        self._book_buffer_last_flush_ts = ts

    def _process_l2_snapshot(self, snap: Any, ts: int) -> None:
        """
        Обработка L2 snapshot (вынесено для переиспользования).
        
        Args:
            snap: L2Snapshot
            ts: Timestamp
        """
        # если L2Metrics содержит ts — используем его (лучше для l2_age_ms)
        try:
            if getattr(snap, "m", None) is not None and getattr(snap.m, "ts", 0):
                ts = int(snap.m.ts)
        except Exception:
            pass

        self._l2_last = snap

        # base OBI fields keep depth=5 semantics
        self._last_obi = float(snap.m.obi_5)
        self._last_obi_ts = ts

        # OBI is computed by L2MicrostructureEngine and stored in BucketState.
        # No legacy tracking needed here.
        pass

        # Update L2 snapshot timestamp
        self._l2_last_ts = ts

        # L3-lite: update decomposition on every book snapshot
        try:
            if self.l3_lite_enabled and self.l3 and snap.m:
                self.l3.on_book(ts=ts, depth_bid_5=float(snap.m.depth_bid_5), depth_ask_5=float(snap.m.depth_ask_5))
        except Exception:
            pass

    # -------------------- delta logic --------------------

    def _feed_delta_bucket(self, delta: float, ts: int) -> int | None:
        """
        Bucketization по bucket_id (ts // bucket_ms).
        Возвращает ID закрытого бакета, когда предыдущий бакет закрыт и записан в delta_window.
        None => бакет ещё не закрыт.
        """
        b = ts // max(self.delta_bucket_ms, 1)

        if self._bucket_id is None:
            self._bucket_id = b
            self._bucket_sum = float(delta)
            self._last_bucket_value = 0.0
            return None

        if b != self._bucket_id:
            closed_id = int(self._bucket_id)
            # flush previous
            self._last_bucket_value = self._bucket_sum
            self.delta_window.append(self._last_bucket_value)

            # fill gaps (bounded)
            gap = b - self._bucket_id - 1
            if gap > 0:
                for _ in range(min(gap, self.max_zero_buckets)):
                    self.delta_window.append(0.0)

            # start new
            self._bucket_id = b
            self._bucket_sum = 0.0
            self._bucket_sum += delta
            return closed_id

        self._bucket_sum += delta
        return None

    def _classify_delta(self, tick: Tick) -> float:
        """
        Базовая delta-логика: знак (aggressor proxy) * volume.
        Специализации (crypto) могут переопределять.
        """
        vol = float(tick.volume) if tick.volume and tick.volume > 0 else 1.0

        if tick.flags:
            if tick.flags & 2:
                return +vol
            if tick.flags & 4:
                return -vol

        if tick.last and tick.ask and tick.last >= tick.ask:
            return +vol
        if tick.last and tick.bid and tick.last <= tick.bid:
            return -vol

        mid = (tick.bid + tick.ask) / 2.0 if (tick.bid and tick.ask) else 0.0
        if tick.last and mid:
            return +vol if tick.last > mid else -vol

        return 0.0

    def _taker_side(self, tick: Tick) -> int:
        """
        +1 taker-buy, -1 taker-sell, 0 unknown.
        Base fallback uses last vs mid.
        Crypto overrides via is_buyer_maker.
        """
        if tick.last and tick.bid and tick.ask:
            mid = 0.5 * (tick.bid + tick.ask)
            if tick.last > mid:
                return +1
            if tick.last < mid:
                return -1
        return 0

    # -------------------- OBI logic --------------------

    def _obi_sustained_eval(self, samples: list[float], thr: float) -> tuple[float, bool]:
        """
        Оценка sustained OBI по фракции сэмплов, подтверждающих направление.
        
        Returns:
            (avg, sustained) где sustained=True если достаточно сэмплов подтверждают направление
        """
        if not samples:
            return 0.0, False
        avg = sum(samples) / len(samples)

        if not self.obi_use_fraction:
            return avg, abs(avg) >= thr

        if len(samples) < max(1, self.obi_min_samples):
            return avg, False

        sgn = 1 if avg > 0 else (-1 if avg < 0 else 0)
        if sgn == 0:
            return avg, False

        ok = 0
        for v in samples:
            if (v * sgn) > 0 and abs(v) >= thr:
                ok += 1

        frac = ok / max(1, len(samples))
        return avg, frac >= max(0.0, min(1.0, self.obi_min_fraction))

    def _track_obi(self, ts: int, obi5: float, obi20: float) -> None:
        """
        Отслеживание OBI на двух глубинах (5 и 20 уровней).
        """
        duration_ms = int(self.config.obi_min_duration * 1000)

        self._obi_state_5.append((ts, float(obi5)))
        self._obi_state_20.append((ts, float(obi20)))

        while self._obi_state_5 and ts - self._obi_state_5[0][0] > duration_ms:
            self._obi_state_5.popleft()
        while self._obi_state_20 and ts - self._obi_state_20[0][0] > duration_ms:
            self._obi_state_20.popleft()

    def _get_obi(self, ts: int) -> tuple[float, float, bool, float, float, bool]:
        """
        Возвращает OBI на двух глубинах (5 и 20 уровней).
        
        Returns:
            (obi5, avg5, sustained5, obi20, avg20, sustained20)
        """
        max_stale_ms = int(os.getenv("OBI_MAX_STALE_MS", "2500"))

        stale5 = (not self._last_obi_ts) or (ts - self._last_obi_ts > max_stale_ms)
        stale20 = (not self._last_obi_20_ts) or (ts - self._last_obi_20_ts > max_stale_ms)

        if stale5:
            self._obi_state_5.clear()
            obi5 = self._calc_obi_surrogate()
            avg5, sus5 = obi5, False
        else:
            samples5 = [v for _, v in self._obi_state_5]
            avg5, sus5 = self._obi_sustained_eval(samples5, self.config.obi_threshold)
            obi5 = self._last_obi

        if stale20:
            self._obi_state_20.clear()
            obi20 = obi5  # degrade gracefully
            avg20, sus20 = obi20, False
        else:
            samples20 = [v for _, v in self._obi_state_20]
            avg20, sus20 = self._obi_sustained_eval(samples20, self.config.obi_threshold)
            obi20 = self._last_obi_20

        return float(obi5), float(avg5), bool(sus5), float(obi20), float(avg20), bool(sus20)

    def _metric_tags_base(self) -> dict[str, str]:
        """
        Базовые теги, чтобы не размазывать метрики:
          symbol/timeframe (+ family/venue если есть)
        """
        tags: dict[str, str] = {
            "symbol": str(getattr(self, "symbol", "") or ""),
            "timeframe": str(getattr(self, "timeframe", "") or ""),
        }
        fam = getattr(self, "family", None)
        ven = getattr(self, "venue", None)
        if fam:
            tags["family"] = str(fam)
        if ven:
            tags["venue"] = str(ven)
        return tags

    def _m_inc(self, name: str, value: int = 1, tags: dict[str, Any] | None = None) -> None:
        m = getattr(self, "_m2", None)
        if m is None or not hasattr(m, "inc"):
            return
        try:
            m.inc(name, int(value), tags)
        except Exception:
            return

    def _update_micro_fast(self, tick: Tick) -> None:
        # --- 4.2 also harden fast micro update against future ticks
        try:
            nm = int(self._steady_clock.now_ms())
            res: SanitizeResult | None = self._tick_time.sanitize_ts_ms(getattr(tick, "ts", None), now_ms=nm)
            if res is None:
                return
            if self._drop_bad_time and res.drop_reason is not None:
                self._bad_time_quarantine.on_hard_drop(str(res.drop_reason), now_ms=nm)
                return
            if res.flags:
                for f in res.flags:
                    self._bad_time_quarantine.on_soft_event(str(f))
            else:
                self._bad_time_quarantine.on_ok_tick()

            # keep legacy lag gate (stricter), after sanitize
            if int(getattr(self, "max_tick_lag_ms", 0) or 0) > 0:
                if (nm - int(res.ts_ms)) > int(self.max_tick_lag_ms):
                    return
            tick.ts = int(res.ts_ms)
        except Exception:
            # fail-open: do not crash the handler on telemetry
            pass
        # ... existing _update_micro_fast body continues unchanged ...

    def _calc_obi_surrogate(self) -> float:
        if not self.delta_window:
            return 0.0
        buys = sum(1 for v in self.delta_window if v > 0)
        sells = sum(1 for v in self.delta_window if v < 0)
        total = buys + sells
        if total <= 0:
            return 0.0
        return (buys - sells) / total

    # -------------------- bar range & pivots --------------------

    def _update_bar_range(self, price: float, ts: int) -> None:
        if self.bar_start_ts == 0:
            self.bar_start_ts = ts
            self.bar_high = price
            self.bar_low = price
            return

        if ts - self.bar_start_ts >= 60_000:
            self.bar_start_ts = ts
            self.bar_high = price
            self.bar_low = price
            return

        self.bar_high = max(self.bar_high, price)
        self.bar_low = min(self.bar_low, price)

    def _update_pivots(self, ts: int) -> None:
        from datetime import datetime

        current_date = datetime.utcfromtimestamp(ts / 1000).date()
        date_str = current_date.strftime("%Y-%m-%d")

        if self.last_pivot_date == current_date and self.daily_pivots:
            return

        self.last_pivot_date = current_date

        cache_key = f"{self.symbol}:{date_str}"
        cached = self._pivot_cache.get_pivots(cache_key)
        if cached:
            self.daily_pivots = cached
            return

        hlc = self._load_yesterday_hlc()
        if not hlc:
            hlc = self._get_default_hlc()

        piv = compute_daily_pivots(hlc)
        self.daily_pivots = piv
        self._pivot_cache.set_pivots(cache_key, piv)

    def _load_yesterday_hlc(self) -> dict[str, float] | None:
        """
        1) pivots:latest:{symbol}
        2) pivots:latest (dict с ключом symbol или просто HLC)
        3) fallback: расчет из тиков за 24h
        """
        try:
            raw = self.redis.get(f"pivots:latest:{self.symbol}")
            if raw:
                data = json.loads(_to_str(raw))
                if all(k in data for k in ("H", "L", "C")):
                    return {"H": float(data["H"]), "L": float(data["L"]), "C": float(data["C"])}
        except Exception:
            pass

        try:
            raw = self.redis.get("pivots:latest")
            if raw:
                data = json.loads(_to_str(raw))
                if isinstance(data, dict) and self.symbol in data and isinstance(data[self.symbol], dict):
                    d = data[self.symbol]
                    if all(k in d for k in ("H", "L", "C")):
                        return {"H": float(d["H"]), "L": float(d["L"]), "C": float(d["C"])}
                if isinstance(data, dict) and all(k in data for k in ("H", "L", "C")):
                    return {"H": float(data["H"]), "L": float(data["L"]), "C": float(data["C"])}
        except Exception:
            pass

        return self._calculate_hlc_from_ticks()

    def _calculate_hlc_from_ticks(self) -> dict[str, float]:
        """
        HLC за ~24 часа на основе тикового потока.
        Поддержка legacy + JSON-in-data.
        """
        try:
            now_ms = get_ny_time_millis()
            cutoff_ms = now_ms - 24 * 60 * 60 * 1000
            min_id = f"{cutoff_ms}-0"

            prices: list[float] = []
            last_price: float | None = None

            ticks = self.redis_ticks.xrevrange(self.tick_stream, max="+", min=min_id, count=10000) or []
            for _, fields in ticks:
                bid = ask = None
                if "data" in fields:
                    try:
                        tick_json = json.loads(_to_str(fields.get("data")))
                        bid = float(tick_json.get("bid", 0))
                        ask = float(tick_json.get("ask", 0))
                    except Exception:
                        continue
                else:
                    try:
                        bid = float(fields.get("bid", 0))
                        ask = float(fields.get("ask", 0))
                    except Exception:
                        continue

                if not bid or not ask:
                    continue

                mid = (bid + ask) / 2.0
                if mid <= 0:
                    continue

                prices.append(mid)
                if last_price is None:
                    last_price = mid

            if not prices:
                return self._get_default_hlc()

            return {"H": max(prices), "L": min(prices), "C": float(last_price or prices[0])}
        except Exception:
            return self._get_default_hlc()

    def _get_default_hlc(self) -> dict[str, float]:
        return {"H": 1.0, "L": 0.9, "C": 0.95}

    # -------------------- ATR --------------------

    def _get_atr(self, price: float, ts: int) -> float:
        """
        IMPORTANT:
          We keep return type float for backward compatibility.
          ATR timestamp is exposed via:
            self._last_atr_ts_ms  (int | None) - refreshed on each call.
          This allows downstream gates to use ctx.of.atr_ts_ms without signature changes.
        """
        # Reset per-call ATR timestamp marker (prevents leaking previous value).
        with contextlib.suppress(Exception):
            self._last_atr_ts_ms = None

        # сбрасываем маркер на каждом вызове (в реале вы зовёте _get_atr много раз,
        # но bucket-boundary нас интересует "последний" источник)
        try:
            if "atr_fallback" in self._quality_flags_bucket:
                self._quality_flags_bucket.remove("atr_fallback")
            if "hlc_fallback" in self._quality_flags_bucket:
                self._quality_flags_bucket.remove("hlc_fallback")
        except Exception:
            pass

        atr_source = os.getenv("ATR_SOURCE", "auto").lower()
        atr_tf_env = os.getenv("ATR_TF", "1m")
        atr_tf_cache = atr_tf_env.lower()
        timeframe = self._normalize_timeframe(atr_tf_env)

        # internal: фиксируем происхождение ATR (чтобы hlc_fallback корректно переживал cache-hit)
        if not hasattr(self, "_atr_source_cache"):
            with contextlib.suppress(Exception):
                self._atr_source_cache = {}
        src_cache = getattr(self, "_atr_source_cache", None) or {}
        src_key = (getattr(self, "symbol", ""), atr_tf_cache)

        cached = self._atr_cache.get_atr(self.symbol, atr_tf_cache)
        if cached is not None:
            origin = src_cache.get(src_key) if isinstance(src_cache, dict) else None
            with contextlib.suppress(Exception):
                self._last_hlc_fallback_used = (atr_source == "auto" and origin == "redis")
            return cached

        # auto/local: prefer tick-fed ATR
        if atr_source in {"auto", "local"}:
            try:
                v_local = self.atr_calculator.value()
            except Exception:
                v_local = None
            if v_local and v_local > 0:
                # Tick-fed ATR: its "timestamp" is effectively now/current bucket.
                # This is good enough for staleness gates (it should never be stale).
                with contextlib.suppress(Exception):
                    self._last_atr_ts_ms = int(ts) if ts else None
                self._atr_cache.set_atr(self.symbol, atr_tf_cache, v_local)
                if isinstance(src_cache, dict):
                    src_cache[src_key] = "local"
                with contextlib.suppress(Exception):
                    self._last_hlc_fallback_used = False
                return v_local

        # redis/auto: candles-derived ATR from Redis
        if atr_source in {"redis", "auto"}:
            v_redis, atr_ts = self._load_tracker_atr_from_redis_with_ts(timeframe, ts)
            if v_redis is None and atr_source == "redis":
                v_redis = self._load_legacy_atr_from_redis()
                atr_ts = None  # Legacy doesn't have timestamp
            if v_redis is not None and v_redis > 0:
                # Persist timestamp for downstream quality gates (ATR staleness).
                try:
                    self._last_atr_ts_ms = int(atr_ts) if atr_ts else None
                except Exception:
                    self._last_atr_ts_ms = None
                self._atr_cache.set_atr(self.symbol, atr_tf_cache, v_redis)
                if isinstance(src_cache, dict):
                    src_cache[src_key] = "redis"
                # atrh:* в вашем проекте заполняется candles-worker => помечаем hlc_fallback
                # (если не хотите считать это fallback — переименуйте флаг, но смысл 4.1 сохранится)
                with contextlib.suppress(Exception):
                    self._quality_flags_bucket.add("hlc_fallback")
                with contextlib.suppress(Exception):
                    self._last_hlc_fallback_used = (atr_source == "auto")
                return v_redis

        # fallback estimate => ухудшаем качество данных
        with contextlib.suppress(Exception):
            self._quality_flags_bucket.add("atr_fallback")
        # Estimated ATR has no reliable candle timestamp.
        with contextlib.suppress(Exception):
            self._last_atr_ts_ms = None
        v_est = self._estimate_atr(price)
        self._atr_cache.set_atr(self.symbol, atr_tf_cache, v_est)
        if isinstance(src_cache, dict):
            src_cache[src_key] = "estimate"
        with contextlib.suppress(Exception):
            self._last_hlc_fallback_used = False
        return v_est

    def _get_atr_with_ts(self, price: float, ts: int) -> tuple[float, int | None]:
        """
        Get ATR value and its timestamp.

        Returns:
          (atr_value, atr_ts_ms)
        """
        # сбрасываем маркер на каждом вызове (в реале вы зовёте _get_atr много раз,
        # но bucket-boundary нас интересует "последний" источник)
        try:
            if "atr_fallback" in self._quality_flags_bucket:
                self._quality_flags_bucket.remove("atr_fallback")
            if "hlc_fallback" in self._quality_flags_bucket:
                self._quality_flags_bucket.remove("hlc_fallback")
        except Exception:
            pass

        atr_source = os.getenv("ATR_SOURCE", "auto").lower()
        atr_tf_env = os.getenv("ATR_TF", "1m")
        atr_tf_cache = atr_tf_env.lower()
        timeframe = self._normalize_timeframe(atr_tf_env)

        # internal: фиксируем происхождение ATR (чтобы hlc_fallback корректно переживал cache-hit)
        if not hasattr(self, "_atr_source_cache"):
            with contextlib.suppress(Exception):
                self._atr_source_cache = {}
        src_cache = getattr(self, "_atr_source_cache", None) or {}
        src_key = (getattr(self, "symbol", ""), atr_tf_cache)

        cached = self._atr_cache.get_atr(self.symbol, atr_tf_cache)
        if cached is not None:
            origin = src_cache.get(src_key) if isinstance(src_cache, dict) else None
            with contextlib.suppress(Exception):
                self._last_hlc_fallback_used = (atr_source == "auto" and origin == "redis")
            # For cached values, we don't have timestamp, so return None for ts
            return cached, None

        # auto/local: prefer tick-fed ATR
        if atr_source in {"auto", "local"}:
            try:
                v_local = self.atr_calculator.value()
            except Exception:
                v_local = None
            if v_local and v_local > 0:
                self._atr_cache.set_atr(self.symbol, atr_tf_cache, v_local)
                if isinstance(src_cache, dict):
                    src_cache[src_key] = "local"
                with contextlib.suppress(Exception):
                    self._last_hlc_fallback_used = False
                return v_local, None  # Local ATR doesn't have timestamp

        # redis/auto: candles-derived ATR from Redis
        if atr_source in {"redis", "auto"}:
            v_redis, ts_redis = self._load_tracker_atr_from_redis_with_ts(timeframe, ts)
            if v_redis is None and atr_source == "redis":
                v_redis = self._load_legacy_atr_from_redis()
                ts_redis = None  # Legacy doesn't have timestamp
            if v_redis is not None and v_redis > 0:
                self._atr_cache.set_atr(self.symbol, atr_tf_cache, v_redis)
                if isinstance(src_cache, dict):
                    src_cache[src_key] = "redis"
                # atrh:* в вашем проекте заполняется candles-worker => помечаем hlc_fallback
                # (если не хотите считать это fallback — переименуйте флаг, но смысл 4.1 сохранится)
                with contextlib.suppress(Exception):
                    self._quality_flags_bucket.add("hlc_fallback")
                with contextlib.suppress(Exception):
                    self._last_hlc_fallback_used = (atr_source == "auto")
                return v_redis, ts_redis

        # fallback estimate => ухудшаем качество данных
        with contextlib.suppress(Exception):
            self._quality_flags_bucket.add("atr_fallback")
        v_est = self._estimate_atr(price)
        self._atr_cache.set_atr(self.symbol, atr_tf_cache, v_est)
        if isinstance(src_cache, dict):
            src_cache[src_key] = "estimate"
        return v_est, None

    def _estimate_atr(self, price: float) -> float:
        return price * 0.0003

    def _dq_flags(self, ctx: Any) -> list[str]:
        """Гарантирует наличие ctx.data_quality_flags как списка."""
        flags = getattr(ctx, "data_quality_flags", None)
        if flags is None:
            flags = []
            with contextlib.suppress(Exception):
                ctx.data_quality_flags = flags
        # если кто-то положил tuple/str — нормализуем мягко
        if not isinstance(flags, list):
            flags = list(flags) if flags else []
            with contextlib.suppress(Exception):
                ctx.data_quality_flags = flags
        return flags

    def _mark_hlc_fallback_flag(self, ctx: Any) -> None:
        """4.1: candles fallback -> ctx.data_quality_flags += ['hlc_fallback'] (только когда это именно fallback)."""
        if getattr(self, "_last_hlc_fallback_used", False):
            flags = self._dq_flags(ctx)
            if "hlc_fallback" not in flags:
                flags.append("hlc_fallback")

    def _mark_l3_missing_policy(self, ctx: Any) -> None:
        """4.1: L3 недоступен -> не veto, l3_score=0.5 + метрика l3_missing_rate."""
        # lazy counters
        if not hasattr(self, "_l3_seen"):
            self._l3_seen = 0
            self._l3_missing = 0
        self._l3_seen += 1
        self._l3_missing += 1
        try:
            ctx.l3_score01 = 0.5
            ctx.l3_missing_rate = float(self._l3_missing) / float(max(self._l3_seen, 1))
        except Exception:
            pass
        flags = self._dq_flags(ctx)
        if "l3_missing" not in flags:
            flags.append("l3_missing")

    def _load_tracker_atr_from_redis(self, timeframe: str, current_ts: int) -> float | None:
        val, _ts = self._load_tracker_atr_from_redis_with_ts(timeframe, current_ts)
        return val

    def _load_tracker_atr_from_redis_with_ts(self, timeframe: str, current_ts: int) -> tuple[float | None, int | None]:
        """
        ATR tracker reader with timestamp.
        Reads:
          key = ATR:{symbol}:{timeframe}
          fields: atr, lastCloseTime
        Returns:
          (atr_value, atr_ts_ms)
        Applies the same staleness rule as legacy:
          current_ts - lastCloseTime <= timeframe_ms * ATR_REDIS_STALENESS_MULT
        """
        if not timeframe:
            return None, None
        key = f"ATR:{self.symbol}:{timeframe}"
        val, atr_ts_ms, logged = load_tracker_atr_from_redis_hmget(
            redis_client=self.redis,
            key=key,
            timeframe=timeframe,
            current_ts=int(current_ts) if current_ts else 0,
            timeframe_to_ms_fn=self._timeframe_to_ms,
            logger=self.logger,
            warning_logged=bool(getattr(self, "_redis_atr_warning_logged", False)),
        )
        with contextlib.suppress(Exception):
            self._redis_atr_warning_logged = bool(logged)
        return val, atr_ts_ms

    def _load_tracker_atr_from_redis_with_ts(self, timeframe: str, current_ts: int) -> tuple[float | None, int | None]:
        """
        ATR tracker reader with timestamp.
        Reads:
          key = ATR:{symbol}:{timeframe}
          fields: atr, lastCloseTime
        Returns:
          (atr_value, atr_ts_ms)
        Applies the same staleness rule as legacy:
          current_ts - lastCloseTime <= timeframe_ms * ATR_REDIS_STALENESS_MULT
        """
        if not timeframe:
            return None, None
        key = f"ATR:{self.symbol}:{timeframe}"
        val, atr_ts_ms, logged = load_tracker_atr_from_redis_hmget(
            redis_client=self.redis,
            key=key,
            timeframe=timeframe,
            current_ts=int(current_ts) if current_ts else 0,
            timeframe_to_ms_fn=self._timeframe_to_ms,
            logger=self.logger,
            warning_logged=bool(getattr(self, "_redis_atr_warning_logged", False)),
        )
        with contextlib.suppress(Exception):
            self._redis_atr_warning_logged = bool(logged)
        return val, atr_ts_ms

    def _load_legacy_atr_from_redis(self) -> float | None:
        try:
            raw = self.redis.get(f"ta:last:atr:{self.symbol}")
            if not raw:
                return None
            data = json.loads(_to_str(raw))
            v = float(data.get("atr", 0))
            return v if v > 0 else None
        except Exception:
            return None

    def _normalize_timeframe(self, tf: str) -> str:
        m = {
            "1m": "M1", "m1": "M1",
            "5m": "M5", "m5": "M5",
            "15m": "M15", "m15": "M15",
            "30m": "M30", "m30": "M30",
            "1h": "H1", "h1": "H1",
            "4h": "H4", "h4": "H4",
            "1d": "D1", "d1": "D1",
        }
        tf0 = (tf or "1m").strip().lower()
        return m.get(tf0, (tf or "M1").strip().upper())

    def _timeframe_to_ms(self, tf: str) -> int:
        m = {
            "M1": 60_000, "M5": 300_000, "M15": 900_000, "M30": 1_800_000,
            "H1": 3_600_000, "H4": 14_400_000, "D1": 86_400_000,
        }
        return m.get(tf, 180_000)

    # -------------------- signal generation --------------------

    def _cooldown_ok(self, kind: str, level_key: str, ts: int) -> bool:
        if self.level_cooldown_ms <= 0:
            return True
        if not level_key or level_key == "na":
            return True
        k = (kind, level_key)
        last = self._last_level_signal_ts.get(k)
        if last and (ts - last) < self.level_cooldown_ms:
            return False
        return True

    def _mark_cooldown(self, kind: str, level_key: str, ts: int) -> None:
        if self.level_cooldown_ms <= 0:
            return
        if not level_key or level_key == "na":
            return
        self._last_level_signal_ts[(kind, level_key)] = ts

    def _burst_gate_ok(self, ctx: OrderflowContext) -> bool:
        """Проверка quality gate для burstiness метрик."""
        # imbalance proxy: OBI avg (лучше 20, иначе 5)
        obi_avg_used = float(ctx.obi_avg_20 or 0.0)
        if obi_avg_used == 0.0:
            obi_avg_used = float(ctx.obi_avg or 0.0)
        imbalance = abs(obi_avg_used)

        return (
            int(ctx.burst_trade_count_bucket or 0) >= self.min_trades_breakout
            and float(ctx.burst_ratio or 0.0) >= self.burst_ratio_min
            and float(ctx.burst_fano_counts or 0.0) >= self.fano_min
            and float(ctx.burst_flip_ratio or 0.0) <= self.flip_ratio_max
            and float(imbalance) >= self.imbalance_min
        )

    def _exec_quality_ok(self, ctx: OrderflowContext, impulse_side: str) -> bool:
        """
        Execution-quality фильтр: burstiness + OBI(20) + ETA.
        
        Проверяет, что рынок имеет достаточную активность, импульсность,
        имбаланс в сторону сигнала и разумное время до заполнения depth.
        """
        if not self.exec_filters_enabled:
            return True

        # Нужен свежий L2, иначе OBI/ETA невалидны
        if bool(getattr(ctx, "l2_is_stale", True)):
            return False

        # 1) Burstiness: достаточная активность и "импульсность", без churn
        if int(getattr(ctx, "burst_trade_count_bucket", 0)) < self.min_trades_breakout:
            return False
        if float(getattr(ctx, "burst_ratio", 0.0)) < self.burst_ratio_min:
            return False
        if float(getattr(ctx, "burst_fano_counts", 0.0)) < self.fano_min:
            return False
        if float(getattr(ctx, "burst_flip_ratio", 1.0)) > self.flip_ratio_max:
            return False

        # 2) OBI(20): имбаланс в сторону импульса
        obi20_avg = float(getattr(ctx, "obi_avg_20", 0.0) or 0.0)
        obi20_sus = bool(getattr(ctx, "obi_sustained_20", False))

        if (not obi20_sus) or (abs(obi20_avg) < self.imbalance_min):
            return False
        if impulse_side == "LONG" and obi20_avg <= 0:
            return False
        if impulse_side == "SHORT" and obi20_avg >= 0:
            return False

        # 3) ETA: "есть поток, который способен съесть ближайшую глубину"
        # LONG -> едим ask; SHORT -> едим bid
        eta_key = "eta_fill_ask_sec" if impulse_side == "LONG" else "eta_fill_bid_sec"
        eta = float(getattr(ctx, eta_key, 0.0) or 0.0)
        if eta <= 0:
            return False
        if eta > self.eta_max_sec:
            return False

        return True

    def _generate_signals(self, ctx: OrderflowContext) -> bool:
        """
        DEPRECATED: Legacy method for backward compatibility.
        Now delegates to UnifiedSignalPipeline for all signal generation logic.
        """
        if self._unified_pipeline is None:
            # Fallback to old logic if no pipeline provided
            self.logger.warning("No unified pipeline configured, skipping signal generation")
            return False

        # Delegate to unified pipeline
        signal = self._unified_pipeline.process(ctx)
        return signal is not None

    def _custom_signal_conditions(self, ctx: OrderflowContext) -> dict[str, Any] | None:
        return None

    def _apply_experiment_filters(self, ctx: OrderflowContext, signal_type: str) -> bool:
        """
        Apply experiment-specific filtering logic.

        For control groups: apply baseline filters only
        For treatment groups: apply baseline + experimental filters

        Returns True if signal should be allowed to pass.
        """
        if not ctx.experiment_variant:
            # No active experiment - apply baseline logic only
            return self._apply_baseline_filters(ctx, signal_type)

        if ctx.experiment_variant == "control":
            # Control group: baseline filters only
            ctx.filter_flags["baseline_passed"] = self._apply_baseline_filters(ctx, signal_type)
            return ctx.filter_flags["baseline_passed"]

        elif ctx.experiment_variant == "treatment":
            # Treatment group: baseline + experimental filters
            baseline_passed = self._apply_baseline_filters(ctx, signal_type)
            ctx.filter_flags["baseline_passed"] = baseline_passed

            if not baseline_passed:
                return False

            # Apply experimental filter based on experiment config
            experimental_passed = self._apply_experimental_filters(ctx, signal_type)
            filter_name = ctx.experiment_config.get("filter_name", "experimental_filter")
            ctx.filter_flags[f"{filter_name}_passed"] = experimental_passed

            return experimental_passed

        # Unknown variant - default to baseline
        ctx.filter_flags["baseline_passed"] = self._apply_baseline_filters(ctx, signal_type)
        return ctx.filter_flags["baseline_passed"]

    def _apply_baseline_filters(self, ctx: OrderflowContext, signal_type: str) -> bool:
        """
        Apply baseline filtering logic (existing filters).
        This is the current production logic.
        """
        # For now, delegate to existing confidence and quality checks
        # This can be expanded based on specific baseline requirements
        return True  # Placeholder - actual logic depends on signal type

    def _apply_experimental_filters(self, ctx: OrderflowContext, signal_type: str) -> bool:
        """
        Apply experimental filtering logic based on experiment config.
        This is where new filters/features are tested.
        """
        config = ctx.experiment_config

        # Example experimental filters - can be extended based on experiment needs
        if "confidence_threshold" in config:
            min_conf = config["confidence_threshold"]
            if hasattr(ctx, 'confidence') and ctx.confidence < min_conf:
                return False

        if "z_threshold_multiplier" in config:
            multiplier = config["z_threshold_multiplier"]
            # Apply modified z-threshold for experimental group
            experimental_z_threshold = self.main_z_threshold * multiplier
            z_abs = abs(ctx.z_delta)
            if z_abs < experimental_z_threshold:
                return False

        if "require_weak_progress" in config and config["require_weak_progress"]:
            if not getattr(ctx, 'weak_progress', False):
                return False

        # Add more experimental filters as needed...

        return True  # All experimental filters passed

    def _touch_dbg(self, ctx: OrderflowContext) -> str:
        """Форматирует touch-метрики для debug/audit логов."""
        try:
            stale = bool(getattr(ctx, "touch_is_stale", True))
            btag = str(getattr(ctx, "touch_bid_tag", "none"))
            atag = str(getattr(ctx, "touch_ask_tag", "none"))
            brho = float(getattr(ctx, "touch_bid_rho", 0.0))
            arho = float(getattr(ctx, "touch_ask_rho", 0.0))
            bT = float(getattr(ctx, "touch_bid_traded_w", 0.0))
            aT = float(getattr(ctx, "touch_ask_traded_w", 0.0))
            bD = float(getattr(ctx, "touch_bid_drop_w", 0.0))
            aD = float(getattr(ctx, "touch_ask_drop_w", 0.0))

            return (
                f"touch(stale={int(stale)} "
                f"bid:{btag} rho={brho:.2f} T={bT:.3f} D={bD:.3f} | "
                f"ask:{atag} rho={arho:.2f} T={aT:.3f} D={aD:.3f})"
            )
        except Exception:
            return "touch(err)"

    def _ctx_l2_debug(self, ctx: OrderflowContext) -> dict[str, Any]:
        """
        Полный набор micro + L2 + L3-lite полей для indicators и audit_payload.
        """
        return {
            # L2 staleness
            "l2_ts": int(getattr(ctx, "l2_ts", 0) or 0),
            "l2_age_ms": int(getattr(ctx, "l2_age_ms", 0) or 0),
            "l2_is_stale": bool(getattr(ctx, "l2_is_stale", True)),

            # Microstructure
            "spread_bps": round(float(getattr(ctx, "spread_bps", 0.0) or 0.0), 3),
            "realized_bps": round(float(getattr(ctx, "realized_bps", 0.0) or 0.0), 3),
            "realized_ema_bps": round(float(getattr(ctx, "realized_ema_bps", 0.0) or 0.0), 3),
            "adverse_ratio_ema": round(float(getattr(ctx, "adverse_ratio_ema", 0.0) or 0.0), 3),
            "market_mode": str(getattr(ctx, "market_mode", "mixed")),

            # L2
            "obi_20": round(float(getattr(ctx, "obi_20", 0.0) or 0.0), 4),
            "obi_avg_20": round(float(getattr(ctx, "obi_avg_20", 0.0) or 0.0), 4),
            "obi_sustained_20": bool(getattr(ctx, "obi_sustained_20", False)),
            "microprice_shift_bps_20": round(float(getattr(ctx, "microprice_shift_bps_20", 0.0) or 0.0), 3),
            "wall_bid": bool(getattr(ctx, "wall_bid", False)),
            "wall_ask": bool(getattr(ctx, "wall_ask", False)),
            "wall_bid_dist_bps": round(float(getattr(ctx, "wall_bid_dist_bps", 0.0) or 0.0), 3),
            "wall_ask_dist_bps": round(float(getattr(ctx, "wall_ask_dist_bps", 0.0) or 0.0), 3),
            "refill_score": round(float(getattr(ctx, "refill_score", 0.0) or 0.0), 4),
            "depletion_score": round(float(getattr(ctx, "depletion_score", 0.0) or 0.0), 4),
            "impact_proxy": round(float(getattr(ctx, "impact_proxy", 0.0) or 0.0), 4),
            "depth_bid_5": round(float(getattr(ctx, "depth_bid_5", 0.0) or 0.0), 6),
            "depth_ask_5": round(float(getattr(ctx, "depth_ask_5", 0.0) or 0.0), 6),
            "depth_bid_20": round(float(getattr(ctx, "depth_bid_20", 0.0) or 0.0), 6),
            "depth_ask_20": round(float(getattr(ctx, "depth_ask_20", 0.0) or 0.0), 6),

            # L3-lite (queue-events proxy)
            "taker_buy_qty_bucket": round(float(getattr(ctx, "taker_buy_qty_bucket", 0.0) or 0.0), 6),
            "taker_sell_qty_bucket": round(float(getattr(ctx, "taker_sell_qty_bucket", 0.0) or 0.0), 6),
            "taker_buy_rate_ema": round(float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0), 6),
            "taker_sell_rate_ema": round(float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0), 6),
            "pull_ask_qty_proxy": round(float(getattr(ctx, "pull_ask_qty_proxy", 0.0) or 0.0), 6),
            "pull_bid_qty_proxy": round(float(getattr(ctx, "pull_bid_qty_proxy", 0.0) or 0.0), 6),
            "cancel_to_trade_bid": round(float(getattr(ctx, "cancel_to_trade_bid", 0.0) or 0.0), 4),
            "cancel_to_trade_ask": round(float(getattr(ctx, "cancel_to_trade_ask", 0.0) or 0.0), 4),
            "cancel_bid_rate_ema": round(float(getattr(ctx, "cancel_bid_rate_ema", 0.0) or 0.0), 4),
            "cancel_ask_rate_ema": round(float(getattr(ctx, "cancel_ask_rate_ema", 0.0) or 0.0), 4),
            "eta_fill_bid_sec": round(float(getattr(ctx, "eta_fill_bid_sec", 0.0) or 0.0), 3),
            "eta_fill_ask_sec": round(float(getattr(ctx, "eta_fill_ask_sec", 0.0) or 0.0), 3),

            # Burstiness metrics
            "burst_trade_count_bucket": int(getattr(ctx, "burst_trade_count_bucket", 0) or 0),
            "burst_rate_short": round(float(getattr(ctx, "burst_rate_short", 0.0) or 0.0), 6),
            "burst_rate_long": round(float(getattr(ctx, "burst_rate_long", 0.0) or 0.0), 6),
            "burst_ratio": round(float(getattr(ctx, "burst_ratio", 0.0) or 0.0), 4),
            "burst_cv_dt": round(float(getattr(ctx, "burst_cv_dt", 0.0) or 0.0), 4),
            "burst_fano_counts": round(float(getattr(ctx, "burst_fano_counts", 0.0) or 0.0), 4),
            "burst_flip_ratio": round(float(getattr(ctx, "burst_flip_ratio", 0.0) or 0.0), 4),

            # Touch-level metrics (v2 simplified)
            "touch_bid_tag": str(getattr(ctx, "touch_bid_tag", "none")),
            "touch_ask_tag": str(getattr(ctx, "touch_ask_tag", "none")),
            "touch_bid_rho": round(float(getattr(ctx, "touch_bid_rho", 0.0) or 0.0), 4),
            "touch_ask_rho": round(float(getattr(ctx, "touch_ask_rho", 0.0) or 0.0), 4),
            "touch_bid_traded_w": round(float(getattr(ctx, "touch_bid_traded_w", 0.0) or 0.0), 6),
            "touch_ask_traded_w": round(float(getattr(ctx, "touch_ask_traded_w", 0.0) or 0.0), 6),
            "touch_bid_drop_w": round(float(getattr(ctx, "touch_bid_drop_w", 0.0) or 0.0), 6),
            "touch_ask_drop_w": round(float(getattr(ctx, "touch_ask_drop_w", 0.0) or 0.0), 6),
            "touch_is_stale": bool(getattr(ctx, "touch_is_stale", True)),
        }

    def _nearest_pivot_key(self, price: float, pivots: dict[str, float]) -> str:
        if not pivots:
            return "na"
        best_k = "na"
        best_d = 1e18
        for k, lvl in pivots.items():
            try:
                d = abs(float(lvl) - price)
            except Exception:
                continue
            if d < best_d:
                best_d = d
                best_k = str(k)
        return best_k

    def _breakout_cross_info(self, price: float, up: bool, pivots: dict[str, float]) -> str | None:
        """
        Проверяет пересечение pivot уровня между предыдущей оценкой (bucket boundary) и текущей.
        """
        prev = self._prev_eval_price
        if prev is None or not pivots:
            return None

        keys = ["R1", "R2", "R3"] if up else ["S1", "S2", "S3"]
        for k in keys:
            lvl = pivots.get(k)
            if lvl is None:
                continue
            try:
                lvl_f = float(lvl)
            except Exception:
                continue
            if up and prev <= lvl_f < price:
                return k
            if (not up) and prev >= lvl_f > price:
                return k
        return None

    # -------------------- publishing --------------------

    def _extend_outbox_envelope(self, envelope: dict[str, Any], signal: Signal, ctx: OrderflowContext) -> None:
        """
        Hook для наследников: расширение envelope (например crypto->manual-signals).
        """
        return

    def _compute_confidence(
        self,
        ctx: OrderflowContext,
        signal_type: str,
    ) -> tuple[float | None, dict[str, float] | None]:
        """
        Универсальный хук для вычисления confidence с использованием нового SignalScoringEngine.

        - Сначала обновляем режим рынка
        - Используем SignalScoringEngine для комплексного скоринга
        - Применяем фильтр should_emit
        """
        # NEW: обновляем режим рынка перед скорингом
        self._update_market_regime(ctx)

        # NEW: hook для наследников — обновление геометрии/ликвидности
        self._update_geometry_liquidity_context(ctx, ctx.last_price or 0.0, ctx.ts)

        # NEW: используем SignalScoringEngine для комплексного скоринга
        try:
            # Создаем SignalContext для скоринга
            scoring_ctx = ScoringSignalContext(
                ts=ctx.ts_utc,
                symbol=ctx.symbol,
                side=ctx.side,
                session=ctx.session or "",
                regime=ctx.regime_label or "mixed",
                pattern_name=signal_type,
                delta_spike_z=ctx.deltaSpikeZ,
                obi=ctx.obi,
                weak_progress=ctx.weakProgress,
                atr_quantile=ctx.atr_quantile,
                # Additional fields for fade patterns
                reverse_delta_spike_z=getattr(ctx, 'reverseDeltaSpikeZ', None),
                volume_z=getattr(ctx, 'volumeZ', None),
            )

            # Вычисляем confidence через новый engine
            confidence = self._scoring_engine.compute_confidence(scoring_ctx)

            # NEW: Создаем ExecutionPlan если confidence достаточно высокий
            if confidence >= (ctx.min_confidence_used or 80):
                try:
                    execution_plan = self._create_execution_plan(scoring_ctx)
                    if execution_plan:
                        # Сохраняем план в контексте для передачи в payload
                        ctx.execution_plan = execution_plan
                        # Можно сразу сохранить в базу данных
                        self._save_execution_plan(execution_plan)
                except Exception:
                    self.logger.exception("Failed to create execution plan")

            # Копируем результаты обратно в ctx
            ctx.confidence = scoring_ctx.confidence
            ctx.min_confidence_used = scoring_ctx.min_confidence_used
            ctx.is_golden_pattern = scoring_ctx.is_golden_pattern
            ctx.golden_pattern_label = scoring_ctx.golden_pattern_label
            ctx.delta_spike_z_local_q = scoring_ctx.delta_spike_z_local_q
            ctx.obi_local_q = scoring_ctx.obi_local_q
            ctx.weak_progress_local_q = scoring_ctx.weak_progress_local_q
            ctx.atr_local_q = scoring_ctx.atr_local_q

            # Создаем breakdown для совместимости
            breakdown = {
                "confidence": confidence,
                "min_confidence_used": ctx.min_confidence_used or 0,
                "delta_spike_z_local_q": ctx.delta_spike_z_local_q or 0.0,
                "obi_local_q": ctx.obi_local_q or 0.0,
                "weak_progress_local_q": ctx.weak_progress_local_q or 0.0,
                "atr_local_q": ctx.atr_local_q or 0.0,
                "is_golden_pattern": ctx.is_golden_pattern,
            }

            return confidence, breakdown

        except Exception:
            self.logger.exception("Failed to compute confidence via SignalScoringEngine")
            return None, None

    def _publish_signal(
        self,
        label: str,
        side: str,
        signal_type: str,
        strength: float,
        reason: str,
        ctx: OrderflowContext,
        confidence_value: float | None = None,
        entry_tag: str = "",
    ) -> PublishResult:
        # ---- build signal ----
        # Сначала вычисляем SL/TP levels
        levels = compute_levels(ctx.price, ctx.atr, side, {
            "STOP_MODE": self.config.stop_mode,
            "STOP_ATR_MULT": self.config.stop_atr_mult,
            "STOP_PCT": self.config.stop_pct,
            "STOP_POINTS": self.config.stop_points,
            "TP_MODE": self.config.tp_mode,
            "TP_RR": self.config.tp_rr,
            "TP_ATR_MULTS": self.config.tp_atr_mults,
        }, symbol=ctx.symbol)

        # Сдвигаем TP дальше: TP1 по умолчанию +40% к дистанции, TP2/TP3 без изменений (можно задать ENV)
        try:
            tp1_shift = float(os.getenv("TP1_SHIFT_MULT", "1.4"))
        except Exception:
            tp1_shift = 1.4
        try:
            tp2_shift = float(os.getenv("TP2_SHIFT_MULT", "1.0"))
        except Exception:
            tp2_shift = 1.0
        try:
            tp3_shift = float(os.getenv("TP3_SHIFT_MULT", "1.0"))
        except Exception:
            tp3_shift = 1.0

        tps = list(levels.get("tp_levels", []))
        if len(tps) >= 1:
            tps[0] = ctx.price + (tps[0] - ctx.price) * tp1_shift
        if len(tps) >= 2:
            tps[1] = ctx.price + (tps[1] - ctx.price) * tp2_shift
        if len(tps) >= 3:
            tps[2] = ctx.price + (tps[2] - ctx.price) * tp3_shift
        levels["tp_levels"] = tps

        # ✅ Рассчитываем lot на основе риска (после того как SL определен)
        # Для крипты возвращается (lot, position_size_usd, deposit, leverage)
        lot, position_size_usd, deposit, leverage = calculate_position_size(
            symbol=self.symbol,
            entry_price=ctx.price,
            sl_price=levels["sl"],
            side=side,
            redis_client=self.redis,
        )
        if lot <= 0:
            self.logger.warning(
                "🚫 [VETO] (%s) Risk veto: lot=0 (sl_floor/fee_risk). entry=%.8f sl=%.8f",
                self.symbol, ctx.price, levels["sl"],
            )
            return PublishResult(sent=False, dedup=False)

        """
        Финальный publish (Redis / Kafka / что угодно).

        Логика по confidence:
          - для crypto:
              * если фильтр выключен → conf None превращаем в 100.0
              * если включен → режем сигналы без conf или ниже CRYPTO_SIGNAL_MIN_CONF
          - для не-crypto → ведём себя как раньше (можно расширить).
        """
        symbol_up = self.symbol.upper()
        is_crypto = symbol_up.endswith(("USDT", "USDC", "USD", "BUSD"))

        # Специальный фильтр для : более мягкий confidence
        if is_crypto and symbol_up.startswith("XAU"):
            min_conf = 20.0
        else:
            min_conf = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "30.0"))

        # Приоритет: атрибут инстанса
        disable_conf_attr = getattr(self, "disable_conf_filter", None)
        if disable_conf_attr is not None:
            disable_conf_filter = bool(disable_conf_attr)
        else:
            # fallback: env
            disable_conf_filter = (
                os.getenv("DISABLE_CONFIDENCE_FILTER", "false").lower() == "true"
            )

        # Regime guard: проверка состояния сигнала и черных зон
        try:
            from datetime import datetime
            now = datetime.now(UTC)

            # 1) Проверка черных зон по новостям
            mode = self.black_zone_scheduler.mode_for(
                now=now,
                venue=ctx.venue,
                symbol=ctx.symbol,
                family=ctx.family,
                timeframe=ctx.timeframe,
            )
            if mode == "blocked":
                self.logger.info(
                    "🚫 Signal blocked by news blackzone (venue=%s, symbol=%s, family=%s)",
                    ctx.venue, ctx.symbol, ctx.family
                )
                return PublishResult(sent=False, dedup=True)

            # 2) Проверка состояния режима сигнала
            status, threshold_mult = self.regime_runtime.get_state(
                venue=ctx.venue,
                symbol=ctx.symbol,
                timeframe=ctx.timeframe,
                family=ctx.family,
            )
            if status == "disabled":
                self.logger.info(
                    "🚫 Signal disabled by regime guard (venue=%s, symbol=%s, family=%s)",
                    ctx.venue, ctx.symbol, ctx.family
                )
                return PublishResult(sent=False, dedup=True)

            # Применяем множитель порогов для degraded режима
            if status == "degraded":
                min_conf *= threshold_mult
                self.logger.debug(
                    "⚠️ Signal threshold adjusted by regime guard: min_conf *= %.2f (venue=%s, symbol=%s, family=%s)",
                    threshold_mult, ctx.venue, ctx.symbol, ctx.family
                )

            # Применяем stricter режим для новостей
            if mode == "strict":
                min_conf *= 1.5
                self.logger.debug(
                    "⚠️ Signal threshold adjusted by news mode: min_conf *= 1.5 (venue=%s, symbol=%s, family=%s)",
                    ctx.venue, ctx.symbol, ctx.family
                )

        except Exception as e:
            self.logger.warning("Regime guard check failed: %s", e)
            # Продолжаем обработку при ошибке

        if is_crypto:
            if disable_conf_filter:
                # Фильтр выключен: если ничего не посчитали — считаем 100
                if confidence_value is None:
                    confidence_value = 100.0
            else:
                # Фильтр включен
                if confidence_value is None:
                    self.logger.info(
                        "🚫 Signal filtered by confidence: conf missing < min_conf=%.2f (symbol=%s)",
                        min_conf,
                        self.symbol,
                    )
                    return PublishResult(sent=False, dedup=True)

                if confidence_value < min_conf:
                    self.logger.info(
                        "🚫 Signal filtered by confidence: conf=%.2f < min_conf=%.2f (symbol=%s)",
                        confidence_value,
                        min_conf,
                        self.symbol,
                    )
                    return PublishResult(sent=False, dedup=True)

        # Финальное значение, которое уйдёт в payload
        final_conf = float(confidence_value) if confidence_value is not None else 0.0

        # ctx → dict
        if hasattr(ctx, "to_dict"):
            ctx_dict: dict[str, Any] = ctx.to_dict()
        else:
            ctx_dict = ctx.__dict__.copy()

        # Синхронизируем поля в ctx_dict
        ctx_dict["confidence"] = final_conf
        ctx_dict.setdefault(
            "confidence_breakdown",
            getattr(ctx, "confidence_breakdown", {}),
        )

        # Collect all signal generation settings
        signal_settings = {
            # Z-score thresholds
            "breakoutZThreshold": self.breakout_z_threshold,
            "absorptionZThreshold": self.absorption_z_threshold,
            "extremeZMult": self.extreme_z_mult,
            "extremeZThreshold": self.extreme_z_threshold,
            "mainZThreshold": self.main_z_threshold,

            # OBI settings
            "obiSustainedMinSamples": self.obi_min_samples,
            "obiSustainedMinFraction": self.obi_min_fraction,
            "obiSustainedUseFraction": self.obi_use_fraction,

            # Delta bucket settings
            "deltaBucketMs": self.delta_bucket_ms,
            "deltaBucketMaxZeroFill": self.max_zero_buckets,

            # Burstiness settings
            "burstRatioMin": self.burst_ratio_min,
            "fanoMin": self.fano_min,
            "flipRatioMax": self.flip_ratio_max,
            "imbalanceMin": self.imbalance_min,
            "minTradesBreakout": self.min_trades_breakout,

            # Execution filters
            "execFiltersEnabled": self.exec_filters_enabled,
            "etaMaxSec": self.eta_max_sec,

            # Absorption settings
            "absorptionRequireWeakProgress": self.absorption_require_weak_progress,

            # Breakout settings
            "breakoutRequireObi": self.breakout_require_obi,
            "breakoutMinDistAtr": self.breakout_min_dist_atr,

            # Confidence settings
            "minSignalConfidence": min_conf,
            "disableConfidenceFilter": disable_conf_filter,

            # TP shift settings
            "tp1ShiftMult": tp1_shift,
            "tp2ShiftMult": tp2_shift,
            "tp3ShiftMult": tp3_shift,

            # Signal processing settings
            "signalDedupTtlMs": self.dedup_ttl_ms,
            "levelCooldownMs": self.level_cooldown_ms,
            "maxTickLagMs": self.max_tick_lag_ms,

            # GPU settings
            "gpuMinN": getattr(self, 'gpu_service', None) and getattr(self.gpu_service, 'gpu_min_n', 2048) or 2048,
            "robustBackend": self.robust_backend,
            "robustGpuMinN": GPU_MIN_N,

            # L2 settings
            "l2KSmall": self.l2_k_small,
            "l2KLarge": self.l2_k_large,
            "l2WallMult": self.l2_wall_mult,
            "l2WallMaxDistBps": self.l2_wall_max_dist_bps,
            "l2BatchSize": self._book_buffer_max,
            "l2BatchTimeoutMs": self._book_buffer_timeout_ms,

            # L3 settings
            "l3LiteEnabled": self.l3_lite_enabled,
            "l3LiteEmaAlpha": getattr(self, 'l3', None) and getattr(self.l3, 'alpha', 0.08) or 0.08,

            # Touch settings
            "touchWindowMs": getattr(self, 'touch', None) and getattr(self.touch, 'window_ms', 500) or 500,
            "touchTauRefillMs": getattr(self, 'touch', None) and getattr(self.touch, 'tau_refill_ms', 250) or 250,
            "touchRecoverFrac": getattr(self, 'touch', None) and getattr(self.touch, 'recover_frac', 0.90) or 0.90,
            "touchRhoRefillMin": getattr(self, 'touch', None) and getattr(self.touch, 'rho_refill_min', 1.5) or 1.5,
            "touchRhoDepletionMax": getattr(self, 'touch', None) and getattr(self.touch, 'rho_depletion_max', 1.5) or 1.5,

            # ATR settings
            "atrSource": os.getenv("ATR_SOURCE", "auto"),
            "atrTf": os.getenv("ATR_TF", "1m"),

            # Stop/Loss settings
            "stopMode": self.config.stop_mode,
            "stopAtrMult": self.config.stop_atr_mult,
            "stopPct": self.config.stop_pct,
            "stopPoints": self.config.stop_points,

            # Take Profit settings
            "tpMode": self.config.tp_mode,
            "tpRr": self.config.tp_rr,
            "tpAtrMults": self.config.tp_atr_mults,

            # Config settings
            "deltaWindowTicks": self.config.delta_window_ticks,
            "deltaZThreshold": self.config.delta_z_threshold,
            "obiThreshold": self.config.obi_threshold,
            "weakProgressAtr": self.config.weak_progress_atr,
            "minSignalIntervalSec": self.config.min_signal_interval_sec,
            "readCount": self.config.read_count,
            "readBlockMs": self.config.read_block_ms,
            "distAtrThreshold": self.config.dist_atr_threshold,
        }

        signal_payload: dict[str, Any] = {
            "symbol": self.symbol,
            "label": label,
            "side": side,
            "signal_type": signal_type,
            "strength": strength,
            "reason": reason,
            "ts": get_ny_time_millis(),
            "confidence": final_conf,
            "confidence_breakdown": ctx_dict.get("confidence_breakdown", {}),
            "entry_tag": entry_tag,
            "isGoldenPattern": getattr(ctx, "is_golden_pattern", False),
            "goldenPatternLabel": getattr(ctx, "golden_pattern_label", None),
            "patternWeight": getattr(ctx, "pattern_weight", 1.0),
            "baseScore": getattr(ctx, "base_score", 0.0),
            "finalScore": getattr(ctx, "final_score", 0.0),
            "session": getattr(ctx, "session", ""),
            "regime": getattr(ctx, "regime", ""),
            "deltaSpikeZLocalQ": getattr(ctx, "delta_spike_z_local_q", float("nan")),
            "deltaSpikeZLocalThr": getattr(ctx, "delta_spike_z_local_thr", float("nan")),
            "obiLocalQ": getattr(ctx, "obi_local_q", float("nan")),
            "obiLocalThr": getattr(ctx, "obi_local_thr", float("nan")),
            "weakProgressLocalQ": getattr(ctx, "weak_progress_local_q", float("nan")),
            "weakProgressLocalThr": getattr(ctx, "weak_progress_local_thr", float("nan")),
            "atrQuantileLocalQ": getattr(ctx, "atr_quantile_local_q", float("nan")),
            "atrQuantileLocalThr": getattr(ctx, "atr_quantile_local_thr", float("nan")),
            # Weak Progress fields
            "weakProgress": getattr(ctx, "weak_progress", None),
            "progressScoreComponent": getattr(ctx, "progress_score_component", None),
            "patternFamily": getattr(ctx, "pattern_family", None),
            "reverseDeltaSpikeZ": getattr(ctx, "reverse_delta_spike_z", None),
            "volumeZ": getattr(ctx, "volume_z", None),
            # Execution Plan fields
            "executionPlan": self._execution_plan_to_dict(getattr(ctx, "execution_plan", None)),
            # Signal Quality fields
            "quality": {
                "offline": getattr(ctx, "quality_offline", 0.0),
                "online": getattr(ctx, "quality_online", 50.0),
                "combined": getattr(ctx, "quality_combined", 0.0),
                "status": getattr(ctx, "quality_status", "unknown"),
            },
            "finalScoreWithQuality": getattr(ctx, "final_score", 0.0),
            "isDisabledByQuality": getattr(ctx, "is_disabled_by_quality", False),
            # Signal settings used for generation
            "signalSettings": signal_settings,
            "ctx": ctx_dict,
            # Experiment layer fields
            "experimentId": getattr(ctx, "experiment_id", None),
            "experimentVariant": getattr(ctx, "experiment_variant", None),
            "filterFlags": getattr(ctx, "filter_flags", {}),
        }

        # === INTEGRATION: Unified Signal Execution ===
        # Если сигнал прошел confidence фильтр, обрабатываем через unified service
        if final_conf >= min_conf and final_conf > 0 and self._signal_service:
            try:
                # Создаем unified SignalContext из существующего ctx
                from signal_exec import AccountState, Side
                from signal_exec import SignalContext as UnifiedSignalContext

                # Определяем side
                signal_side = Side.LONG if side.lower().startswith('l') else Side.SHORT

                # Создаем account state (в продакшене подтягивать из актуального состояния)
                account_state = AccountState(
                    equity_usd=float(os.getenv("ACCOUNT_EQUITY_USD", "10000")),
                    open_risk_usd=float(os.getenv("ACCOUNT_OPEN_RISK_USD", "0")),
                    max_risk_per_trade_pct=float(os.getenv("ACCOUNT_MAX_RISK_PER_TRADE_PCT", "0.5")),
                    max_portfolio_risk_pct=float(os.getenv("ACCOUNT_MAX_PORTFOLIO_RISK_PCT", "5.0")),
                )

                # Создаем unified context
                unified_ctx = UnifiedSignalContext(
                    signal_id=getattr(ctx, 'signal_id', f"{self.symbol}-{get_ny_time_millis()}"),
                    symbol=self.symbol,
                    setup_type=signal_type,
                    side=signal_side,
                    ts_signal=datetime.fromtimestamp(ts / 1000, tz=UTC),
                    price_at_signal=float(ctx.price),
                    atr_1m=getattr(ctx, 'atr_14_1m', 1.0),
                    tick_size=getattr(ctx, 'tick_size', 0.01),
                    contract_size=getattr(ctx, 'contract_size', 1.0),
                    final_score=final_conf / 100.0,  # Convert to 0-1 scale
                    account_state=account_state,
                    # Add microstructure if available
                    features={
                        'deltaSpikeZ': getattr(ctx, 'deltaSpikeZ', 0.0),
                        'OBI': getattr(ctx, 'obi', 0.0),
                        'weakProgress': getattr(ctx, 'weakProgress', 0.0),
                        'volumeZ': getattr(ctx, 'volumeZ', 0.0),
                    }
                )

                # Обрабатываем сигнал через unified service
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._signal_service.on_new_signal(unified_ctx))
                    self.logger.info(f"📋 Signal processed through unified service: {unified_ctx.signal_id}")
                finally:
                    loop.close()

                # Сохраняем unified context в payload для совместимости
                ctx.unified_signal_context = unified_ctx
                ctx.execution_plan = None  # Will be set by service if successful

            except Exception as e:
                self.logger.exception(f"Failed to process signal through unified service: {e}")

        # Публикуем сигнал в outbox для доставки в Telegram и другие системы
        if self.outbox_enabled:
            try:
                result = self.outbox.publish(
                    source=self.source_name,
                    strategy=self._get_strategy_key(),
                    symbol=self.symbol,
                    side=side,
                    kind=signal_type,
                    level_key=f"{signal_type}_{side}",
                    ts_ms=get_ny_time_millis(),
                    envelope={
                        "signal_payload": signal_payload,
                        "signal_settings": signal_settings
                    }
                )
                if result.sent:
                    self.logger.info(f"✅ Signal published to outbox: {signal_payload.get('sid', 'unknown')}")
                else:
                    self.logger.info(f"⚠️ Signal deduplicated (not sent): {signal_payload.get('sid', 'unknown')}")
            except Exception as e:
                self.logger.exception(f"Failed to publish signal to outbox: {e}")

        # Публикуем сигнал в stream:trade:entry_candidate для SMT entry policy
        self._publish_entry_candidate(side, signal_type, signal_settings)

        # Возвращаем результат публикации
        return PublishResult(sent=True, dedup=False)

    # === L3-Lite stream processing ===

    def _parse_l3_event(self, fields: dict[str, Any]) -> Any | None:
        """Parse L3-Lite event from Redis stream message."""
        try:
            from regime.l3_lite_models import L3LiteEvent

            ts_ms = fields.get("ts_ms") or fields.get("timestamp") or fields.get("ts")
            if ts_ms is None:
                return None

            if isinstance(ts_ms, str):
                ts_ms = float(ts_ms)
            ts_ms = int(ts_ms)

            kind = fields.get("kind") or fields.get("type") or fields.get("event")
            if not kind:
                return None

            side = fields.get("side")
            price = float(fields.get("price", 0.0))
            qty = float(fields.get("qty", 0.0)) or float(fields.get("quantity", 0.0)) or float(fields.get("size", 0.0))

            return L3LiteEvent(
                ts_ms=ts_ms,
                kind=kind,
                side=side,
                price=price,
                qty=qty,
            )

        except Exception as e:
            self.logger.warning("Failed to parse L3 event: %s", e)
            return None

    # --------------------------------------------------------------------
    # Signal-level метрики (минимальный набор):
    #   candidates_total{kind}
    #   signals_veto{kind,reason}
    #   touch_suppressed_total{kind}
    #   spread_filter_drops
    #   cooldown_drops
    #
    # Эти методы специально "тонкие" — их можно вызывать из любых handler/validator/scorer.
    # --------------------------------------------------------------------
    def _m_candidate(self, kind: str) -> None:
        m2 = getattr(self, "_m2", None)
        try:
            if m2 is not None:
                m2.inc("candidates_total", 1, tags={"kind": str(kind)})
        except Exception:
            return

    def _m_veto(self, kind: str, reason: str) -> None:
        m2 = getattr(self, "_m2", None)
        try:
            if m2 is not None:
                m2.inc("signals_veto", 1, tags={"kind": str(kind), "reason": reason})
        except Exception:
            return

    def _m_touch_suppressed(self, kind: str) -> None:
        m2 = getattr(self, "_m2", None)
        try:
            if m2 is not None:
                m2.inc("touch_suppressed_total", 1, tags={"kind": str(kind)})
        except Exception:
            return

    def _m_spread_drop(self) -> None:
        m2 = getattr(self, "_m2", None)
        try:
            if m2 is not None:
                m2.inc("spread_filter_drops", 1)
        except Exception:
            return

    def _m_cooldown_drop(self) -> None:
        m2 = getattr(self, "_m2", None)
        try:
            if m2 is not None:
                m2.inc("cooldown_drops", 1)
        except Exception:
            return

    def _process_l3_event(self, l3_event) -> None:
        """Process L3-Lite event - delegate to handler if it supports L3."""
        # Only process if handler has L3 aggregator (e.g., CryptoOrderFlowHandler)
        if hasattr(self, 'on_l3_event'):
            try:
                self.on_l3_event(l3_event)
            except Exception as e:
                self.logger.warning("Failed to process L3 event: %s", e)
