# regime_service.py
"""
Единый сервис для определения режима рынка.
Объединяет всю функциональность из base_orderflow_handler.py и других мест.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple, List, Literal, TYPE_CHECKING
from collections import deque, defaultdict
import time

from common.log import setup_logger
from .signal_types import MarketRegime
from .contexts import BarSample

if TYPE_CHECKING:
    from .handlers.base_orderflow_handler import OrderflowSignalContext, OrderflowTickContext


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
    regime_score: float = 0.0             # итоговый score для определения режима


@dataclass
class RegimeState:
    label: str  # "trending", "range", "mixed", "unknown"
    trend_score: float  # [-1, +1] raw trend score
    range_score: float  # [-1, +1] raw range score
    session_bias: float = 0.0
    daily_open_cross_freq: float = 0.0
    ts: float = 0.0  # timestamp
    symbol: str = ""  # symbol identifier
    last_update_ts: float = 0.0  # timestamp of last regime update


@dataclass
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
    session_bias_default: Dict[str, float] = field(default_factory=lambda: {
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
    def from_env(cls) -> "RegimeConfig":
        """Create config from environment variables"""
        import os
        return cls(
            atr_quantile_trend_thr=float(os.getenv("REGIME_ATR_TREND_THR", "0.7")),
            atr_quantile_range_thr=float(os.getenv("REGIME_ATR_RANGE_THR", "0.3")),
        )


@dataclass
class RegimeUpdatePayload:
    """Единый payload для обновления режима"""
    symbol: str
    ts: int
    regime: MarketRegime
    features: RegimeFeatures
    source: str
    meta: dict[str, Any]


@dataclass
class RegimeSample:
    """Sample for regime history tracking"""
    ts: float
    price: float
    vwap_side: int        # -1 / 0 / +1 (ниже / на / выше VWAP)
    daily_open_side: int  # -1 / 0 / + 1 (ниже / на / выше open)
    bar_index: int | None = None


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
        self._history: Dict[str, deque[Tuple[float, float, float]]] = defaultdict(
            lambda: deque(maxlen=240)  # ~4 часа по 1m, настраивается
        )
        self._last_state: Dict[str, RegimeState] = {}

        # история режима по инструментам
        self._regime_history: dict[str, deque[RegimeSample]] = defaultdict(
            lambda: deque(maxlen=self._cfg.regime_window_size)
        )

        # история баров для поиска локальных экстремумов
        self._bar_history: Dict[str, deque[BarSample]] = defaultdict(
            lambda: deque(maxlen=200)
        )

        # режимное окно для классификации TREND/RANGE/MIXED
        self._regime_window: deque[dict] = deque(maxlen=30)  # 30 минут по 1m

    def _compute_cross_bias_window(self) -> float:
        """
        Считаем долю бычьих / медвежьих направлений в окне self._regime_window.
        Возвращаем bias в диапазоне [-1; +1].
        """
        if not self._regime_window:
            return 0.0

        longs = 0
        shorts = 0

        for r in self._regime_window:
            d = r.get("trend_dir", 0.0)
            if d > 0:
                longs += 1
            elif d < 0:
                shorts += 1

        n = longs + shorts
        if n == 0:
            return 0.0

        return (longs - shorts) / n  # (-1 .. +1)

    def _base_regime_from_scores(
        self,
        trend_score: float,
        vol_score: float,
    ) -> RegimeLabel:
        # Базовый режим по текущей свече
        if abs(trend_score) < 0.5 and vol_score < 0.3:
            return "squeeze"
        if abs(trend_score) < 0.5:
            return "range"
        if trend_score >= 0.5:
            return "trending_bull"
        return "trending_bear"

    # --- daily_open: range & crossings ---

    def _compute_daily_open_metrics(
        self,
        hist: deque[Tuple[float, float, float]],
    ) -> Tuple[float, float]:
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

    def _decide_regime_from_features(self, f: RegimeFeatures, cross_bias: float = 0.0) -> Tuple[MarketRegime, float]:
        cfg = self._cfg

        # 1) TREND score ∈ [0,1]
        trend_parts: List[float] = []

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
        range_parts: List[float] = []

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

    def _decide_regime(
        self,
        row: Dict,
        cross_bias: float,
    ) -> RegimeLabel:
        """
        Принимает cross_bias и использует его как тай-брейкер и для окраски боковика.
        """
        cfg = self._cfg

        trend_score: float = row.get("trend_score", 0.0)
        vol_score: float = row.get("vol_score", 0.0)

        # Базовый режим по текущей свече
        base_regime: RegimeLabel = self._base_regime_from_scores(
            trend_score=trend_score,
            vol_score=vol_score,
        )

        bias_strong = getattr(cfg, "BIAS_STRONG", 0.6)
        bias_weak = getattr(cfg, "BIAS_WEAK", 0.25)
        trend_bias_zone = getattr(cfg, "TREND_SCORE_BIAS_ZONE", 0.5)

        # 1) Тай-брейкер: в «серой зоне» по тренду
        if abs(trend_score) < trend_bias_zone:
            if cross_bias > bias_strong:
                return "trending_bull"
            if cross_bias < -bias_strong:
                return "trending_bear"

        # 2) Окраска боковика по bias
        if base_regime == "range":
            if cross_bias > bias_weak:
                return "range_bullish"
            if cross_bias < -bias_weak:
                return "range_bearish"

        if base_regime == "squeeze":
            if cross_bias > bias_weak:
                return "squeeze_bullish"
            if cross_bias < -bias_weak:
                return "squeeze_bearish"

        # 3) В остальных случаях оставляем базовый режим
        return base_regime

    # --- публичные методы ---

    def detect(self, snapshot, session: str, daily_stats) -> RegimeState:
        """
        Определяет режим рынка по snapshot данным.
        """
        label = "unknown"
        trend_score = 0.0
        range_score = 0.0

        if hasattr(snapshot, "features") and isinstance(snapshot.features, RegimeFeatures):
            regime, score = self._decide_regime_from_features(snapshot.features)
            # Определяем label на основе regime
            if regime == MarketRegime.TREND:
                label = "trending"
            elif regime == MarketRegime.RANGE:
                label = "range"
            elif regime == MarketRegime.MIXED:
                label = "mixed"
            # разложим score на два канала
            trend_score = max(score, 0.0)
            range_score = max(-score, 0.0)

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

    def update_state(
        self,
        *,
        symbol: str,
        ts: int,
        regime: MarketRegime,
        features: RegimeFeatures,
        source: str = "manual",
        meta: Optional[dict[str, Any]] = None,
    ) -> RegimeState:
        """
        Тонкая обёртка над _update_impl: режим и фичи уже заданы снаружи.
        Никакой своей логики детекта/вычислений тут нет.
        """
        payload = RegimeUpdatePayload(
            symbol=symbol,
            ts=ts,
            regime=regime,
            features=features,
            source=source,
            meta=meta or {},
        )
        return self._update_impl_to_regime_state(payload)

    def last_state(self, symbol: str) -> Optional[RegimeState]:
        """Можно использовать для логов / визуализации по времени."""
        return self._last_state.get(symbol)

    def session_bias(self, session: str) -> float:
        """Получить bias по сессии."""
        return self._cfg.session_bias_default.get(session, 0.0)

    def _update_regime_history(self, ctx: "OrderflowSignalContext | OrderflowTickContext", bar_index: int | None = None) -> None:
        price = getattr(ctx, "last_price", None) or getattr(ctx, "price", None)
        if ctx.symbol is None or price is None or ctx.vwap is None or ctx.daily_open is None:
            return

        now = ctx.ts_utc or time.time()

        # сторон VWAP
        vwap_side = 0
        if ctx.vwap is not None:
            diff_v = price - ctx.vwap
            if diff_v > 0.0:
                vwap_side = 1
            elif diff_v < 0.0:
                vwap_side = -1

        # сторона daily_open
        daily_open_side = 0
        if ctx.daily_open is not None:
            diff_o = price - ctx.daily_open
            if diff_o > 0.0:
                daily_open_side = 1
            elif diff_o < 0.0:
                daily_open_side = -1

        hist = self._regime_history[ctx.symbol]
        hist.append(
            RegimeSample(
                ts=now,
                price=price,
                vwap_side=vwap_side,
                daily_open_side=daily_open_side,
                bar_index=bar_index,
            )
        )

    def _compute_cross_bias_from_history(self, symbol: str) -> float | None:
        hist = self._regime_history.get(symbol)
        if not hist or len(hist) < 3:
            return None

        vwap_crosses = 0
        open_crosses = 0
        pairs = 0

        prev = hist[0]
        for cur in list(hist)[1:]:
            if prev.vwap_side != 0 and cur.vwap_side != 0 and prev.vwap_side != cur.vwap_side:
                vwap_crosses += 1
            if prev.daily_open_side != 0 and cur.daily_open_side != 0 and prev.daily_open_side != cur.daily_open_side:
                open_crosses += 1

            pairs += 1
            prev = cur

        if pairs == 0:
            return None

        cross_rate_vwap = vwap_crosses / pairs
        cross_rate_open = open_crosses / pairs
        cross_rate = 0.5 * (cross_rate_vwap + cross_rate_open)

        bias = 1.0 - 2.0 * max(0.0, min(1.0, cross_rate))  # [0..1] → [+1..-1]
        return bias

    def _compute_cross_bias(self, symbol: str | None = None) -> float:
        """
        Унифицированный расчёт cross_bias.

        1) Если передан symbol и есть история VWAP/daily_open —
           считаем bias по ней.
        2) Иначе — fallback на старую логику по self._regime_window (trend_dir).
        """
        # 1) Попытка по истории VWAP/daily_open
        if symbol:
            bias_hist = self._compute_cross_bias_from_history(symbol)
            if bias_hist is not None:
                return bias_hist

        # 2) Fallback: bias по окну режимов, на основе trend_dir
        if not self._regime_window:
            return 0.0

        longs = 0
        shorts = 0

        for r in self._regime_window:
            d = r.get("trend_dir", 0.0)
            if d > 0:
                longs += 1
            elif d < 0:
                shorts += 1

        n = longs + shorts
        if n == 0:
            return 0.0

        # (-1 .. +1): +1 — чистый лонг, -1 — чистый шорт
        return (longs - shorts) / n

    def _compute_regime_features(
        self,
        ctx: "OrderflowTickContext | OrderflowSignalContext",
    ) -> RegimeFeatures:
        price = _coalesce(ctx.last_price, ctx.price, default=0.0)
        vwap = _coalesce(ctx.vwap, default=0.0)
        daily_open = _coalesce(ctx.daily_open, default=0.0)

        # ATR intraday в bps уже есть
        atr_intraday_bps = float(_coalesce(ctx.atr_14_bps, default=0.0))
        atr_quantile_1d = float(_coalesce(ctx.atr_14_q, default=0.5))

        weak_progress = float(_coalesce(ctx.weak_progress_raw, default=0.0))

        vwap_distance_bps = 0.0
        if price > 0 and vwap > 0:
            vwap_distance_bps = abs(price - vwap) / price * 10_000.0

        # пока без оценки тренда VWAP (можно добавить позже)
        vwap_trend_bps = 0.0

        daily_open_range_bps = float(ctx.daily_open_dist_bps or 0.0)

        # частоту пересечений daily_open берём из истории
        hist = self._history[ctx.symbol]
        _, cross_freq = self._compute_daily_open_metrics(hist)

        return RegimeFeatures(
            atr_intraday_bps=atr_intraday_bps,
            atr_quantile_1d=atr_quantile_1d,
            weak_progress=weak_progress,
            vwap_distance_bps=vwap_distance_bps,
            vwap_trend_bps=vwap_trend_bps,
            daily_open_range_bps=daily_open_range_bps,
            daily_open_cross_freq=cross_freq,
        )

    def _detect_regime_from_ctx(
        self,
        ctx: "OrderflowTickContext | OrderflowSignalContext",
    ) -> Tuple[MarketRegime, RegimeFeatures]:
        # 1) обновляем историю пересечений
        self._update_regime_history(ctx)

        feats = self._compute_regime_features(ctx)

        # 2) режим и score из фич
        regime_from_feats, score_feats = self._decide_regime_from_features(feats)

        # 3) cross_bias как стабилизатор/тай-брейкер
        cross_bias = self._compute_cross_bias(ctx.symbol)
        score = 0.75 * score_feats + 0.25 * cross_bias  # веса можно вынести в cfg

        # 4) сохранить score в feats и в ctx
        feats.regime_score = float(max(-1.0, min(1.0, score)))

        ctx.market_regime = regime_from_feats
        ctx.market_regime_score = feats.regime_score
        ctx.regime_trend_score = max(feats.regime_score, 0.0)
        ctx.regime_range_score = max(-feats.regime_score, 0.0)
        ctx.cross_bias = cross_bias  # храним отдельно как дополнительный фактор

        return regime_from_feats, feats

    def update_from_ctx(self, ctx: "OrderflowTickContext | OrderflowSignalContext", *, source: str = "auto") -> RegimeState:
        symbol = getattr(ctx, "symbol", "") or "unknown"

        # ts_ms: предпочитаем ctx.ts (обычно ms), иначе вычисляем из ts_utc
        ts_ms = int(getattr(ctx, "ts", 0) or 0)
        if ts_ms <= 0:
            ts_utc = float(getattr(ctx, "ts_utc", 0.0) or time.time())
            ts_ms = int(ts_utc * 1000)

        # история daily_open-crossings хранится в секундах
        ts_s = float(getattr(ctx, "ts_utc", 0.0) or (ts_ms / 1000.0))

        price = float(_coalesce(getattr(ctx, "last_price", None), getattr(ctx, "price", None), default=0.0))
        daily_open = float(_coalesce(getattr(ctx, "daily_open", None), default=0.0))

        hist = self._history[symbol]
        hist.append((ts_s, price, daily_open))

        dopen_dist_bps, _ = self._compute_daily_open_metrics(hist)
        ctx.daily_open_dist_bps = dopen_dist_bps

        regime, features = self._detect_regime_from_ctx(ctx)

        payload = RegimeUpdatePayload(
            symbol=symbol,
            ts=ts_ms,
            regime=regime,
            features=features,
            source=source,
            meta={
                "timeframe_s": getattr(ctx, "timeframe_s", 60),
                "venue": getattr(ctx, "venue", "unknown"),
                "family": getattr(ctx, "family", "unknown"),
            },
        )
        return self._update_impl_to_regime_state(payload)

    def update(
        self,
        row: Dict | None = None,
        *,
        symbol: str | None = None,
        ts: int | None = None,
        regime: MarketRegime | None = None,
        features: RegimeFeatures | None = None,
        source: str = "manual",
        meta: Optional[dict[str, Any]] = None,
    ) -> RegimeDecision | RegimeState:
        """
        Унифицированный метод:

        1) legacy-путь: update(row) -> RegimeDecision
        2) новый путь: update(symbol=..., ts=..., regime=..., features=..., ...)
           -> RegimeState
        """
        # ---- 1) Старый путь: row с метриками свечи ----
        if row is not None:
            self._regime_window.append(row)

            # Берём symbol из row, если он там есть
            symbol_from_row = row.get("symbol")
            cross_bias = self._compute_cross_bias(symbol_from_row)

            regime_label = self._decide_regime(row, cross_bias)
            return RegimeDecision(regime=regime_label, cross_bias=cross_bias)

        # ---- 2) Новый путь: payload для RegimeState ----
        if symbol is None or ts is None or regime is None or features is None:
            raise ValueError(
                "Either `row` must be provided, "
                "or (`symbol`, `ts`, `regime`, `features`) must be set."
            )

        payload = RegimeUpdatePayload(
            symbol=symbol,
            ts=ts,
            regime=regime,
            features=features,
            source=source,
            meta=meta or {},
        )
        return self._update_impl_to_regime_state(payload)

    def update_with_bias(self, row: Dict) -> RegimeDecision:
        """
        Явный алиас для legacy-пути: всегда возвращает RegimeDecision.
        """
        return cast(RegimeDecision, self.update(row))

    def _update_impl_to_regime_state(self, payload: RegimeUpdatePayload) -> RegimeState:
        """
        Внутренняя реализация обновления режима через единый payload.
        Возвращает RegimeState для совместимости с существующим API.
        """
        # Преобразуем score в trend_score и range_score для совместимости
        # score > 0 означает trend, score < 0 означает range
        score = getattr(payload.features, 'regime_score', 0.0)
        trend_score = max(score, 0.0)
        range_score = max(-score, 0.0)

        state = RegimeState(
            label=self._regime_enum_to_label(payload.regime),
            trend_score=trend_score,
            range_score=range_score,
            session_bias=payload.meta.get('session_bias', 0.0),
            daily_open_cross_freq=payload.features.daily_open_cross_freq,
            ts=time.time(),
            symbol=payload.symbol,
            last_update_ts=time.time(),
        )
        self._last_state[payload.symbol] = state
        return state

    def _regime_enum_to_label(self, regime: MarketRegime) -> str:
        """Конвертирует MarketRegime enum в строковый label."""
        return {
            MarketRegime.TREND: "trending",
            MarketRegime.RANGE: "range",
            MarketRegime.MIXED: "mixed",
            MarketRegime.UNKNOWN: "unknown",
        }.get(regime, "unknown")


