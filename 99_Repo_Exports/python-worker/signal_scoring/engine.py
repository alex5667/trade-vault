from __future__ import annotations
from core.confidence_utils import normalize_confidence_pct, confidence_pct_to_ratio

import math
import os
from enum import Enum
from typing import Dict, Optional, Tuple, cast

# Try to import optional dependencies, provide stubs if not available
try:
    from local_calibration.store import LocalCalibrationStore, eval_local_quantile
    LOCAL_CALIBRATION_AVAILABLE = True
except ImportError:
    LOCAL_CALIBRATION_AVAILABLE = False
    # Stub classes
    class LocalCalibrationStore:
        pass
    def eval_local_quantile(*args, **kwargs):
        return 0.5

try:
    from signal_quality import SignalQualityEstimator, make_feature_bucket
    SIGNAL_QUALITY_AVAILABLE = True
except ImportError:
    SIGNAL_QUALITY_AVAILABLE = False
    # Stub classes
    class SignalQualityEstimator:
        pass
    def make_feature_bucket(*args, **kwargs):
        return {}

try:
    from signal_scoring.weak_progress import (
        get_weak_progress_config,
        apply_weak_progress_and_fade_filters,
        validate_signal_for_weak_progress
    )
    WEAK_PROGRESS_AVAILABLE = True
except ImportError:
    WEAK_PROGRESS_AVAILABLE = False
    # Stub functions
    def get_weak_progress_config():
        return {}
    def apply_weak_progress_and_fade_filters(*args, **kwargs):
        return True
    def validate_signal_for_weak_progress(*args, **kwargs):
        return True

# LiquidityContext: lazy-import at module load to avoid circular dependency at
# service startup, while still allowing IDE resolution and early error detection.
try:
    from handlers.base_orderflow_handler import LiquidityContext as _LiquidityContext
except ImportError:  # pragma: no cover – only absent in unit-test environments
    _LiquidityContext = None  # type: ignore[assignment,misc]

from .config import ScoringConfig
from .ctx import SignalContext
from scoring.scoring_engine import ScoringResult, QualityResult, SignalQualityLabel


class LiquidityPattern(str, Enum):
    NONE = "none"
    BREAK = "break"
    ABSORPTION = "absorption"


class SignalScoringEngine:
    def __init__(
        self,
        calib_store: LocalCalibrationStore,
        config: ScoringConfig,
        quality_estimator: Optional[SignalQualityEstimator] = None
    ):
        self._calib_store = calib_store
        self._cfg = config
        self._quality_estimator = quality_estimator

    # ---- локальный квантиль по метрике ----

    def _metric_local_q(
        self,
        ctx: SignalContext,
        metric: str,
        invert: bool = False,
    ) -> float | None:
        # Get raw metric value from ctx attributes
        value = getattr(ctx, metric, None)
        if value is None:
            return None

        cfg = self._calib_store.get_metric_cfg(
            symbol=ctx.symbol,
            session=ctx.session,
            regime=ctx.regime,
            metric=metric,
        )
        if cfg is None or not cfg.cdf_points:
            return None

        q = eval_local_quantile(cfg.cdf_points, value)

        if invert:
            q = 1.0 - q
        return max(0.0, min(1.0, float(q)))

    # ---- основной расчёт confidence ----

    def compute_confidence(self, ctx: SignalContext) -> int:
        """
        Compute confidence with weak progress logic.

        1. Calculate base confidence from metrics (delta_spike_z, obi, atr_quantile)
        2. Apply weak progress filters and scoring adjustments
        3. Return final confidence
        """
        metric_qs: Dict[str, float] = {}

        # delta_spike_z: high better => invert=False
        q_delta = self._metric_local_q(ctx, "delta_spike_z", invert=False)
        metric_qs["delta_spike_z"] = q_delta or 0.0

        # obi: high better => invert=False
        q_obi = self._metric_local_q(ctx, "obi", invert=False)
        metric_qs["obi"] = q_obi or 0.0

        # atr_quantile: high better (strong deviation from average ATR)
        q_atr = self._metric_local_q(ctx, "atr_quantile", invert=False)
        metric_qs["atr_quantile"] = q_atr or 0.0

        # Save to ctx
        ctx.delta_spike_z_local_q = metric_qs["delta_spike_z"]
        ctx.obi_local_q = metric_qs["obi"]
        ctx.atr_local_q = metric_qs["atr_quantile"]

        # Weighted average of base metrics (excluding weak_progress for now)
        weights = self._cfg.metric_weights
        num = 0.0
        den = 0.0
        for name, q in metric_qs.items():
            w = float(weights.get(name, 0.0))
            if w <= 0.0:
                continue
            num += q * w
            den += w

        if den <= 0.0:
            base_combined_q = 0.0
        else:
            base_combined_q = num / den

        # Apply pattern weight to base confidence
        pattern_weight = self._cfg.get_pattern_weight(ctx.pattern_name)
        base_combined_q = max(0.0, min(1.0, base_combined_q * pattern_weight))

        # Base confidence from structural factors (0-100)
        base_confidence = int(round(base_combined_q * 100))
        ctx.base_confidence = base_confidence

        # Get weak progress configuration for this pattern
        wp_cfg = get_weak_progress_config(ctx.pattern_name)
        ctx.pattern_family = wp_cfg.family

        # Apply weak progress filters and scoring
        final_confidence = apply_weak_progress_and_fade_filters(
            ctx=ctx,
            pattern_cfg=wp_cfg,
            base_conf=base_confidence,
        )

        # Minimum confidence threshold (considers symbol and pattern)
        min_conf = self._cfg.get_min_confidence(ctx.symbol, ctx.pattern_name)
        ctx.min_confidence_used = min_conf

        # Golden pattern: based on final confidence level
        ctx.is_golden_pattern = final_confidence >= self._cfg.golden_pattern_min_confidence
        ctx.golden_pattern_label = (
            f"{ctx.pattern_name}_golden"
            if ctx.is_golden_pattern and ctx.pattern_name
            else None
        )

        return final_confidence

    # ---- оценка качества сигнала ----

    def _enrich_with_quality(self, ctx: SignalContext) -> None:
        """Обогащает контекст оценкой качества из исторических данных."""
        if self._quality_estimator is None:
            # Если нет quality estimator, устанавливаем нейтральные значения
            ctx.quality_offline = 0.0
            ctx.quality_online = 50.0
            ctx.quality_combined = 0.0
            ctx.quality_status = "unknown"
            ctx.final_score = float(ctx.confidence or 0)
            return

        # Создаем feature bucket для поиска качества
        fb = make_feature_bucket(
            delta_spike_z=ctx.delta_spike_z,
            obi=ctx.obi,
            weak_progress=ctx.weak_progress,
            atr_quantile=ctx.atr_quantile,
        )

        # Оцениваем качество
        q_est = self._quality_estimator.estimate(
            symbol=ctx.symbol,
            signal_type=ctx.pattern_name or "generic",
            side=ctx.side,
            session=ctx.session,
            regime=ctx.regime,
            feature_bucket=fb,
        )

        if q_est is None:
            # Нет данных по качеству
            ctx.quality_offline = 0.0
            ctx.quality_online = 50.0
            ctx.quality_combined = 0.0
            ctx.quality_status = "unknown"
            ctx.final_score = float(ctx.confidence or 0)
            return

        # Заполняем поля качества
        ctx.quality_offline = q_est.offline_score
        ctx.quality_online = q_est.online_score
        ctx.quality_combined = q_est.combined_score
        ctx.quality_status = q_est.status

        # Вычисляем финальный скор как комбинацию confidence и quality
        ctx.final_score = self._combine_conf_and_quality(
            conf=ctx.confidence or 0,
            quality=ctx.quality_combined,
        )

        # Проверяем на отключение по качеству
        ctx.is_disabled_by_quality = (q_est.status == "disabled")

    # ---- комбинация confidence и quality ----

    def _combine_conf_and_quality(self, conf: int, quality: float) -> float:
        """
        Комбинирует confidence (0-100) и quality (0-100) в финальный скор.

        Использует геометрическое среднее для консервативного подхода.
        """
        if conf <= 0 or quality <= 0:
            return 0.0

        # Нормализуем к 0-1
        conf_norm = conf / 100.0
        quality_norm = quality / 100.0

        # Геометрическое среднее (консервативный подход)
        combined_norm = math.sqrt(conf_norm * quality_norm)

        # Обратно к 0-100
        return combined_norm * 100.0

    # ---- liquidity component ----

    def _compute_liquidity_component(self, ctx: SignalContext) -> float:
        """
        Мультипликативная надбавка к base_score в долях [-0.2 .. +0.2].

        - pattern == "break": даём бонус (чем выше liquidity_context_score, тем больше +%)
        - pattern == "absorption": даём штраф (чем выше score, тем больше -%)
        - pattern == "none": слабая двухсторонняя модуляция вокруг 0
        """
        liq_ctx: Optional[object] = getattr(ctx, "liquidity_context", None)
        if liq_ctx is None:
            return 0.0

        score = liq_ctx.liquidity_context_score
        if score is None:
            return 0.0

        # safety clamp на всякий случай
        score = float(max(0.0, min(1.0, score)))

        pattern = liq_ctx.pattern or "none"

        # Тюнимые максимумы через ENV (при отсутствии – дефолты)
        try:
            max_bonus_break = float(os.getenv("SCORING_LIQ_BREAK_MAX", "0.15"))      # до +15% к base_score
            max_penalty_abs = float(os.getenv("SCORING_LIQ_ABSORB_MAX", "0.15"))     # до -15% к base_score
            neutral_span = float(os.getenv("SCORING_LIQ_NEUTRAL_SPAN", "0.05"))      # ±5% для pattern="none"
        except ValueError:
            max_bonus_break = 0.15
            max_penalty_abs = 0.15
            neutral_span = 0.05

        if pattern == "break":
            # только бонус: 0 -> 0, 1 -> +max_bonus_break
            component = max_bonus_break * score

        elif pattern == "absorption":
            # только штраф: 0 -> 0, 1 -> -max_penalty_abs
            component = -max_penalty_abs * score

        else:
            # pattern == "none" или что-то неизвестное:
            # слабая симметричная модуляция вокруг 0.5
            centered = score - 0.5  # [-0.5 .. +0.5]
            # 0 -> ~0, 1 -> +neutral_span, 0 -> -neutral_span
            component = (centered / 0.5) * neutral_span

        # Жёсткий глобальный safety-кламп
        return max(-0.20, min(0.20, component))

    # ---- новый унифицированный метод score ----
    def score(self, ctx: SignalContext) -> ScoringResult:
        """
        Единая точка: считает score, confidence, quality, should_emit.
        """
        # 1) базовый score (confidence из compute_confidence)
        #    базовый score держим в [0..1]
        # compute_confidence must return percent (0..100).
        # Backward-compat: if it returns 0..1 ratio, normalize_confidence_pct converts to pct first.
        conf_pct = normalize_confidence_pct(self.compute_confidence(ctx))
        base_score = confidence_pct_to_ratio(conf_pct)

        # 2) базовый confidence (используем то же значение)
        base_confidence = base_score

        # --- LIQUIDITY: подмешиваем компонент, завязанный на LiquidityContext ---
        liquidity_component = self._compute_liquidity_component(ctx)
        # base_score * (1 + component), клампим в [0..1]
        adjusted_score_0_1 = base_score * (1.0 + liquidity_component)
        adjusted_score_0_1 = max(0.0, min(1.0, adjusted_score_0_1))

        # 3) качество через SignalQualityEstimator (работает от базового score)
        quality_res = (
            self._quality_estimator.estimate_quality(
                ctx=ctx,
                base_score=base_score,
                base_confidence=base_confidence,
            )
            if self._quality_estimator
            else None
        )

        if quality_res:
            confidence = quality_res.confidence
            quality_label = quality_res.label
            reasons = list(quality_res.reasons)
            force_reject = quality_res.force_reject
        else:
            # Fallback без quality estimator
            confidence = base_confidence
            quality_label = SignalQualityLabel.C
            reasons = ["no_quality_estimator"]
            force_reject = False

        # причины по ликвидности
        liq_ctx = getattr(ctx, "liquidity_context", None)
        if liq_ctx is None:
            reasons.append("no_liquidity_context")
        else:
            score = getattr(liq_ctx, "liquidity_context_score", None)
            if score is not None:
                reasons.append(f"liquidity:{score:.3f}")
            pattern = getattr(liq_ctx, "pattern", None)
            if pattern:
                reasons.append(f"liq_pattern:{pattern}")

        # 4) финальный score в шкале 0-100
        final_score = adjusted_score_0_1 * 100.0

        # 5) порог confidence по символу
        # thresholds are also percent in config
        min_conf_pct = normalize_confidence_pct(self._get_min_confidence_for_symbol(ctx.symbol))
        min_conf = confidence_pct_to_ratio(min_conf_pct)

        # 6) проверки should_emit
        allow_conf = confidence >= min_conf
        allow_score = final_score >= 30.0  # минимальный порог
        allow_quality = not force_reject

        # NEW: дополнительный гейт по ликвидности
        should_emit = allow_conf and allow_score and allow_quality

        return ScoringResult(
            # score — базовый (без учёта ликвидности), в шкале 0-100
            score=base_score * 100.0,
            # final_score — с учётом ликвидности
            final_score=final_score,
            confidence=confidence,
            quality_label=quality_label,
            reasons=reasons,
            should_emit=should_emit,
            debug={
                "min_confidence": min_conf,
                "allow_conf": allow_conf,
                "allow_score": allow_score,
                "allow_quality": allow_quality,
                "base_score": base_score,
                "base_confidence": base_confidence,
                "liquidity_component": liquidity_component,
                "adjusted_score_0_1": adjusted_score_0_1,
                "force_reject": force_reject,
            },
        )

    def _get_min_confidence_for_symbol(self, symbol: str) -> float:
        """
        Возвращает минимальный порог confidence для символа (в шкале 0-100).
        """
        return float(self._cfg.get_min_confidence(symbol, pattern=None))

    # ---- финальный фильтр should_emit ----

    def should_emit(self, ctx: SignalContext) -> bool:
        """
        Обертка над новым score() методом для обратной совместимости.
        """
        res = self.score(ctx)

        # Заполняем поля в контексте для совместимости
        ctx.score_raw = res.score
        ctx.score_final = res.final_score
        ctx.confidence = res.confidence
        ctx.quality_label = res.quality_label
        ctx.quality_reasons = res.reasons

        return res.should_emit