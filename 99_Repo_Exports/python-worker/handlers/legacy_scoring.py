# legacy_scoring.py - Deprecated scoring methods for backward compatibility
"""
DEPRECATED: Legacy scoring methods that are no longer used in the main pipeline.

These methods are kept for backward compatibility only.
The main pipeline now uses UnifiedSignalPipeline for all scoring operations.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base_orderflow_handler import OrderflowSignalContext


def _apply_golden_logic(ctx: "OrderflowSignalContext") -> None:
    """
    DEPRECATED: Логика golden pattern теперь в UnifiedSignalPipeline.apply_golden_logic().
    Этот метод оставлен для совместимости, но не используется в основном пайплайне.

    1) Определяем порог для этого паттерна (из ENV или дефолтный).
    2) Ставим флаги is_golden_pattern / golden_pattern_label.
    3) Подтягиваем вес паттерна (для последующей агрегации, если нужно).
    """
    # from core.config import (
    #     get_pattern_conf_threshold,
    #     get_pattern_weight,
    # )

    label = getattr(ctx, "pattern_label", None) or getattr(ctx, "golden_pattern_label", None)
    threshold = get_pattern_conf_threshold(label)

    confidence = getattr(ctx, "confidence", 0.0)
    ctx.is_golden_pattern = confidence >= threshold
    ctx.golden_pattern_label = label if ctx.is_golden_pattern else None

    ctx.pattern_weight = get_pattern_weight(label)


def _apply_scoring(ctx: "OrderflowSignalContext") -> None:
    """
    DEPRECATED: Логика скоринга теперь в UnifiedSignalPipeline.apply_scoring().
    Этот метод оставлен для совместимости, но не используется в основном пайплайне.

    final_score = (confidence_scaled) * pattern_weight * golden_mult
    всё ограничиваем FINAL_SCORE_MAX.
    """
    # from core.config import (
    #     CONFIDENCE_SCALE,
    #     GOLDEN_SCORE_MULTIPLIER,
    #     FINAL_SCORE_MAX,
    # )

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

    ctx.final_score = score


def _should_emit(ctx: "OrderflowSignalContext") -> bool:
    """
    DEPRECATED: This method is no longer used and will be removed.
    Use signals.router.should_emit() directly instead.
    """
    raise RuntimeError("_should_emit is deprecated, use signals.router.should_emit() instead")
