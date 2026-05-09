"""
Golden Pattern Service

Определяет "золотые" паттерны на основе комбинации скоринговых метрик
и применяет соответствующие бусты к финальному скору.
"""


from signals.types import SignalContext


class GoldenPatternService:
    """
    Сервис для определения и применения golden pattern логики.
    """

    def __init__(self):
        # Golden pattern thresholds
        self.golden_regime_min = 0.7
        self.golden_geometry_min = 0.7
        self.golden_liquidity_min = 0.7
        self.golden_score_multiplier = 1.2  # 20% boost for golden patterns

    def apply(self, ctx: SignalContext) -> tuple[float, list[str]]:
        """
        Определяет, является ли паттерн золотым, и возвращает буст скора + теги.

        Returns:
            Tuple[float, List[str]]: (score_boost, extra_tags)
        """
        score_boost = 0.0
        extra_tags = []

        # Проверяем, является ли паттерн золотым
        if self._is_golden_pattern(ctx):
            score_boost = (ctx.base_score * self.golden_score_multiplier) - ctx.base_score
            extra_tags.append("golden_pattern")
            ctx.is_golden_pattern = True
            ctx.golden_pattern_label = self._determine_pattern_label(ctx)

        return score_boost, extra_tags

    def _is_golden_pattern(self, ctx: SignalContext) -> bool:
        """
        Определяет, является ли паттерн золотым на основе скоринговых метрик.
        """
        # Проверяем наличие необходимых метрик в orderflow контексте
        of = ctx.of

        # Режим рынка
        regime_score = getattr(of, 'regime_trend_score', 0.0) - getattr(of, 'regime_range_score', 0.0)
        regime_score_norm = max(0.0, min(1.0, (regime_score + 1.0) / 2.0))  # нормализуем в [0,1]

        # Геометрия (пока используем Z-delta как прокси)
        geometry_score = min(1.0, abs(of.z_delta) / 3.0)  # нормализуем Z-delta

        # Ликвидность (пока используем spread как прокси)
        spread_bps = getattr(of, 'spread_bps', 0.0)
        liquidity_score = max(0.0, 1.0 - (spread_bps / 100.0))  # меньше спред = лучше ликвидность

        # Проверяем пороги
        return (
            regime_score_norm >= self.golden_regime_min and
            geometry_score >= self.golden_geometry_min and
            liquidity_score >= self.golden_liquidity_min
        )

    def _determine_pattern_label(self, ctx: SignalContext) -> str:
        """
        Определяет лейбл паттерна на основе характеристик сигнала.
        """
        of = ctx.of
        z_delta = of.z_delta

        if z_delta > 0:
            return "bullish_golden"
        else:
            return "bearish_golden"
