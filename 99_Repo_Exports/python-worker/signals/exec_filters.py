"""
Execution Filters Group

Группа фильтров для проверки возможности исполнения сигнала:
- Session filters (торговые сессии)
- Spread filters (ликвидность)
- Volatility filters (волатильность)
- Time filters (время суток)
- Liquidity guard (дополнительный фильтр по liquidity_score)
"""

from signal_scoring.config import ScoringConfig
from signals.unified_pipeline import SignalContext


class ExecFiltersGroup:
    """
    Группа фильтров для проверки возможности исполнения сигнала.
    """

    def __init__(self, cfg: ScoringConfig | None = None):
        self._cfg = cfg or ScoringConfig.from_env()

        # Настройки фильтров
        self.max_spread_bps = 50.0  # максимальный спред в базисных пунктах
        self.min_atr_bps = 1.0      # минимальная ATR для волатильности
        self.max_atr_bps = 500.0    # максимальная ATR для волатильности
        self.allowed_sessions = ["asia", "europe", "us"]  # разрешенные сессии

    def check(self, ctx: SignalContext) -> bool:
        """
        Проверяет все фильтры исполнения.

        Returns:
            bool: True если все фильтры пройдены
        """
        return (
            self._check_session(ctx) and
            self._check_spread(ctx) and
            self._check_volatility(ctx) and
            self._check_time_filters(ctx) and
            self._check_liquidity(ctx)
        )

    def _check_session(self, ctx: SignalContext) -> bool:
        """
        Проверяет сессию торгов.
        """
        return ctx.session in self.allowed_sessions

    def _check_spread(self, ctx: SignalContext) -> bool:
        """
        Проверяет спред (ликвидность).
        """
        spread_bps = getattr(ctx.of, 'spread_bps', 0.0)
        return spread_bps <= self.max_spread_bps

    def _check_volatility(self, ctx: SignalContext) -> bool:
        """
        Проверяет волатильность (ATR в разумных пределах).
        """
        atr = getattr(ctx.of, 'atr', 0.0)
        if atr <= 0:
            return False

        # Для forex ATR обычно в базисных пунктах
        # Для crypto может быть в процентах - адаптируем
        atr_bps = atr
        if atr < 1.0:  # вероятно в процентах (0.01 = 1%)
            atr_bps = atr * 10000  # конвертируем в базисные пункты

        return self.min_atr_bps <= atr_bps <= self.max_atr_bps

    def _check_time_filters(self, ctx: SignalContext) -> bool:
        """
        Проверяет временные фильтры.
        """
        # Пример: запрет на сигналы в первые/последние минуты часа
        ts_ms = ctx.ts_event_ms
        seconds_in_hour = (ts_ms // 1000) % 3600

        # Запрещаем сигналы в первые 30 секунд часа (возможные гэпы)
        if seconds_in_hour < 30:
            return False

        # Запрещаем сигналы в последние 30 секунд часа (возможные фиксинги)
        if seconds_in_hour > 3570:  # 3600 - 30
            return False

        return True

    def _check_liquidity(self, ctx: SignalContext) -> bool:
        """
        Проверяет liquidity_score - дополнительный guard для совсем плохой ликвидности.
        """
        if not self._cfg.liquidity_enabled:
            return True

        liquidity_score_norm = getattr(ctx, "liquidity_score_norm", None)
        if liquidity_score_norm is None:
            return True  # если данных нет — пропускаем

        if liquidity_score_norm < self._cfg.liquidity_hard_floor:
            return False

        return True
