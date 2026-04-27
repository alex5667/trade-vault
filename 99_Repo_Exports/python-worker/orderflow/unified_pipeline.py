"""
Unified Signal Generation Pipeline

Цель: консолидировать всю логику генерации сигналов в один пайплайн,
чтобы избежать дублирования между OrderflowContext и SignalContext путями.

Основной пайплайн генерации сигнала:
1) собираем OrderflowContext (raw orderflow features, bar, l2, trades, etc.)
2) attach regime (+local calibration)  -> regime, atr_quantiles, regime_flags
3) считаем confidence / scoring       -> base_score, sub-scores
4) golden_logic + final_score         -> final_score, tags
5) should_emit + regime guard + exec_filters
6) publish (outbox + optional execution plan)

Этот пайплайн должен использоваться BaseOrderFlowHandler'ом:
- Собрать OrderflowContext из тика/бара
- Передать в UnifiedSignalPipeline.process()
- Получить готовый сигнал или None
"""

from typing import Optional, List, Dict, Any
from datetime import datetime

# Импорты сервисов
from signals.types import OrderflowContext, SignalContext
from signals.golden_pattern_service import GoldenPatternService
from signals.calibration_service import CalibrationService
from signals.exec_filters import ExecFiltersGroup
from signals.signal_publisher import SignalPublisher


class UnifiedSignalPipeline:
    """
    Основной пайплайн генерации сигналов.

    Консолидирует всю логику:
    - Скоринг через SignalScoringEngine
    - Калибровка через CalibrationService
    - Golden pattern через GoldenPatternService
    - Regime guard через RegimeService
    - Exec filters через ExecFiltersGroup
    - Публикация через SignalPublisher
    """

    def __init__(
        self,
        scoring_engine,  # SignalScoringEngine
        regime_service,  # MarketRegimeService
        golden_logic: GoldenPatternService,
        exec_filters: ExecFiltersGroup,
        publisher: SignalPublisher,
        calibrator: CalibrationService | None = None,
    ):
        self._scoring_engine = scoring_engine
        self._regime_service = regime_service
        self._golden_logic = golden_logic
        self._exec_filters = exec_filters
        self._publisher = publisher
        self._calibrator = calibrator

    # === 1. OrderflowContext -> SignalContext ===
    def build_ctx(self, of_ctx: OrderflowContext) -> SignalContext:
        """
        Создает SignalContext из OrderflowContext.
        """
        ctx = SignalContext(
            symbol=of_ctx.symbol,
            ts_event_ms=of_ctx.ts,
            of=of_ctx,
            session="",  # будет заполнено в attach_regime
            tags=[],
        )
        return ctx

    # === 2. attach regime (+local calibration) ===
    def attach_regime(self, ctx: SignalContext) -> None:
        """
        Прикрепляет информацию о режиме рынка и применяет локальную калибровку.
        """
        # Получаем режим рынка
        regime = self._regime_service.get_regime(ctx.symbol, ctx.ts_event_ms)
        ctx.regime = regime

        # Определяем сессию (упрощенная логика, можно улучшить)
        ts_utc = ctx.of.ts_utc or (ctx.ts_event_ms / 1000.0)
        dt = datetime.fromtimestamp(ts_utc)
        hour = dt.hour
        if 0 <= hour < 8:
            ctx.session = "asia"
        elif 8 <= hour < 16:
            ctx.session = "europe"
        else:
            ctx.session = "us"

        # Применяем локальную калибровку, если есть
        if self._calibrator is not None:
            self._calibrator.apply_local_calibration(ctx)

    # === 3. считаем confidence / scoring ===
    def apply_scoring(self, ctx: SignalContext) -> None:
        """
        Вычисляет базовый скор через новый SignalScoringEngine.score().
        """
        scoring_result = self._scoring_engine.score(ctx)

        # Заполняем поля в контексте
        ctx.base_score = scoring_result.score
        ctx.final_score = scoring_result.final_score
        ctx.confidence = scoring_result.confidence
        ctx.quality_label = scoring_result.quality_label
        ctx.quality_reasons = scoring_result.quality_reasons

        # Сохраняем результат скоринга для использования в should_emit
        ctx._scoring_result = scoring_result

    # === 4. should_emit + regime guard + exec_filters ===
    def should_emit(self, ctx: SignalContext) -> bool:
        """
        Проверяет, следует ли эмиттировать сигнал.
        Использует результат скоринга из apply_scoring.
        """
        # Получаем результат скоринга
        scoring_result = getattr(ctx, '_scoring_result', None)
        if scoring_result:
            # Используем should_emit из скоринга
            if not scoring_result.should_emit:
                return False
        else:
            # Fallback на старые проверки
            if ctx.final_score <= 0:
                return False

            if ctx.is_disabled_by_quality:
                return False

        # Дополнительные проверки (regime guard, exec filters)
        # Note: эти проверки теперь должны быть включены в scoring_result.should_emit,
        # но оставляем для совместимости

        # Regime guard
        if ctx.regime and not self._regime_service.allow_emit(ctx.regime, ctx):
            return False

        # Exec filters (session, spread, volatility, etc.)
        if not self._exec_filters.check(ctx):
            return False

        return True

    # === 6. publish ===
    def build_signal(self, ctx: SignalContext) -> "Signal":
        """
        Создает объект Signal из SignalContext.
        """
        # Определяем сторону сигнала
        side = "LONG" if ctx.of.z_delta > 0 else "SHORT"

        # Создаем сигнал (структура будет уточнена при интеграции)
        signal = {
            "symbol": ctx.symbol,
            "ts_event_ms": ctx.ts_event_ms,
            "side": side,
            "score": ctx.final_score,
            "tags": ctx.tags.copy(),
            "regime": ctx.regime.regime_type if ctx.regime else None,
            "session": ctx.session,
            "base_score": ctx.base_score,
            "is_golden_pattern": ctx.is_golden_pattern,
            "golden_pattern_label": ctx.golden_pattern_label,
            "quality_combined": ctx.quality_combined,
            # Добавляем метрики из orderflow context для анализа
            "z_delta": ctx.of.z_delta,
            "obi": ctx.of.obi,
            "atr": ctx.of.atr,
            "weak_progress": ctx.of.weak_progress,
        }

        return signal

    def publish(self, signal: "Signal") -> None:
        """
        Публикует сигнал через publisher.
        """
        self._publisher.publish(signal)

    # === Высокоуровневый entry-point ===
    def process(self, of_ctx: OrderflowContext) -> Optional["Signal"]:
        """
        Основной метод пайплайна: OrderflowContext -> Signal или None.

        Выполняет полный пайплайн:
        1. Создает SignalContext
        2. Прикрепляет режим и калибровку
        3. Применяет скоринг
        4. Применяет golden logic
        5. Проверяет should_emit
        6. Публикует сигнал

        Returns:
            Signal object если сигнал должен быть опубликован, иначе None
        """
        # 1. Создаем контекст для скоринга
        ctx = self.build_ctx(of_ctx)

        # 2. Прикрепляем режим и применяем калибровку
        self.attach_regime(ctx)

        # 3. Применяем скоринг (теперь включает golden logic и quality)
        self.apply_scoring(ctx)

        # 4. Проверяем should_emit
        if not self.should_emit(ctx):
            return None

        # 6. Создаем и публикуем сигнал
        sig = self.build_signal(ctx)
        self.publish(sig)

        return sig
