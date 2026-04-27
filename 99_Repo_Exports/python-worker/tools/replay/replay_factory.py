"""
Record & Replay: фабрика пайплайна для локального прогона записанных ctx.

ВАЖНО: вы уточнили реальную сигнатуру UnifiedSignalPipeline.__init__:
    def __init__(self, scoring_engine, regime_service, golden_logic, exec_filters, publisher, calibrator=None)

Этот файл приводит replay-обвязку в соответствие с актуальным API
и добавляет "мягкие" фоллбеки (noop-зависимости), чтобы тесты и локальный replay
могли работать без Redis/WS/TG и прочего окружения.
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any, Optional, Protocol


def _env_flag(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v not in {"0", "false", "no", "off", ""}


def _safe_import(path: str) -> Any:
    """
    Импорт по строке 'pkg.mod:Attr' или 'pkg.mod.Attr'.
    Возвращает None при ошибке — в replay-режиме это ожидаемо.
    """
    try:
        if ":" in path:
            mod, attr = path.split(":", 1)
        else:
            mod, attr = path.rsplit(".", 1)
        m = importlib.import_module(mod)
        return getattr(m, attr)
    except Exception:
        return None


class _PublisherLike(Protocol):
    """
    Универсальный "публикатор" для пайплайна.
    В проде это может быть SignalPublisher, который пишет в outbox/эмиттер.
    В replay/test мы подсовываем CapturePublisher.
    """
    def publish(self, payload: dict[str, Any]) -> bool: ...


@dataclass
class ReplayPipelineBundle:
    pipeline: Any
    publisher: _PublisherLike


class NoopGoldenPatternService:
    """
    GoldenPatternService в replay может быть отключён: он не должен ломать прогон.
    Если у вас golden-логика важна для поведения — передайте реальный инстанс.
    """
    def __init__(self) -> None:
        self.enabled = False

    # Пайплайн в разных версиях мог вызывать разные методы — держим "мягкий" интерфейс.
    def match(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        return None

    def process(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        return None


class NoopExecFiltersGroup:
    """
    ExecFiltersGroup: в replay чаще всего хотим fail-open.
    Если вам нужен реализм исполнения — передайте реальный exec_filters.
    """
    def allow(self, *args: Any, **kwargs: Any) -> bool:  # pragma: no cover
        return True

    def validate(self, *args: Any, **kwargs: Any) -> bool:  # pragma: no cover
        return True

    def apply(self, *args: Any, **kwargs: Any) -> bool:  # pragma: no cover
        return True


class CtxRegimeService:
    """
    MarketRegimeService-фоллбек:
    - ничего не тянем извне
    - если ctx уже содержит regime/trend_score/range_score — считаем, что режим известен
    """
    def get_regime(self, symbol: str) -> Any:  # pragma: no cover
        return None

    def get_snapshot(self, symbol: str) -> Any:  # pragma: no cover
        return None


class StubScoringEngine:
    """
    Минимальный скоринг для replay/test, если реальный SignalScoringEngine не передан.
    ВАЖНО: это НЕ продовый скоринг. Его задача — обеспечить детерминизм тестов
    и не падать на неполных ctx.
    """
    def score(self, ctx: Any) -> tuple[float, dict[str, Any]]:  # pragma: no cover
        raw = float(getattr(ctx, "z_delta", 0.0) or 0.0)
        # conf_factor ∈ [0..1] — "нейтрально"
        conf = 0.5
        parts = {"stub": 1, "raw": raw, "conf": conf}
        return raw * conf, parts


def build_unified_pipeline_for_replay(
    *,
    logger: Any,
    publisher: Optional[_PublisherLike] = None,
    scoring_engine: Optional[Any] = None,
    regime_service: Optional[Any] = None,
    golden_logic: Optional[Any] = None,
    exec_filters: Optional[Any] = None,
    calibrator: Optional[Any] = None,
) -> ReplayPipelineBundle:
    """
    Собирает UnifiedSignalPipeline в соответствии с актуальной сигнатурой.

    Приоритет зависимостей:
    1) явно переданные аргументы
    2) автосборка реальных классов (если включено REPLAY_USE_REAL_DEPS=1)
    3) noop/stub (чтобы replay не падал без окружения)
    """
    UnifiedSignalPipeline = _safe_import("signals.unified_pipeline:UnifiedSignalPipeline")
    if UnifiedSignalPipeline is None:
        # Иногда модуль мог лежать по другому пути (оставляем "мягкий" шанс).
        UnifiedSignalPipeline = _safe_import("python_worker.signals.unified_pipeline:UnifiedSignalPipeline")
    if UnifiedSignalPipeline is None:
        raise RuntimeError("UnifiedSignalPipeline not found (signals.unified_pipeline:UnifiedSignalPipeline)")

    # ------------- publisher -------------
    if publisher is None:
        CapturePublisher = _safe_import("tools.replay.capture_publisher:CapturePublisher")
        if CapturePublisher is None:
            raise RuntimeError("CapturePublisher not found (tools.replay.capture_publisher:CapturePublisher)")
        publisher = CapturePublisher(logger=logger)

    # ------------- optional real deps -------------
    use_real = _env_flag("REPLAY_USE_REAL_DEPS", "0")

    if scoring_engine is None and use_real:
        # Наиболее вероятный путь в вашем проекте: python-worker/signal_scoring/engine.py
        ScoringEngine = (
            _safe_import("signal_scoring.engine:SignalScoringEngine")
            or _safe_import("python_worker.signal_scoring.engine:SignalScoringEngine")
        )
        if ScoringEngine is not None:
            try:
                scoring_engine = ScoringEngine()
            except Exception:
                scoring_engine = None

    if regime_service is None and use_real:
        RegimeService = (
            _safe_import("services.market_regime_service:MarketRegimeService")
            or _safe_import("python_worker.services.market_regime_service:MarketRegimeService")
            or _safe_import("handlers.regime_service:MarketRegimeService")
        )
        if RegimeService is not None:
            try:
                regime_service = RegimeService()
            except Exception:
                regime_service = None

    if golden_logic is None and use_real:
        Golden = (
            _safe_import("services.golden_pattern_service:GoldenPatternService")
            or _safe_import("signals.golden_pattern_service:GoldenPatternService")
        )
        if Golden is not None:
            try:
                golden_logic = Golden()
            except Exception:
                golden_logic = None

    if exec_filters is None and use_real:
        ExecFilters = (
            _safe_import("exec_filters.group:ExecFiltersGroup")
            or _safe_import("services.exec_filters:ExecFiltersGroup")
        )
        if ExecFilters is not None:
            try:
                exec_filters = ExecFilters()
            except Exception:
                exec_filters = None

    # ------------- hard fallbacks -------------
    if scoring_engine is None:
        scoring_engine = StubScoringEngine()
    if regime_service is None:
        regime_service = CtxRegimeService()
    if golden_logic is None:
        golden_logic = NoopGoldenPatternService()
    if exec_filters is None:
        exec_filters = NoopExecFiltersGroup()

    # ------------- build pipeline (REAL SIGNATURE) -------------
    # Ваше уточнение: UnifiedSignalPipeline(scoring_engine, regime_service, golden_logic, exec_filters, publisher, calibrator=None)
    pipeline = UnifiedSignalPipeline(
        scoring_engine,
        regime_service,
        golden_logic=golden_logic,
        exec_filters=exec_filters,
        publisher=publisher,
        calibrator=calibrator,
    )

    return ReplayPipelineBundle(pipeline=pipeline, publisher=publisher)
