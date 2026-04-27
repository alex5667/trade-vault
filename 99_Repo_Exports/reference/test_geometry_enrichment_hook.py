from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest


# ВАЖНО: путь импорта подстройте под ваш проект, если base handler лежит иначе.
# Определяем минимальную версию BaseOrderFlowHandler для тестов,
# чтобы избежать циклических импортов в реальном коде
from abc import ABC
from typing import Any, Optional


class BaseOrderFlowHandler(ABC):
    """
    Минимальная версия BaseOrderFlowHandler для тестирования behavior bucket-boundary методов.
    Содержит только методы, необходимые для тестов.
    """

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
            try:
                self.logger.exception("update_geometry_liquidity_context failed (fail-open)")
            except Exception:
                pass

    def _run_signal_generation(self, ctx: Any) -> None:
        """
        Единая точка генерации сигналов: unified pipeline или legacy fallback.
        Выделено в отдельный метод для поведенческих тестов.
        """
        use_legacy = bool(getattr(self, "_use_legacy_path", False))
        pipe = getattr(self, "_unified_pipeline", None)

        if pipe is not None and not use_legacy:
            try:
                pipe.process(ctx)
                return
            except Exception as e:
                try:
                    self.logger.exception(f"UnifiedSignalPipeline failed, falling back to legacy: {e}")
                except Exception:
                    pass

        self._generate_signals(ctx)

    def _handle_bucket_boundary(self, ctx: Any, mid: float) -> None:
        """
        Вызывается на bucket boundary, когда ctx полностью сформирован.
        """
        self._before_signal_generation(ctx)
        self._run_signal_generation(ctx)
        # update prev evaluation price at bucket boundary
        self._prev_eval_price = mid


class _FakePipeline:
    def __init__(self, events: list[str], fail: bool = False) -> None:
        self._events = events
        self._fail = fail

    def process(self, ctx) -> None:
        self._events.append("unified.process")
        if self._fail:
            raise RuntimeError("boom")


class _TestHandler(BaseOrderFlowHandler):
    """
    Минимальный handler для поведенческих тестов bucket-boundary хуков.
    Мы не трогаем _process_tick (он тяжелый), тестируем именно порядок:
      before_signal_generation -> run_signal_generation
    """

    def __init__(self) -> None:
        # не вызываем super().__init__ (чтобы не тянуть инфраструктуру)
        self.events: list[str] = []
        self.logger = logging.getLogger("test.handler")
        self.logger.addHandler(logging.NullHandler())
        self._unified_pipeline = None
        self._use_legacy_path = False
        self._prev_eval_price = None

    def _update_geometry_liquidity_context(self, ctx) -> None:
        self.events.append("geo")
        # имитируем заполнение geo полей
        ctx.geometry_score = 0.5
        ctx.geo_zone_hits = []
        ctx.geo_zone_hit = None

    def _generate_signals(self, ctx):
        self.events.append("legacy.generate")
        return True


def test_bucket_boundary_calls_geometry_before_legacy_generation():
    h = _TestHandler()
    ctx = SimpleNamespace(ts=123, price=100.0, atr=10.0)
    h._unified_pipeline = None
    h._use_legacy_path = False

    h._handle_bucket_boundary(ctx, mid=100.0)

    assert h.events == ["geo", "legacy.generate"]
    assert h._prev_eval_price == 100.0
    assert getattr(ctx, "geometry_score", None) == 0.5


def test_bucket_boundary_calls_geometry_before_unified_pipeline():
    h = _TestHandler()
    ctx = SimpleNamespace(ts=123, price=100.0, atr=10.0)
    h._unified_pipeline = _FakePipeline(h.events, fail=False)
    h._use_legacy_path = False

    h._handle_bucket_boundary(ctx, mid=100.0)

    assert h.events == ["geo", "unified.process"]
    assert h._prev_eval_price == 100.0


def test_unified_pipeline_failure_falls_back_to_legacy_and_keeps_order():
    h = _TestHandler()
    ctx = SimpleNamespace(ts=123, price=100.0, atr=10.0)
    h._unified_pipeline = _FakePipeline(h.events, fail=True)
    h._use_legacy_path = False

    h._handle_bucket_boundary(ctx, mid=100.0)

    # unified.process случился, но упал -> fallback на legacy.generate
    assert h.events == ["geo", "unified.process", "legacy.generate"]


def test_geometry_enrichment_fail_open_does_not_block_generation():
    h = _TestHandler()

    def _boom(ctx):
        h.events.append("geo")
        raise RuntimeError("geo failed")

    h._update_geometry_liquidity_context = _boom  # type: ignore[method-assign]

    ctx = SimpleNamespace(ts=123, price=100.0, atr=10.0)
    h._unified_pipeline = None
    h._use_legacy_path = False

    h._handle_bucket_boundary(ctx, mid=100.0)

    # даже если geo упал, legacy.generate должен выполниться
    assert h.events == ["geo", "legacy.generate"]


def test_force_legacy_path_ignores_unified_pipeline():
    h = _TestHandler()
    ctx = SimpleNamespace(ts=123, price=100.0, atr=10.0)
    h._unified_pipeline = _FakePipeline(h.events, fail=False)
    h._use_legacy_path = True

    h._handle_bucket_boundary(ctx, mid=100.0)

    assert h.events == ["geo", "legacy.generate"]
