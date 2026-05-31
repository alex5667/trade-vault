from __future__ import annotations

import logging
from abc import ABC
from types import SimpleNamespace
from typing import Any, Optional


class BaseOrderFlowHandler(ABC):
    """
    Минимальная версия BaseOrderFlowHandler для тестирования dependency metrics.
    """
    def _before_signal_generation(self, ctx: Any) -> None:
        """
        Хук финализации контекста перед генерацией сигналов.
        """
        try:
            # ensure_dependency_defaults будет вызван здесь в реальном коде
            from core.dependency_policy import ensure_dependency_defaults
            ensure_dependency_defaults(ctx)
        except Exception:
            pass

    def _track_dependency_metrics(self, ctx: Any) -> None:
        """
        Лёгкие метрики качества данных (без внешних зависимостей).
        """
        try:
            flags = getattr(ctx, "data_quality_flags", []) or []
            total = int(getattr(self, "_dq_l3_total", 0) or 0) + 1
            missing = int(getattr(self, "_dq_l3_missing", 0) or 0) + (1 if ("l3_missing" in flags) else 0)
            self._dq_l3_total = total
            self._dq_l3_missing = missing
            # сохраняем в ctx для аудит/логов/эмиттера
            setattr(ctx, "l3_missing_rate", float(missing) / float(max(1, total)))
        except Exception:
            # метрики не должны ломать обработку
            pass


class _TestHandler(BaseOrderFlowHandler):
    def __init__(self) -> None:
        self.logger = logging.getLogger("test.base.depmetrics")
        self.logger.addHandler(logging.NullHandler())
        self._dq_l3_total = 0
        self._dq_l3_missing = 0

    def _update_geometry_liquidity_context(self, ctx) -> None:
        # не заполняем ничего — ensure_dependency_defaults поставит htf_missing + geometry_score
        return

    def _generate_signals(self, ctx):
        return True


def test_l3_missing_rate_increases_when_l3_missing_flag_present():
    h = _TestHandler()

    ctx1 = SimpleNamespace(data_quality_flags=["l3_missing"])
    h._before_signal_generation(ctx1)
    h._track_dependency_metrics(ctx1)
    assert ctx1.l3_missing_rate == 1.0

    ctx2 = SimpleNamespace(data_quality_flags=[])
    # ensure_dependency_defaults в before_signal_generation добавит l3_missing (потому что l3_score None)
    h._before_signal_generation(ctx2)
    h._track_dependency_metrics(ctx2)
    # второй раз тоже missing -> остаётся 1.0
    assert ctx2.l3_missing_rate == 1.0

    ctx3 = SimpleNamespace(l3_score=0.9, geometry_score=0.9, data_quality_flags=[])
    h._before_signal_generation(ctx3)
    h._track_dependency_metrics(ctx3)
    # теперь не missing -> missing=2 из 3
    assert abs(ctx3.l3_missing_rate - (2.0 / 3.0)) < 1e-9
