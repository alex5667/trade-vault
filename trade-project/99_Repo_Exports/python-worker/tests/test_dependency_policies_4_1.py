from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any

import pytest
import contextlib


# Минимальная версия для тестирования dependency policies
class BaseOrderflowHandler(ABC):
    def __init__(self):
        self.symbol = "BTCUSDT"
        self._last_hlc_fallback_used = False
        self._geometry_service = None

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

    def _attach_modular_services_data(self, ctx) -> None:
        """Attach data from new modular services to OrderflowContext"""
        # Get geometry data
        geometry_snapshot = None
        if self._geometry_service:
            geometry_snapshot = self._geometry_service.get_geometry(
                symbol=self.symbol,
                ts_event_ms=ctx.ts,
                price=ctx.price
            )
        if geometry_snapshot:
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


@dataclass
class Ctx:
    data_quality_flags: list[str] | None = None
    l3_score01: float | None = None
    l3_missing_rate: float | None = None
    geometry_score: float | None = None
    ts: int = 0
    price: float = 0.0


class _GeomSvc:
    def __init__(self, ret):
        self._ret = ret
    def get_geometry(self, symbol: str, ts_event_ms: int, price: float):
        return self._ret


class _H(BaseOrderflowHandler):
    def __init__(self):
        super().__init__()
        self._geometry_service = _GeomSvc(ret=None)


def test_hlc_fallback_flag_is_added_once():
    h = _H()
    ctx = Ctx()
    h._last_hlc_fallback_used = True

    h._mark_hlc_fallback_flag(ctx)
    h._mark_hlc_fallback_flag(ctx)
    assert ctx.data_quality_flags == ["hlc_fallback"]


def test_l3_missing_policy_sets_neutral_score_and_rate():
    h = _H()
    ctx = Ctx()

    h._mark_l3_missing_policy(ctx)
    assert ctx.l3_score01 == pytest.approx(0.5)
    assert ctx.l3_missing_rate == pytest.approx(1.0)
    assert "l3_missing" in (ctx.data_quality_flags or [])


def test_htf_missing_sets_neutral_geometry_score_and_flag():
    h = _H()
    ctx = Ctx(ts=123, price=100.0)
    # geometry_service already returns None in _H

    # вызываем реальный метод, который должен поставить neutral + флаг
    h._attach_modular_services_data(ctx)  # type: ignore[arg-type]
    assert ctx.geometry_score == pytest.approx(0.1)
    assert "htf_missing" in (ctx.data_quality_flags or [])
