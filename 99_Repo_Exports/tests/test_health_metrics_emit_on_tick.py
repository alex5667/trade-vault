# tests/test_health_metrics_emit_on_tick.py
"""
Тесты для интеграции HealthMetrics.on_tick() в orderflow handler.
Проверяем корректность вызова с нужными параметрами и устойчивость к отсутствующим полям.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
import math

import pytest

# Добавляем python-worker в path для импорта
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

try:
    from handlers.base_orderflow_handler import _emit_health_on_tick, _to_float_or_nan, _to_opt_float
except ImportError as e:
    pytest.skip(f"Cannot import handlers: {e}", allow_module_level=True)


class FakeHealthMetrics:
    """Mock HealthMetrics для тестирования без Redis"""
    def __init__(self):
        self.calls = []

    def on_tick(self, **kwargs):
        self.calls.append(kwargs)


def test_to_float_or_nan_happy_path():
    """Тест _to_float_or_nan с валидными значениями"""
    assert _to_float_or_nan(12.5) == 12.5
    assert _to_float_or_nan(7) == 7.0
    assert _to_float_or_nan("10.3") == 10.3


def test_to_float_or_nan_none_and_invalid():
    """Тест _to_float_or_nan с None и невалидными значениями"""
    assert math.isnan(_to_float_or_nan(None))
    assert math.isnan(_to_float_or_nan("invalid"))
    assert math.isnan(_to_float_or_nan({}))


def test_to_opt_float_happy_path():
    """Тест _to_opt_float с валидными значениями"""
    assert _to_opt_float(150.0) == 150.0
    assert _to_opt_float(1.25) == 1.25
    assert _to_opt_float("0.35") == 0.35


def test_to_opt_float_none_and_invalid():
    """Тест _to_opt_float с None и невалидными значениями"""
    assert _to_opt_float(None) is None
    assert _to_opt_float("invalid") is None
    assert _to_opt_float(float("nan")) is None


def test_emit_health_on_tick_happy_path():
    """Тест _emit_health_on_tick с полными данными L2 метрик"""
    hm = FakeHealthMetrics()
    ctx = SimpleNamespace(
        l2_age_ms=12.5,
        l2_age_ms_tick=7.0,
        l2_is_stale=True,
        l2_is_stale_now=False,
        eta_fill_ms=150.0,
        burst_ratio=1.25,
        imbalance_min=0.35,
    )
    _emit_health_on_tick(hm, symbol="BTCUSDT", ctx=ctx)
    assert len(hm.calls) == 1
    call = hm.calls[0]
    assert call["symbol"] == "BTCUSDT"
    assert call["l2_age_ms"] == 12.5
    assert call["l2_age_ms_tick"] == 7.0
    assert call["l2_is_stale"] is True
    assert call["l2_is_stale_now"] is False
    assert call["eta_fill_ms"] == 150.0
    assert call["burst_ratio"] == 1.25
    assert call["imbalance_min"] == 0.35


def test_emit_health_on_tick_missing_fields_is_safe_and_sends_nan():
    """Тест устойчивости _emit_health_on_tick к отсутствующим полям (fail-open)"""
    hm = FakeHealthMetrics()
    ctx = SimpleNamespace()  # ничего нет
    _emit_health_on_tick(hm, symbol="ETHUSDT", ctx=ctx)
    assert len(hm.calls) == 1
    call = hm.calls[0]
    assert call["symbol"] == "ETHUSDT"
    assert math.isnan(call["l2_age_ms"])
    assert math.isnan(call["l2_age_ms_tick"])
    assert call["l2_is_stale"] is False
    assert call["l2_is_stale_now"] is False
    assert call["eta_fill_ms"] is None
    assert call["burst_ratio"] is None
    assert call["imbalance_min"] is None


def test_emit_health_on_tick_fallback_field_names():
    """Тест fallback на альтернативные имена полей (L2AgeMsNow, L2AgeMsTick)"""
    hm = FakeHealthMetrics()
    ctx = SimpleNamespace(
        L2AgeMsNow=15.0,  # fallback имя
        L2AgeMsTick=8.0,  # fallback имя
        L2IsStale=True,  # fallback имя
        L2IsStaleNow=True,  # fallback имя
    )
    _emit_health_on_tick(hm, symbol="SOLUSDT", ctx=ctx)
    assert len(hm.calls) == 1
    call = hm.calls[0]
    assert call["l2_age_ms"] == 15.0
    assert call["l2_age_ms_tick"] == 8.0
    assert call["l2_is_stale"] is True
    assert call["l2_is_stale_now"] is True


def test_emit_health_on_tick_no_call_if_hm_none():
    """Тест что _emit_health_on_tick не падает если health_metrics=None"""
    ctx = SimpleNamespace(l2_age_ms=10.0)
    _emit_health_on_tick(None, symbol="BTCUSDT", ctx=ctx)  # не должно падать


def test_emit_health_on_tick_no_call_if_symbol_empty():
    """Тест что _emit_health_on_tick не падает если symbol пустой"""
    hm = FakeHealthMetrics()
    ctx = SimpleNamespace(l2_age_ms=10.0)
    _emit_health_on_tick(hm, symbol="", ctx=ctx)  # не должно падать
    assert len(hm.calls) == 0  # не должно быть вызовов


def test_emit_health_on_tick_partial_fields():
    """Тест с частичными данными (только обязательные поля)"""
    hm = FakeHealthMetrics()
    ctx = SimpleNamespace(
        l2_age_ms=20.0,
        l2_age_ms_tick=5.0,
        l2_is_stale=False,
        # l2_is_stale_now отсутствует - должен fallback на False
    )
    _emit_health_on_tick(hm, symbol="ADAUSDT", ctx=ctx)
    assert len(hm.calls) == 1
    call = hm.calls[0]
    assert call["l2_age_ms"] == 20.0
    assert call["l2_age_ms_tick"] == 5.0
    assert call["l2_is_stale"] is False
    assert call["l2_is_stale_now"] is False  # fallback
    assert call["eta_fill_ms"] is None
    assert call["burst_ratio"] is None
    assert call["imbalance_min"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

