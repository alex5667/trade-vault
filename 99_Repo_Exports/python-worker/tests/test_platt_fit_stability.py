#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_platt_fit_stability.py

Тесты для проверки стабильности PlattLogitCalibrator:
  - fit_platt_logit не даёт NaN
  - apply_one монотонен
  - детерминированность (одинаковые входы -> одинаковые выходы)
"""

from __future__ import annotations

import numpy as np
import pytest

from services.ml_calibration import fit_platt_logit, PlattLogitCalibrator


def test_platt_fit_no_nan():
    """Проверка, что fit_platt_logit не возвращает NaN."""
    # Синтетические данные
    n = 1000
    probs = np.random.rand(n).tolist()
    y = (np.random.rand(n) > 0.5).astype(int).tolist()

    cal = fit_platt_logit(probs, y, l2=1e-3, max_iter=50)

    assert not np.isnan(cal.a), f"cal.a is NaN"
    assert not np.isnan(cal.b), f"cal.b is NaN"
    assert not np.isinf(cal.a), f"cal.a is Inf"
    assert not np.isinf(cal.b), f"cal.b is Inf"


def test_platt_apply_one_monotonic():
    """Проверка монотонности apply_one."""
    cal = PlattLogitCalibrator(a=1.5, b=-0.1)

    # Тестируем на возрастающей последовательности
    probs = np.linspace(0.01, 0.99, 100)
    calibrated = [cal.apply_one(float(p)) for p in probs]

    # Проверка монотонности: если p1 < p2, то cal(p1) < cal(p2)
    for i in range(len(calibrated) - 1):
        assert calibrated[i] <= calibrated[i + 1] + 1e-9, (
            f"Не монотонно: cal({probs[i]})={calibrated[i]} > cal({probs[i+1]})={calibrated[i+1]}"
        )


def test_platt_apply_one_bounds():
    """Проверка, что apply_one возвращает значения в [0, 1]."""
    cal = PlattLogitCalibrator(a=2.0, b=-0.5)

    # Тестируем на граничных значениях
    test_probs = [0.0, 0.001, 0.1, 0.5, 0.9, 0.999, 1.0]

    for p in test_probs:
        cal_p = cal.apply_one(p)
        assert 0.0 <= cal_p <= 1.0, f"cal({p})={cal_p} вне диапазона [0, 1]"


def test_platt_fit_deterministic():
    """Проверка детерминированности fit_platt_logit."""
    # Фиксированный seed для воспроизводимости
    np.random.seed(42)
    n = 500
    probs = np.random.rand(n).tolist()
    y = (np.random.rand(n) > 0.5).astype(int).tolist()

    # Обучаем дважды
    cal1 = fit_platt_logit(probs, y, l2=1e-3, max_iter=50)
    cal2 = fit_platt_logit(probs, y, l2=1e-3, max_iter=50)

    # Параметры должны совпадать
    assert abs(cal1.a - cal2.a) < 1e-9, f"a не детерминирован: {cal1.a} vs {cal2.a}"
    assert abs(cal1.b - cal2.b) < 1e-9, f"b не детерминирован: {cal1.b} vs {cal2.b}"


def test_platt_fit_edge_cases():
    """Проверка на граничных случаях."""
    # Пустой список
    cal_empty = fit_platt_logit([], [])
    assert cal_empty.a == 1.0, "Пустой список должен давать default a=1.0"
    assert cal_empty.b == 0.0, "Пустой список должен давать default b=0.0"

    # Все нули
    cal_zeros = fit_platt_logit([0.0] * 10, [0] * 10, l2=1e-3, max_iter=50)
    assert not np.isnan(cal_zeros.a), "Все нули не должны давать NaN"
    assert not np.isnan(cal_zeros.b), "Все нули не должны давать NaN"

    # Все единицы
    cal_ones = fit_platt_logit([1.0] * 10, [1] * 10, l2=1e-3, max_iter=50)
    assert not np.isnan(cal_ones.a), "Все единицы не должны давать NaN"
    assert not np.isnan(cal_ones.b), "Все единицы не должны давать NaN"

    # Смешанные данные
    cal_mixed = fit_platt_logit([0.0, 0.5, 1.0], [0, 1, 1], l2=1e-3, max_iter=50)
    assert not np.isnan(cal_mixed.a), "Смешанные данные не должны давать NaN"
    assert not np.isnan(cal_mixed.b), "Смешанные данные не должны давать NaN"


def test_platt_apply_one_identity():
    """Проверка, что при a=1.0, b=0.0 калибровка близка к identity."""
    cal = PlattLogitCalibrator(a=1.0, b=0.0)

    test_probs = [0.1, 0.3, 0.5, 0.7, 0.9]
    for p in test_probs:
        cal_p = cal.apply_one(p)
        # При a=1.0, b=0.0 должно быть близко к identity (но не точно из-за численных погрешностей)
        assert abs(cal_p - p) < 0.1, f"При a=1.0, b=0.0 должно быть близко к identity: cal({p})={cal_p}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

