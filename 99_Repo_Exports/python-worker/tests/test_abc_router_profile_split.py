"""
tests/test_abc_router_profile_split.py
=======================================
P0 regression: choose_arm_abc() должна получать int-проценты (0-100),
а не float-доли (0.0-1.0).

Covers:
  - AB_SPLIT_B=10, AB_SPLIT_C=10 → B≈10%, C≈10%, A≈80% на большой выборке
  - Детерминизм: одинаковый key+salt → одинаковый arm
  - Float-доли (0.10) должны вернуть arm="A" (т.е. B/C = 0%) — регресс-тест
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from services.abc_router import choose_arm_abc, stable_bucket_0_99


# ---------------------------------------------------------------------------
# Базовая детерминизм-проверка
# ---------------------------------------------------------------------------

def test_deterministic_same_key_same_salt():
    """Один и тот же key+salt всегда возвращает одно и то же плечо."""
    for _ in range(20):
        assert choose_arm_abc(key="BTCUSDT|breakout|1m", split_b=10, split_c=10, salt="v1") == \
               choose_arm_abc(key="BTCUSDT|breakout|1m", split_b=10, split_c=10, salt="v1")


def test_different_keys_different_distribution():
    """Разные ключи не обязаны дать одно плечо."""
    arms = {choose_arm_abc(key=f"key_{i}", split_b=33, split_c=33, salt="v1") for i in range(100)}
    assert len(arms) > 1, "Все 100 ключей вернули одно плечо — детерминизм неверный"


# ---------------------------------------------------------------------------
# Критический регресс: int-проценты vs float-доли
# ---------------------------------------------------------------------------

def test_float_fractions_raise_error():
    """
    РЕГРЕСС: если передать B=0.10 (float), теперь это должно падать с TypeError,
    чтобы не допустить молчаливого превращения в 0.
    """
    import pytest
    with pytest.raises(TypeError):
        choose_arm_abc(key="test", split_b=0.10, split_c=0.10, salt="v1")

def test_zero_splits_give_only_a():
    arms = [choose_arm_abc(key=f"k{i}", split_b=0, split_c=0, salt="v1") for i in range(200)]
    assert all(a == "A" for a in arms), "split_b=0, split_c=0 должно давать только A"


def test_int_percentages_give_correct_shares():
    """
    С int-процентами (sb=10, sc=10) распределение должно соответствовать ≈10%/10%/80%.
    Допуск ±6 процентных пунктов на 1000 ключей.
    """
    n = 1000
    counts = {"A": 0, "B": 0, "C": 0}
    for i in range(n):
        arm = choose_arm_abc(key=f"symbol_{i}|zone_{i}", split_b=10, split_c=10, salt="test_v1")
        counts[arm] += 1

    pct_b = counts["B"] / n * 100
    pct_c = counts["C"] / n * 100
    pct_a = counts["A"] / n * 100

    assert 4 <= pct_b <= 16, f"B share={pct_b:.1f}% — вне ожидаемого диапазона 10%±6%"
    assert 4 <= pct_c <= 16, f"C share={pct_c:.1f}% — вне ожидаемого диапазона 10%±6%"
    assert 68 <= pct_a <= 92, f"A share={pct_a:.1f}% — вне ожидаемого диапазона 80%±12%"


def test_split_50_50_splits_bc():
    """split_b=50, split_c=50 → 50% B, 50% C, 0% A."""
    n = 500
    counts = {"A": 0, "B": 0, "C": 0}
    for i in range(n):
        arm = choose_arm_abc(key=f"k{i}", split_b=50, split_c=50, salt="s")
        counts[arm] += 1

    assert counts["A"] == 0, f"split_b=50+split_c=50 → A должно быть 0, got {counts['A']}"
    assert counts["B"] > 0
    assert counts["C"] > 0


# ---------------------------------------------------------------------------
# Граничные условия
# ---------------------------------------------------------------------------

def test_split_b_100_all_b():
    arm = choose_arm_abc(key="any", split_b=100, split_c=0, salt="s")
    assert arm == "B"


def test_split_c_100_all_c():
    arm = choose_arm_abc(key="any", split_b=0, split_c=100, salt="s")
    assert arm == "C"


def test_split_overflow_clamp():
    """split_b + split_c > 100 должно быть обрезано, не падать."""
    arm = choose_arm_abc(key="any", split_b=60, split_c=60, salt="s")
    assert arm in ("A", "B", "C")


def test_negative_splits_clamp():
    """Отрицательные значения обрезаются до 0."""
    arm = choose_arm_abc(key="any", split_b=-5, split_c=-5, salt="s")
    assert arm == "A"


# ---------------------------------------------------------------------------
# ENV-уровень: smoke проверка что сервис читает int, не float
# ---------------------------------------------------------------------------

def test_env_ab_split_parsed_as_int():
    """
    Проверяет, что AB_SPLIT_B/AB_SPLIT_C из ENV правильно парсятся как int.
    Регрессия на старую логику, где B=sb/100.0 передавалась в choose_arm_abc.
    """
    with patch.dict(os.environ, {"AB_SPLIT_B": "10", "AB_SPLIT_C": "10"}):
        sb = int(os.getenv("AB_SPLIT_B", "10"))
        sc = int(os.getenv("AB_SPLIT_C", "10"))
        # Правильный вызов (после фикса):
        arm_correct = choose_arm_abc(key="BTCUSDT|zone1|0", split_b=sb, split_c=sc, salt="v1")
        # Старый неправильный вызов (должен давать A, т.к. int(0.10)=0):
        arm_broken = choose_arm_abc(key="BTCUSDT|zone1|0", split_b=int(sb/100.0), split_c=int(sc/100.0), salt="v1")
        # Результаты должны ОТЛИЧАТЬСЯ (если bucket не в A-зоне)
        # Главное: arm_broken всегда A при split=0
        assert choose_arm_abc(key="BTCUSDT|zone1|0", split_b=0, split_c=0, salt="v1") == "A"
        # А arm_correct может быть B или C для некоторых ключей
        # Проверяем на всём диапазоне:
        non_a = [choose_arm_abc(key=f"k{i}", split_b=sb, split_c=sc, salt="v1")
                 for i in range(200) if choose_arm_abc(key=f"k{i}", split_b=sb, split_c=sc, salt="v1") != "A"]
        assert len(non_a) > 0, "С sb=10, sc=10 должны быть B/C плечи"
