"""
Тесты для signals/detectors.py

Чистые функции, нет зависимостей от Redis/конфигурации.
"""
import pytest
from signals.detectors import (
    zscore,
    weak_progress,
    obi_from_book,
    is_absorption,
    obi_is_sustained,
    classify_delta_by_aggressor,
)


# ─────────────────────── zscore ───────────────────────

def test_zscore_empty_window_returns_zero():
    assert zscore(5.0, []) == 0.0


def test_zscore_short_window_returns_zero():
    # Требуется минимум 30 значений
    assert zscore(5.0, [1.0] * 29) == 0.0


def test_zscore_uniform_window_returns_zero():
    # Все значения одинаковы → std=0 → возвращаем 0
    assert zscore(1.0, [1.0] * 30) == 0.0


def test_zscore_normal_calculation():
    # Стандартное распределение: mean=0, std~=1
    import random
    random.seed(42)
    window = [random.gauss(0, 1) for _ in range(100)]
    z = zscore(3.0, window)
    # |z| должен быть > 2 для значения в 3 сигма
    assert abs(z) > 2.0


def test_zscore_positive_outlier():
    window = [0.0] * 30
    # Значение 10 при mean=0, std→near 0 → capped is 0.0 (std=0 case)
    # Используем нетривиальные данные
    window = [float(i) for i in range(30)]  # 0..29, mean=14.5
    z = zscore(100.0, window)
    assert z > 0


def test_zscore_negative_outlier():
    window = [float(i) for i in range(30)]  # mean=14.5
    z = zscore(-10.0, window)
    assert z < 0


# ─────────────────────── weak_progress ───────────────────────

def test_weak_progress_zero_atr_returns_false():
    assert weak_progress(0.5, 0.0) is False
    assert weak_progress(0.5, -1.0) is False


def test_weak_progress_true_when_below_threshold():
    # bar_range/atr = 0.05 <= 0.10 (default threshold)
    assert weak_progress(0.05, 1.0, threshold=0.10) is True


def test_weak_progress_false_when_above_threshold():
    # bar_range/atr = 0.50 > 0.10
    assert weak_progress(0.50, 1.0, threshold=0.10) is False


def test_weak_progress_exactly_on_threshold():
    assert weak_progress(0.10, 1.0, threshold=0.10) is True


def test_weak_progress_custom_threshold():
    assert weak_progress(0.25, 1.0, threshold=0.30) is True
    assert weak_progress(0.35, 1.0, threshold=0.30) is False


# ─────────────────────── obi_from_book ───────────────────────

def test_obi_from_book_none_returns_none():
    assert obi_from_book(None) is None


def test_obi_from_book_empty_dict_returns_none():
    assert obi_from_book({}) is None


def test_obi_from_book_empty_levels():
    book = {"bids": [], "asks": []}
    # Нет объёма → total=0 → 0.0
    result = obi_from_book(book)
    assert result == 0.0


def test_obi_from_book_balanced():
    book = {
        "bids": [[100.0, 10.0], [99.0, 10.0]],
        "asks": [[101.0, 10.0], [102.0, 10.0]],
    }
    result = obi_from_book(book, depth=5)
    assert result == pytest.approx(0.0, abs=1e-6)


def test_obi_from_book_bid_heavy():
    book = {
        "bids": [[100.0, 90.0]],
        "asks": [[101.0, 10.0]],
    }
    result = obi_from_book(book)
    # (90-10)/(90+10) = 0.8
    assert result == pytest.approx(0.8, abs=1e-6)


def test_obi_from_book_ask_heavy():
    book = {
        "bids": [[100.0, 10.0]],
        "asks": [[101.0, 90.0]],
    }
    result = obi_from_book(book)
    # (10-90)/100 = -0.8
    assert result == pytest.approx(-0.8, abs=1e-6)


def test_obi_from_book_depth_limit():
    book = {
        "bids": [[100.0, 10.0], [99.0, 10.0], [98.0, 10.0]],
        "asks": [[101.0, 10.0]],
    }
    # depth=1: only best bid (10) vs best ask (10)
    result = obi_from_book(book, depth=1)
    assert result == pytest.approx(0.0, abs=1e-6)


# ─────────────────────── is_absorption ───────────────────────

def test_is_absorption_all_conditions_met():
    assert is_absorption(z=4.0, weak=True, near_level=True, z_threshold=3.0) is True


def test_is_absorption_no_weak_progress():
    assert is_absorption(z=4.0, weak=False, near_level=True) is False


def test_is_absorption_not_near_level():
    assert is_absorption(z=4.0, weak=True, near_level=False) is False


def test_is_absorption_z_below_threshold():
    assert is_absorption(z=2.0, weak=True, near_level=True, z_threshold=3.0) is False


def test_is_absorption_negative_z_spike():
    # Отрицательный z-score (агрессивные продажи) тоже защита  
    assert is_absorption(z=-4.0, weak=True, near_level=True, z_threshold=3.0) is True


# ─────────────────────── obi_is_sustained ───────────────────────

def test_obi_is_sustained_empty_returns_false():
    assert obi_is_sustained([]) is False


def test_obi_is_sustained_high_positive():
    buf = [(1000, 0.7), (1001, 0.8), (1002, 0.6)]
    assert obi_is_sustained(buf, threshold=0.5) is True


def test_obi_is_sustained_low_average():
    buf = [(1000, 0.2), (1001, -0.1), (1002, 0.3)]
    assert obi_is_sustained(buf, threshold=0.5) is False


# ─────────────────────── classify_delta_by_aggressor ───────────────────────

def test_classify_buy_when_last_gte_ask():
    result = classify_delta_by_aggressor(last=101.0, bid=100.0, ask=101.0, volume=5.0)
    assert result == pytest.approx(5.0)


def test_classify_sell_when_last_lte_bid():
    result = classify_delta_by_aggressor(last=100.0, bid=100.0, ask=101.0, volume=5.0)
    assert result == pytest.approx(-5.0)


def test_classify_fallback_buy():
    # mid-spread → fallback: ask > bid → +volume
    result = classify_delta_by_aggressor(last=100.5, bid=100.0, ask=101.0, volume=5.0)
    assert result == pytest.approx(5.0)
