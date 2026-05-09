"""
Unit тесты для signals/detectors.py

Тестирование:
- Z-score расчет для delta spike
- Weak progress условие
- Реальный OBI из Order Book
- Absorption condition
- Sustained OBI
- Delta classification

Запуск:
    pytest tests/test_detectors.py -v
    pytest tests/test_detectors.py::test_zscore_spike -v
"""

import pytest

from signals.detectors import (
    classify_delta_by_aggressor,
    is_absorption,
    obi_from_book,
    obi_is_sustained,
    weak_progress,
    zscore,
)


class TestZScore:
    """Тесты для Z-score расчета."""

    def test_zscore_spike(self):
        """Тест детекции spike через Z-score."""
        # Окно со стабильными значениями и один spike
        window = [0.0] * 50 + [1.0] * 50
        latest = 10.0  # Сильный spike

        z = zscore(latest, window)
        assert z > 3.0, f"Z-score должен быть > 3.0 для spike, получено {z}"

    def test_zscore_normal(self):
        """Тест что нормальные значения дают низкий Z-score."""
        window = [float(i) for i in range(100)]
        latest = 50.0  # Среднее значение

        z = zscore(latest, window)
        assert abs(z) < 0.5, f"Z-score должен быть близок к 0 для среднего, получено {z}"

    def test_zscore_insufficient_data(self):
        """Тест что маленькое окно возвращает 0."""
        window = [1.0, 2.0, 3.0]  # Меньше 30 значений
        latest = 10.0

        z = zscore(latest, window)
        assert z == 0.0, "Z-score должен быть 0 при недостаточных данных"

    def test_zscore_empty_window(self):
        """Тест обработки пустого окна."""
        window = []
        latest = 5.0

        z = zscore(latest, window)
        assert z == 0.0, "Z-score должен быть 0 для пустого окна"


class TestWeakProgress:
    """Тесты для weak progress детекции."""

    def test_weak_progress_true(self):
        """Тест детекции слабого прогресса."""
        # Диапазон 0.2, ATR 2.0 → 0.1 <= 0.15
        assert weak_progress(bar_range=0.2, atr=2.0, threshold=0.15)

    def test_weak_progress_false(self):
        """Тест что сильный прогресс не проходит."""
        # Диапазон 1.0, ATR 2.0 → 0.5 > 0.15
        assert not weak_progress(bar_range=1.0, atr=2.0, threshold=0.15)

    def test_weak_progress_zero_atr(self):
        """Тест обработки нулевого ATR."""
        assert not weak_progress(bar_range=0.5, atr=0.0, threshold=0.10)

    def test_weak_progress_exact_threshold(self):
        """Тест граничного случая."""
        # 0.2 / 2.0 = 0.10, порог 0.10
        assert weak_progress(bar_range=0.2, atr=2.0, threshold=0.10)


class TestOBIFromBook:
    """Тесты для расчета реального OBI из Order Book."""

    def test_obi_strong_bid_imbalance(self):
        """Тест сильного преобладания bid."""
        book = {
            "bids": [[100.0, 50.0], [99.9, 40.0], [99.8, 10.0]],  # 100 total
            "asks": [[100.1, 10.0], [100.2, 10.0], [100.3, 10.0]]  # 30 total
        }
        obi = obi_from_book(book, depth=3)

        # (100 - 30) / (100 + 30) = 70/130 ≈ 0.538
        assert obi > 0.5, f"OBI должен показывать bid преобладание, получено {obi}"
        assert obi < 1.0, "OBI должен быть < 1.0"

    def test_obi_strong_ask_imbalance(self):
        """Тест сильного преобладания ask."""
        book = {
            "bids": [[100.0, 10.0], [99.9, 10.0]],  # 20 total
            "asks": [[100.1, 50.0], [100.2, 50.0], [100.3, 50.0]]  # 150 total
        }
        obi = obi_from_book(book, depth=5)

        # (20 - 150) / (20 + 150) = -130/170 ≈ -0.765
        assert obi < -0.5, f"OBI должен показывать ask преобладание, получено {obi}"
        assert obi > -1.0, "OBI должен быть > -1.0"

    def test_obi_balanced(self):
        """Тест балансированного стакана."""
        book = {
            "bids": [[100.0, 50.0], [99.9, 50.0]],
            "asks": [[100.1, 50.0], [100.2, 50.0]]
        }
        obi = obi_from_book(book, depth=5)

        # (100 - 100) / 200 = 0
        assert obi == 0.0, f"OBI должен быть 0 для балансированного стакана, получено {obi}"

    def test_obi_empty_book(self):
        """Тест обработки пустого стакана."""
        book = None
        obi = obi_from_book(book, depth=5)

        assert obi is None, "OBI должен быть None для пустого book"

    def test_obi_no_bids(self):
        """Тест стакана без bids."""
        book = {
            "bids": [],
            "asks": [[100.1, 50.0]]
        }
        obi = obi_from_book(book, depth=5)

        # (0 - 50) / 50 = -1
        assert obi == -1.0, "OBI должен быть -1 когда только asks"

    def test_obi_depth_limiting(self):
        """Тест ограничения глубины."""
        book = {
            "bids": [[100.0, 100.0]] * 10,  # 10 уровней по 100
            "asks": [[100.1, 10.0]] * 10    # 10 уровней по 10
        }

        # С depth=3: (300 - 30) / 330 = 0.818
        obi_3 = obi_from_book(book, depth=3)
        # С depth=10: (1000 - 100) / 1100 = 0.818
        obi_10 = obi_from_book(book, depth=10)

        assert obi_3 == obi_10, "OBI должен быть одинаковым (все уровни равны)"


class TestAbsorption:
    """Тесты для absorption condition."""

    def test_absorption_all_conditions_met(self):
        """Тест что absorption детектируется при всех условиях."""
        assert is_absorption(z=3.5, weak=True, near_level=True, z_threshold=3.0)

    def test_absorption_low_z(self):
        """Тест что низкий Z-score не проходит."""
        assert not is_absorption(z=2.0, weak=True, near_level=True, z_threshold=3.0)

    def test_absorption_strong_progress(self):
        """Тест что сильный прогресс не проходит."""
        assert not is_absorption(z=4.0, weak=False, near_level=True, z_threshold=3.0)

    def test_absorption_far_from_level(self):
        """Тест что далеко от уровня не проходит."""
        assert not is_absorption(z=4.0, weak=True, near_level=False, z_threshold=3.0)

    def test_absorption_custom_threshold(self):
        """Тест с кастомным порогом."""
        assert is_absorption(z=2.5, weak=True, near_level=True, z_threshold=2.0)
        assert not is_absorption(z=2.5, weak=True, near_level=True, z_threshold=3.0)


class TestOBISustained:
    """Тесты для sustained OBI."""

    def test_sustained_obi_positive(self):
        """Тест устойчивого положительного OBI."""
        buffer = [(1000 + i, 0.6) for i in range(10)]  # 10 значений ~0.6
        assert obi_is_sustained(buffer, threshold=0.5)

    def test_sustained_obi_negative(self):
        """Тест устойчивого отрицательного OBI."""
        buffer = [(1000 + i, -0.7) for i in range(10)]
        assert obi_is_sustained(buffer, threshold=0.5)

    def test_not_sustained(self):
        """Тест неустойчивого OBI."""
        buffer = [(1000 + i, 0.3) for i in range(10)]  # Среднее 0.3 < 0.5
        assert not obi_is_sustained(buffer, threshold=0.5)

    def test_empty_buffer(self):
        """Тест пустого буфера."""
        buffer = []
        assert not obi_is_sustained(buffer, threshold=0.5)


class TestDeltaClassification:
    """Тесты для классификации направления сделок."""

    def test_aggressive_buy(self):
        """Тест агрессивной покупки (last >= ask)."""
        delta = classify_delta_by_aggressor(
            last=1880.75,
            bid=1880.50,
            ask=1880.75,
            volume=10.0
        )
        assert delta == 10.0, "Должна быть агрессивная покупка (+volume)"

    def test_aggressive_sell(self):
        """Тест агрессивной продажи (last <= bid)."""
        delta = classify_delta_by_aggressor(
            last=1880.50,
            bid=1880.50,
            ask=1880.75,
            volume=10.0
        )
        assert delta == -10.0, "Должна быть агрессивная продажа (-volume)"

    def test_inside_spread(self):
        """Тест сделки внутри spread (fallback)."""
        delta = classify_delta_by_aggressor(
            last=1880.60,  # Между bid и ask
            bid=1880.50,
            ask=1880.75,
            volume=10.0
        )
        # Fallback: ask > bid → +volume
        assert delta == 10.0, "Fallback должен определить направление по spread"


# Фикстуры для интеграционных тестов (опционально)
@pytest.fixture
def sample_orderbook():
    """Пример Order Book для тестов."""
    return {
        "ts": 1698765432000,
        "symbol": "",
        "bids": [
            [1880.50, 100.0],
            [1880.45, 80.0],
            [1880.40, 50.0],
            [1880.35, 30.0],
            [1880.30, 20.0]
        ],
        "asks": [
            [1880.75, 40.0],
            [1880.80, 30.0],
            [1880.85, 20.0],
            [1880.90, 15.0],
            [1880.95, 10.0]
        ]
    }


def test_full_obi_calculation(sample_orderbook):
    """Интеграционный тест полного расчета OBI."""
    obi = obi_from_book(sample_orderbook, depth=5)

    # Bid volume: 100+80+50+30+20 = 280
    # Ask volume: 40+30+20+15+10 = 115
    # OBI = (280-115)/(280+115) = 165/395 ≈ 0.418

    assert obi > 0.40, f"OBI должен показывать bid преобладание, получено {obi}"
    assert obi < 0.45, f"OBI должен быть около 0.42, получено {obi}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

