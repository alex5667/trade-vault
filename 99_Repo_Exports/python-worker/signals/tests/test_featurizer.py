"""
Тесты для signals/featurizer.py

Rolling window statistics, classify_delta, OBI calculation.
"""
import pytest
from signals.featurizer import Rolling, classify_delta, obi_from_book, compute_rolling_metrics


# ─────────────────────── Rolling ───────────────────────

class TestRolling:
    def test_empty_mean_returns_none(self):
        r = Rolling(size=10)
        assert r.mean() is None

    def test_empty_std_returns_none(self):
        r = Rolling(size=10)
        assert r.std() is None

    def test_mean_single_value(self):
        r = Rolling(size=10)
        r.add(5.0)
        assert r.mean() == pytest.approx(5.0)

    def test_std_single_value_returns_none(self):
        r = Rolling(size=10)
        r.add(5.0)
        assert r.std() is None  # n < 2

    def test_mean_multiple_values(self):
        r = Rolling(size=10)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            r.add(v)
        assert r.mean() == pytest.approx(3.0)

    def test_std_uniform_values(self):
        r = Rolling(size=10)
        for _ in range(5):
            r.add(1.0)
        # Все одинаковые → std должен быть 0
        assert r.std() == pytest.approx(0.0, abs=1e-9)

    def test_window_slide(self):
        r = Rolling(size=3)
        r.add(1.0)
        r.add(2.0)
        r.add(3.0)
        r.add(10.0)  # Вытесняет 1.0
        # mean = (2 + 3 + 10) / 3 = 5.0
        assert r.mean() == pytest.approx(5.0)

    def test_len(self):
        r = Rolling(size=5)
        assert len(r) == 0
        r.add(1.0)
        r.add(2.0)
        assert len(r) == 2

    def test_len_does_not_exceed_size(self):
        r = Rolling(size=3)
        for i in range(10):
            r.add(float(i))
        assert len(r) == 3

    def test_sum_consistency_after_slide(self):
        """Проверяем онлайн-алгоритм: sum корректен после вытеснения."""
        r = Rolling(size=4)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:  # 5 вытесняет 1
            r.add(v)
        # buf = [2, 3, 4, 5], sum = 14, mean = 3.5
        assert r.mean() == pytest.approx(3.5)


# ─────────────────────── classify_delta ───────────────────────

class TestClassifyDelta:
    def test_buy_when_last_equals_ask(self):
        tick = {"bid": 100.0, "ask": 101.0, "last": 101.0, "volume": 5.0}
        assert classify_delta(tick) == pytest.approx(5.0)

    def test_buy_when_last_above_ask(self):
        tick = {"bid": 100.0, "ask": 101.0, "last": 101.5, "volume": 3.0}
        assert classify_delta(tick) == pytest.approx(3.0)

    def test_sell_when_last_equals_bid(self):
        tick = {"bid": 100.0, "ask": 101.0, "last": 100.0, "volume": 5.0}
        assert classify_delta(tick) == pytest.approx(-5.0)

    def test_sell_when_last_below_bid(self):
        tick = {"bid": 100.0, "ask": 101.0, "last": 99.5, "volume": 3.0}
        assert classify_delta(tick) == pytest.approx(-3.0)

    def test_fallback_buy_when_ask_gt_bid(self):
        # mid-spread: fallback по spread
        tick = {"bid": 100.0, "ask": 101.0, "last": 100.5, "volume": 4.0}
        assert classify_delta(tick) == pytest.approx(4.0)

    def test_zero_volume(self):
        tick = {"bid": 100.0, "ask": 101.0, "last": 101.0, "volume": 0.0}
        assert classify_delta(tick) == pytest.approx(0.0)

    def test_missing_price_fields(self):
        # Нет bid/ask — fallback возвращает ±volume зависит от ask/bid
        tick = {"volume": 5.0}
        result = classify_delta(tick)
        # Не должен бросать исключение
        assert isinstance(result, float)


# ─────────────────────── obi_from_book (featurizer module) ───────────────────────

class TestObiFromBookFeaturizer:
    """Тесты OBI в featurizer.py (отдельная реализация с сортировкой уровней)."""

    def test_none_input_returns_none(self):
        assert obi_from_book(None) is None

    def test_empty_input_returns_none(self):
        assert obi_from_book({}) is None

    def test_balanced_book(self):
        book = {
            "bids": [[100.0, 10.0], [99.0, 10.0]],
            "asks": [[101.0, 10.0], [102.0, 10.0]],
        }
        result = obi_from_book(book)
        assert result == pytest.approx(0.0, abs=1e-6)

    def test_bid_dominated(self):
        book = {
            "bids": [[100.0, 80.0]],
            "asks": [[101.0, 20.0]],
        }
        result = obi_from_book(book)
        # (80-20)/100 = 0.6
        assert result == pytest.approx(0.6, abs=1e-6)

    def test_ask_dominated(self):
        book = {
            "bids": [[100.0, 20.0]],
            "asks": [[101.0, 80.0]],
        }
        result = obi_from_book(book)
        assert result == pytest.approx(-0.6, abs=1e-6)


# ─────────────────────── compute_rolling_metrics ───────────────────────

class TestComputeRollingMetrics:
    def test_empty_list_returns_defaults(self):
        m, s = compute_rolling_metrics([])
        assert m == pytest.approx(0.0)
        assert s == pytest.approx(1.0)

    def test_single_value_returns_defaults(self):
        m, s = compute_rolling_metrics([5.0])
        assert m == pytest.approx(0.0)
        assert s == pytest.approx(1.0)

    def test_normal_calculation(self):
        data = [1.0, 2.0, 3.0, 4.0, 5.0]
        m, s = compute_rolling_metrics(data)
        assert m == pytest.approx(3.0, rel=1e-4)
        assert s > 0

    def test_window_truncation(self):
        """Только последние N значений используются."""
        data = list(range(200))  # 0..199
        m, s = compute_rolling_metrics(data, window=10)
        # mean = mean(190..199) = 194.5
        assert m == pytest.approx(194.5, rel=1e-4)

    def test_std_nonzero_for_varied_data(self):
        data = [1.0, 5.0, 10.0, 2.0, 8.0]
        m, s = compute_rolling_metrics(data)
        assert s > 0.0
