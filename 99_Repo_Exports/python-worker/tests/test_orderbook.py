"""
Unit тесты для signals/orderbook_metrics.py

Тестирование:
- Best level tracking
- Iceberg detection via duration + refresh count
- Volume refresh logic
- Edge cases

Запуск:
    pytest tests/test_orderbook.py -v
"""

import pytest

from signals.orderbook_metrics import BestLevelTracker


class TestBestLevelTracker:
    """Тесты для BestLevelTracker."""

    def test_iceberg_refresh_and_duration(self):
        """
        Тест детекции iceberg через duration + refresh count.
        
        Сценарий:
        - Best bid держится на 100.0
        - Объем уменьшается (исполнение)
        - Объем увеличивается (refresh #1)
        - Снова уменьшается
        - Снова увеличивается (refresh #2)
        - Длительность > 1000ms, refresh >= 2 → iceberg!
        """
        tracker = BestLevelTracker(
            min_duration_ms=1000,
            refresh_min_abs=1.0,
            refresh_count_target=2
        )

        ts = 0

        # Snapshot 1: начальный объем
        book1 = {"bids": [[100.0, 10.0]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book1, ts := ts + 100)

        # Snapshot 2: объем уменьшился (исполнение)
        book2 = {"bids": [[100.0, 8.0]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book2, ts := ts + 200)

        # Snapshot 3: объем увеличился (refresh #1)
        book3 = {"bids": [[100.0, 10.5]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book3, ts := ts + 200)
        assert tracker.bid.refresh == 1, "Должен быть 1 refresh"

        # Snapshot 4: снова уменьшение
        book4 = {"bids": [[100.0, 9.0]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book4, ts := ts + 200)

        # Snapshot 5: снова увеличение (refresh #2)
        book5 = {"bids": [[100.0, 11.2]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book5, ts := ts + 400)  # Total time ~1.1 sec

        assert tracker.bid.refresh == 2, "Должно быть 2 refresh"
        assert tracker.is_iceberg("bid", ts), "Должен детектировать iceberg"

    def test_price_change_resets_state(self):
        """Тест что изменение цены сбрасывает состояние."""
        tracker = BestLevelTracker()

        ts = 0

        # Уровень на 100.0
        book1 = {"bids": [[100.0, 10.0]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book1, ts := ts + 100)

        # Refresh
        book2 = {"bids": [[100.0, 8.0]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book2, ts := ts + 200)
        book3 = {"bids": [[100.0, 11.0]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book3, ts := ts + 200)

        assert tracker.bid.refresh == 1

        # Цена изменилась - должен сброситься
        book4 = {"bids": [[99.95, 10.0]], "asks": [[100.1, 10.0]]}
        tracker.feed_book(book4, ts := ts + 200)

        assert tracker.bid.price == 99.95, "Цена должна обновиться"
        assert tracker.bid.refresh == 0, "Refresh count должен сброситься"
        assert tracker.bid.since_ms == ts, "since_ms должен обновиться"

    def test_insufficient_duration_no_iceberg(self):
        """Тест что недостаточная длительность не дает iceberg."""
        tracker = BestLevelTracker(
            min_duration_ms=2000,  # Требуется 2 секунды
            refresh_count_target=2
        )

        ts = 0

        # Быстро проходим refresh-и за 500ms
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)
        tracker.feed_book({"bids": [[100.0, 8.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)
        tracker.feed_book({"bids": [[100.0, 11.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)  # refresh 1
        tracker.feed_book({"bids": [[100.0, 9.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)
        tracker.feed_book({"bids": [[100.0, 12.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)  # refresh 2

        # Total time = 500ms < 2000ms
        assert tracker.bid.refresh == 2, "Должно быть 2 refresh"
        assert not tracker.is_iceberg("bid", ts), "Недостаточная длительность для iceberg"

    def test_insufficient_refreshes_no_iceberg(self):
        """Тест что недостаточно refresh-ей не дает iceberg."""
        tracker = BestLevelTracker(
            min_duration_ms=1000,
            refresh_count_target=3  # Требуется 3 refresh
        )

        ts = 0

        # Длительность достаточная, но только 2 refresh
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)
        tracker.feed_book({"bids": [[100.0, 8.0]], "asks": [[100.1, 10.0]]}, ts := ts + 200)
        tracker.feed_book({"bids": [[100.0, 11.0]], "asks": [[100.1, 10.0]]}, ts := ts + 200)  # refresh 1
        tracker.feed_book({"bids": [[100.0, 9.0]], "asks": [[100.1, 10.0]]}, ts := ts + 200)
        tracker.feed_book({"bids": [[100.0, 12.0]], "asks": [[100.1, 10.0]]}, ts := ts + 600)  # refresh 2

        # Total time = 1.3sec > 1.0sec, но только 2 refresh < 3
        assert tracker.bid.refresh == 2
        assert not tracker.is_iceberg("bid", ts), "Недостаточно refresh для iceberg"

    def test_ask_side_iceberg(self):
        """Тест iceberg детекции на ask стороне."""
        tracker = BestLevelTracker(
            min_duration_ms=1000,
            refresh_min_abs=1.0,
            refresh_count_target=2
        )

        ts = 0

        # Iceberg на ask уровне
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 20.0]]}, ts := ts + 100)
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 18.0]]}, ts := ts + 200)  # decrease
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 21.0]]}, ts := ts + 200)  # refresh 1
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 19.0]]}, ts := ts + 200)  # decrease
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 22.0]]}, ts := ts + 400)  # refresh 2

        assert tracker.ask.refresh == 2
        assert tracker.is_iceberg("ask", ts), "Должен детектировать iceberg на ask"
        assert not tracker.is_iceberg("bid", ts), "Bid не должен быть iceberg"

    def test_metrics_calculation(self):
        """Тест расчета метрик."""
        tracker = BestLevelTracker()

        ts = 1000

        book = {"bids": [[100.50, 150.0]], "asks": [[100.75, 80.0]]}
        tracker.feed_book(book, ts)

        # Через 2 секунды
        metrics = tracker.metrics(ts + 2000)

        assert metrics["bid"]["duration"] == 2.0, "Duration должен быть 2.0 секунды"
        assert metrics["bid"]["price"] == 100.50
        assert metrics["bid"]["volume"] == 150.0
        assert metrics["bid"]["refresh"] == 0

        assert metrics["ask"]["duration"] == 2.0
        assert metrics["ask"]["price"] == 100.75
        assert metrics["ask"]["volume"] == 80.0

    def test_small_refresh_not_counted(self):
        """Тест что маленькое увеличение объема не считается refresh."""
        tracker = BestLevelTracker(
            refresh_min_abs=5.0  # Требуется минимум +5.0
        )

        ts = 0

        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)
        tracker.feed_book({"bids": [[100.0, 8.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)  # decrease
        tracker.feed_book({"bids": [[100.0, 10.5]], "asks": [[100.1, 10.0]]}, ts := ts + 100)  # +2.5 < 5.0

        assert tracker.bid.refresh == 0, "Маленькое увеличение не должно считаться refresh"

        # Теперь большое увеличение
        tracker.feed_book({"bids": [[100.0, 8.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)  # decrease again
        tracker.feed_book({"bids": [[100.0, 14.0]], "asks": [[100.1, 10.0]]}, ts := ts + 100)  # +6.0 > 5.0

        assert tracker.bid.refresh == 1, "Большое увеличение должно считаться refresh"

    def test_empty_book(self):
        """Тест обработки пустого Order Book."""
        tracker = BestLevelTracker()

        ts = 1000

        # Пустой book
        tracker.feed_book({}, ts)
        tracker.feed_book(None, ts)
        tracker.feed_book({"bids": [], "asks": []}, ts)

        # Не должно быть ошибок
        metrics = tracker.metrics(ts)
        assert metrics["bid"]["price"] == 0.0
        assert metrics["ask"]["price"] == 0.0

    def test_reset(self):
        """Тест сброса состояния трекера."""
        tracker = BestLevelTracker()

        ts = 1000

        # Заполняем данными
        tracker.feed_book({"bids": [[100.0, 10.0]], "asks": [[100.1, 10.0]]}, ts)
        tracker.feed_book({"bids": [[100.0, 8.0]], "asks": [[100.1, 10.0]]}, ts + 100)
        tracker.feed_book({"bids": [[100.0, 12.0]], "asks": [[100.1, 10.0]]}, ts + 200)

        assert tracker.bid.refresh == 1

        # Сброс
        tracker.reset()

        assert tracker.bid.price is None
        assert tracker.bid.refresh == 0
        assert tracker.ask.price is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

