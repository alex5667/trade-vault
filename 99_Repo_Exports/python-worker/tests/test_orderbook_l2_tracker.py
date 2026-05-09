"""
Тесты для orderbook_l2_tracker.py
"""
import pytest

from signals.orderbook_l2_tracker import L2BookTracker


def test_tracker_first_feed():
    """Тест первого feed (нет prev)"""
    tracker = L2BookTracker(k_small=3, k_large=5)

    book = {
        "bids": [[100.0, 1.0], [99.5, 2.0], [99.0, 1.5]],
        "asks": [[100.5, 0.8], [101.0, 1.2], [101.5, 1.0]],
    }

    snap = tracker.feed(book)

    assert snap is not None
    assert snap.m.best_bid == 100.0
    assert snap.m.best_ask == 100.5

    # Первый feed - нет изменений
    assert snap.ch.bid_top3_ratio == 0.0
    assert snap.ch.ask_top3_ratio == 0.0


def test_tracker_refill_detection():
    """Тест детекции refill (увеличение объёма)"""
    tracker = L2BookTracker(k_small=3, k_large=5)

    # Первый book
    book1 = {
        "bids": [[100.0, 1.0], [99.5, 1.0], [99.0, 1.0]],  # total = 3.0
        "asks": [[100.5, 1.0], [101.0, 1.0], [101.5, 1.0]],
    }
    snap1 = tracker.feed(book1)
    assert snap1.m.bid_top3 == 3.0

    # Второй book - bid refill
    book2 = {
        "bids": [[100.0, 2.0], [99.5, 2.0], [99.0, 2.0]],  # total = 6.0 (+100%)
        "asks": [[100.5, 1.0], [101.0, 1.0], [101.5, 1.0]],
    }
    snap2 = tracker.feed(book2)

    assert snap2.m.bid_top3 == 6.0
    # Ratio = (6 - 3) / 3 = 1.0 (100% increase)
    assert abs(snap2.ch.bid_top3_ratio - 1.0) < 0.01


def test_tracker_depletion_detection():
    """Тест детекции depletion (уменьшение объёма)"""
    tracker = L2BookTracker(k_small=3, k_large=5)

    # Первый book
    book1 = {
        "bids": [[100.0, 2.0], [99.5, 2.0], [99.0, 2.0]],  # total = 6.0
        "asks": [[100.5, 2.0], [101.0, 2.0], [101.5, 2.0]],  # total = 6.0
    }
    snap1 = tracker.feed(book1)

    # Второй book - ask depletion
    book2 = {
        "bids": [[100.0, 2.0], [99.5, 2.0], [99.0, 2.0]],
        "asks": [[100.5, 1.0], [101.0, 1.0], [101.5, 1.0]],  # total = 3.0 (-50%)
    }
    snap2 = tracker.feed(book2)

    # Ratio = (3 - 6) / 6 = -0.5 (-50% decrease)
    assert abs(snap2.ch.ask_top3_ratio - (-0.5)) < 0.01


def test_tracker_get_last():
    """Тест получения последнего снимка"""
    tracker = L2BookTracker()

    assert tracker.get_last() is None  # Ещё не было feed

    book = {
        "bids": [[100.0, 1.0]],
        "asks": [[100.5, 1.0]],
    }
    snap = tracker.feed(book)

    last = tracker.get_last()
    assert last is not None
    assert last.m.best_bid == snap.m.best_bid


def test_tracker_reset():
    """Тест сброса состояния"""
    tracker = L2BookTracker()

    book = {
        "bids": [[100.0, 1.0]],
        "asks": [[100.5, 1.0]],
    }
    tracker.feed(book)

    assert tracker.prev is not None
    assert tracker.last is not None

    tracker.reset()

    assert tracker.prev is None
    assert tracker.last is None


def test_tracker_multiple_feeds():
    """Тест последовательных feed"""
    tracker = L2BookTracker(k_small=3)

    books = [
        {"bids": [[100.0, 1.0], [99.5, 1.0], [99.0, 1.0]], "asks": [[100.5, 1.0], [101.0, 1.0], [101.5, 1.0]]},
        {"bids": [[100.0, 1.5], [99.5, 1.5], [99.0, 1.5]], "asks": [[100.5, 1.0], [101.0, 1.0], [101.5, 1.0]]},
        {"bids": [[100.0, 2.0], [99.5, 2.0], [99.0, 2.0]], "asks": [[100.5, 1.0], [101.0, 1.0], [101.5, 1.0]]},
    ]

    snaps = [tracker.feed(book) for book in books]

    # Первый snap - нет изменений
    assert snaps[0].ch.bid_top3_ratio == 0.0

    # Второй snap - +50%
    assert abs(snaps[1].ch.bid_top3_ratio - 0.5) < 0.01

    # Третий snap - +33% (от 4.5 до 6.0)
    assert abs(snaps[2].ch.bid_top3_ratio - 0.333) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

