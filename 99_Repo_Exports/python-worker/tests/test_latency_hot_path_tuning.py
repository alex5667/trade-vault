from __future__ import annotations

from common.metrics2 import LagTracker
from services.crypto_orderflow_service import CryptoOrderflowService
from services.orderflow.runtime import BookSnapshot


def test_book_snapshot_trusts_sorted_topn_by_default(monkeypatch):
    monkeypatch.delenv("BOOK_SORT_FALLBACK", raising=False)
    snap = BookSnapshot.from_raw(
        {
            "bids": [[100, 1], [101, 2]],
            "asks": [[102, 1], [101, 2]],
            "ts_ms": 123,
        }
    )

    assert snap.top5_bids[:2] == [(100.0, 1.0), (101.0, 2.0)]
    assert snap.top5_asks[:2] == [(102.0, 1.0), (101.0, 2.0)]


def test_book_snapshot_sort_fallback_can_repair_unsorted_levels(monkeypatch):
    monkeypatch.setenv("BOOK_SORT_FALLBACK", "1")
    snap = BookSnapshot.from_raw(
        {
            "bids": [[100, 1], [101, 2]],
            "asks": [[102, 1], [101, 2]],
            "ts_ms": 123,
        }
    )

    assert snap.top5_bids[:2] == [(101.0, 2.0), (100.0, 1.0)]
    assert snap.top5_asks[:2] == [(101.0, 2.0), (102.0, 1.0)]


def test_adaptive_tick_read_count_reduces_batch_on_redis_lag(monkeypatch):
    monkeypatch.setenv("CRYPTO_OF_ADAPTIVE_READ_COUNT", "1")
    monkeypatch.setenv("CRYPTO_OF_ADAPTIVE_LAG_HIGH_MS", "100")
    monkeypatch.setenv("CRYPTO_OF_ADAPTIVE_READ_COUNT_BURST", "50")

    svc = CryptoOrderflowService.__new__(CryptoOrderflowService)
    svc._lag_trackers = {"_redis_BTCUSDT": LagTracker(window=32)}

    for v in (120, 125, 130, 135, 140, 145, 150, 155):
        svc._lag_trackers["_redis_BTCUSDT"].update(v)

    assert svc._adaptive_tick_read_count("BTCUSDT", 200) == 50
