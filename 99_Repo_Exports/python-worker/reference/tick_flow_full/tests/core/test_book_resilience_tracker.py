# tick_flow_full/tests/core/test_book_resilience_tracker.py
# -*- coding: utf-8 -*-
"""
Tests for BookResilienceTracker.
Covers basic recovery flow, slow recovery, window expiry, and no-sweep baseline.
"""
import pytest
from core.book_resilience import BookResilienceTracker


def test_book_resilience_basic():
    """Sweep → depth drop → recovery: standard lifecycle."""
    tracker = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=5000)

    # Initial state — nothing active
    snap = tracker.snapshot()
    assert snap["res_active"] == 0

    # Trigger sweep (baseline depth = min(bid, ask) = 10_000 USD notional)
    tracker.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
    snap = tracker.snapshot()
    assert snap["res_active"] == 1
    assert snap["res_min_ratio"] == 1.0

    # Book update: depth drops to 50 % of bid
    tracker.on_book(1500, bid_depth_usd=5000.0, ask_depth_usd=10_000.0)
    snap = tracker.snapshot()
    assert snap["res_min_ratio"] == 0.5
    assert snap["res_recovered"] == 0

    # Book update: depth recovers to 90 % (> 80 % target)
    # elapsed = 2000 - 1000 = 1000 ms. Grace = 250 ms → need elapsed >= 1250 to deactivate.
    tracker.on_book(2000, bid_depth_usd=9000.0, ask_depth_usd=10_000.0)
    snap = tracker.snapshot()
    assert snap["res_curr_ratio"] == pytest.approx(0.9, abs=1e-6)
    assert snap["res_recovered"] == 1
    assert snap["res_recovery_ms"] == 1000  # 2000 - 1000
    # Still active: elapsed=1000 < recovery_ms(1000)+grace(250)=1250
    assert snap["res_active"] == 1

    # One more update past the grace period → deactivated
    tracker.on_book(2300, bid_depth_usd=9000.0, ask_depth_usd=10_000.0)
    snap = tracker.snapshot()
    assert snap["res_active"] == 0  # deactivated after recovery + grace


def test_book_resilience_no_sweep_no_active():
    """Without a sweep, tracker must remain inactive."""
    tracker = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=5000)
    # Feed book updates without triggering a sweep first
    for ts in range(1000, 6000, 500):
        tracker.on_book(ts, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
    snap = tracker.snapshot()
    assert snap["res_active"] == 0
    assert snap["res_recovered"] == 0


def test_book_resilience_slow_recovery():
    """Deep drop that never recovers before window expiry → res_recovered=0."""
    tracker = BookResilienceTracker(target_recovery_ratio=0.85, max_window_ms=3000)
    tracker.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
    # Depth stays at 40 % (below 85 % target)
    for ts in range(1500, 5000, 500):
        tracker.on_book(ts, bid_depth_usd=4000.0, ask_depth_usd=4000.0)
    # After max_window_ms=3000 ms from sweep ts=1000 → ts=4001 deactivates
    tracker.on_book(4100, bid_depth_usd=4000.0, ask_depth_usd=4000.0)
    snap = tracker.snapshot()
    assert snap["res_active"] == 0   # expired
    assert snap["res_recovered"] == 0


def test_book_resilience_window_expiry():
    """res_active must flip to 0 once max_window_ms has elapsed."""
    tracker = BookResilienceTracker(target_recovery_ratio=0.9, max_window_ms=2000)
    # on_sweep rejects ts_ms=0; use ts_ms=1
    tracker.on_sweep(1, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
    # Update at ts=1501 ms — elapsed=1500 < 2000 → still within window
    tracker.on_book(1501, bid_depth_usd=5000.0, ask_depth_usd=5000.0)
    assert tracker.snapshot()["res_active"] == 1
    # Update at ts=2002 ms — elapsed=2001 >= max_window_ms=2000 → expired
    tracker.on_book(2002, bid_depth_usd=5000.0, ask_depth_usd=5000.0)
    assert tracker.snapshot()["res_active"] == 0


def test_book_resilience_invalid_sweep_depth_ignored():
    """on_sweep with zero or negative depth must be silently ignored."""
    tracker = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=5000)
    tracker.on_sweep(1000, bid_depth_usd=0.0, ask_depth_usd=5000.0)  # zero bid → ignored
    assert tracker.snapshot()["res_active"] == 0

    tracker.on_sweep(1000, bid_depth_usd=-100.0, ask_depth_usd=5000.0)  # negative → ignored
    assert tracker.snapshot()["res_active"] == 0
