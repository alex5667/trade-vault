from __future__ import annotations

from core.signal_pressure import SignalPressureTracker


def test_pressure_rates():
    tr = SignalPressureTracker(window_ms=60_000)
    # 6 candidates in a minute => 6/min
    base = 1_000_000
    for i in range(6):
        tr.record_candidate(base + i * 10_000)
    snap = tr.snapshot(base + 59_000)
    assert snap["cand_per_min"] >= 5.9
    assert tr.is_pressure_hi(base + 59_000, hi_per_min=4.0) is True


def test_prune():
    tr = SignalPressureTracker(window_ms=10_000)
    tr.record_candidate(1_000)
    tr.record_candidate(20_000)
    snap = tr.snapshot(21_000)
    # only second remains in 10s window
    assert snap["cand_per_min"] > 0.0
