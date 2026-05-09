from core.pressure_tracker import PressureTracker


def test_pressure_tracker_burst_logic():
    """Test burst window calculation based on pressure."""
    tracker = PressureTracker(window_ms=1000, ema_alpha=1.0) # alpha=1 for immediate response

    # 0 pressure
    bw = tracker.burst_window_ms(base_ms=2500, min_ms=800, per_min_ema=0.0, hi_per_min=60, extreme_per_min=200)
    assert bw == 2500

    # High pressure (60 per min = 1 per sec)
    bw_hi = tracker.burst_window_ms(base_ms=2500, min_ms=800, per_min_ema=60.0, hi_per_min=60, extreme_per_min=200)
    assert bw_hi < 2500

    # Extreme pressure (200 per min)
    bw_ex = tracker.burst_window_ms(base_ms=2500, min_ms=800, per_min_ema=200.0, hi_per_min=60, extreme_per_min=200)
    assert bw_ex == 800

def test_pressure_tracker_ema():
    """Test EMA property of pressure tracker."""
    tracker = PressureTracker(window_ms=10000, ema_alpha=0.5)
    ts = 1000000

    # First trigger
    tracker.on_raw_trigger(ts_ms=ts)
    s1 = tracker.snapshot(now_ms=ts)
    # 1 trigger in 10s = 0.1/s = 6 per min. EMA starting from 0: 0 + 0.5*(6-0) = 3
    assert s1.per_min_ema == 3.0

    # Second trigger immediately
    tracker.on_raw_trigger(ts_ms=ts + 10)
    s2 = tracker.snapshot(now_ms=ts + 10)
    # instant rate is high, EMA should go up
    assert s2.per_min_ema > 3.0
