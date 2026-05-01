from __future__ import annotations
import pytest
from core.burst_calibrator import BurstCalibrator

def test_burst_calibrator_low_pressure_wide_gaps():
    cal = BurstCalibrator(
        base_window_ms=2500, min_window_ms=300, max_window_ms=3000,
        base_max_age_ms=8000,
    )
    # Slow ticks (e.g. 1 per second => 1000ms gap)
    w, a = cal.compute(gap_p50_ms=1000.0, cand_per_min=5.0)
    
    # Should stay close to base or even widen
    assert w >= 2500
    assert a >= 8000

def test_burst_calibrator_high_pressure_tight_gaps():
    cal = BurstCalibrator(
        base_window_ms=2500, min_window_ms=300, max_window_ms=3000,
        base_max_age_ms=8000,
        pressure_hi_per_min=60.0
    )
    # Fast ticks (e.g. 10 per second => 100ms gap) + high pressure
    w, a = cal.compute(gap_p50_ms=100.0, cand_per_min=100.0)
    
    # Should tighten window/age significantly
    assert w < 2500
    assert w >= 300
    assert a < 8000

def test_burst_calibrator_extreme_pressure():
    cal = BurstCalibrator(
        base_window_ms=2500, min_window_ms=300, max_window_ms=3000,
        base_max_age_ms=8000,
        pressure_extreme_per_min=200.0
    )
    # Extreme pressure
    w, a = cal.compute(gap_p50_ms=50.0, cand_per_min=300.0)
    
    # Should hit min_window
    assert w == 300
    # and floor age (min 2000)
    assert a == 2000
