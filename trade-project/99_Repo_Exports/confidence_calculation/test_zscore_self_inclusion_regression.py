import math
from types import SimpleNamespace

import pytest


def _import_detector():
    # Support multiple repo layouts
    try:
        from core.crypto_orderflow_detectors import DeltaSpikeDetector
        return DeltaSpikeDetector
    except Exception:
        from crypto_orderflow_detectors import DeltaSpikeDetector
        return DeltaSpikeDetector


DeltaSpikeDetector = _import_detector()


def _std_eff(values):
    """Replicate detector's std_eff logic: std_dev with a std_floor based on mean_abs."""
    n = len(values)
    assert n > 0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    std_dev = math.sqrt(var) if var > 0 else 0.0
    mean_abs = sum(abs(v) for v in values) / n
    std_floor = max(1e-6, 0.10 * mean_abs)
    return mean, max(std_dev, std_floor)


def test_delta_zscore_is_computed_on_previous_window_not_including_current():
    """
    This test fails if implementation includes current delta into stats before computing z.
    It validates:
      - z == (delta - mean(prev_window)) / std_eff(prev_window)
      - self-inclusion would produce a noticeably smaller |z| (bias)
    """
    det = DeltaSpikeDetector(window=50, z_threshold=4.0, min_abs_volume=0.0)

    # Build a stable previous window with mean ~0 and std ~1:
    # 20 samples alternating +1/-1 => mean=0, std=1, std_floor=0.1 (inactive)
    for i in range(20):
        is_sell = (i % 2 == 1)
        tick = {"qty": 1.0, "is_buyer_maker": is_sell, "ts_ms": 1000 + i}
        ev = det.push(tick)
        assert ev is None  # no spikes expected

    prev = list(det.values)
    assert len(prev) >= 10

    # Outlier
    delta = 5.0
    tick_outlier = {"qty": delta, "is_buyer_maker": False, "ts_ms": 999999}
    ev = det.push(tick_outlier)
    assert ev is not None, "Outlier should trigger delta_spike event"

    # Expected z computed on previous window
    mean_prev, std_prev = _std_eff(prev)
    z_expected = (delta - mean_prev) / std_prev
    assert ev["z"] == pytest.approx(z_expected, rel=1e-9, abs=1e-12)

    # Compute the biased z if current delta were included
    incl = prev + [delta]
    mean_incl, std_incl = _std_eff(incl)
    z_incl = (delta - mean_incl) / std_incl

    # Bias should reduce |z| materially; 10% is conservative here
    assert abs(z_expected) >= abs(z_incl) * 1.10


def test_zscore_is_finite_even_on_near_flat_window():
    """
    Sanity: when window is near-flat, std_floor prevents inf/NaN z.
    """
    det = DeltaSpikeDetector(window=50, z_threshold=0.5, min_abs_volume=0.0)

    for i in range(20):
        tick = {"qty": 1.0, "is_buyer_maker": False, "ts_ms": 2000 + i}
        det.push(tick)

    ev = det.push({"qty": 1.000001, "is_buyer_maker": False, "ts_ms": 999998})
    # may trigger, but must be finite
    if ev is not None:
        assert math.isfinite(ev["z"])
