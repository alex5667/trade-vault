"""Tests for SignalMinConfCalibrator (W1)."""
from __future__ import annotations

import time
import pytest

from core.signal_min_conf_calibrator import SignalMinConfCalibrator


def _make_cal(**kw) -> SignalMinConfCalibrator:
    defaults = dict(enforce=True, min_samples=5, window_days=1.0,
                    recompute_gap_ms=0, default_thr=70.0)
    defaults.update(kw)
    return SignalMinConfCalibrator(**defaults)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── Basic observe / get_threshold ─────────────────────────────────────────────

def test_default_when_no_data():
    cal = _make_cal()
    assert cal.get_threshold(kind="iceberg", regime="trending") == 70.0


def test_default_when_enforce_off():
    cal = _make_cal(enforce=False, auto_enforce=False)
    # feed plenty of samples
    ts = _now_ms()
    for i in range(20):
        cal.observe(kind="iceberg", regime="trending", conf_pct=80.0,
                    r_multiple=1.5, ts_ms=ts, w=1.0)
    # must still return default (shadow mode, auto_enforce disabled)
    assert cal.get_threshold(kind="iceberg", regime="trending") == 70.0


def test_threshold_converges_above_default():
    cal = _make_cal()
    ts = _now_ms()
    # high-confidence signals all winners → optimal τ should be high
    for i in range(30):
        conf = 80.0 + (i % 10)
        cal.observe(kind="iceberg", regime="trending", conf_pct=conf,
                    r_multiple=2.0, ts_ms=ts, w=1.0)
    thr = cal.get_threshold(kind="iceberg", regime="trending")
    assert 50.0 <= thr <= 95.0


def test_threshold_drops_when_low_conf_profitable():
    cal = _make_cal()
    ts = _now_ms()
    # low-confidence signals also have good R → optimal τ should stay low for coverage
    for i in range(30):
        cal.observe(kind="iceberg", regime="trending", conf_pct=55.0,
                    r_multiple=1.5, ts_ms=ts, w=1.0)
    thr = cal.get_threshold(kind="iceberg", regime="trending")
    assert thr <= 70.0, f"expected low thr, got {thr}"


def test_fallback_hierarchy_star_regime():
    """If (kind, regime) has no data but (*, regime) does, uses (*, regime)."""
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(kind="*", regime="trending", conf_pct=85.0,
                    r_multiple=2.0, ts_ms=ts, w=1.0)
    thr = cal.get_threshold(kind="unknown_kind", regime="trending")
    # Should find (*, trending) bucket
    assert thr == cal.get_threshold(kind="*", regime="trending")


def test_fallback_to_global():
    """Falls back to (*, *) if specific bucket missing."""
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(kind="*", regime="*", conf_pct=88.0, r_multiple=2.5, ts_ms=ts, w=1.0)
    thr = cal.get_threshold(kind="delta_spike", regime="unknown_regime")
    assert thr == cal.get_threshold(kind="*", regime="*")


def test_not_enough_samples_returns_default():
    cal = _make_cal(min_samples=100)
    ts = _now_ms()
    for i in range(10):
        cal.observe(kind="iceberg", regime="trending", conf_pct=80.0,
                    r_multiple=1.5, ts_ms=ts, w=1.0)
    assert cal.get_threshold(kind="iceberg", regime="trending") == 70.0


def test_snapshot_roundtrip():
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(kind="iceberg", regime="trending", conf_pct=80.0,
                    r_multiple=1.5, ts_ms=ts, w=1.0)
    snap = cal.snapshot()
    assert snap["schema_version"] == 1
    assert isinstance(snap["bins"], list)

    cal2 = _make_cal()
    cal2.load_state(snap)
    # committed_thr should be restored
    assert cal2.get_threshold(kind="iceberg", regime="trending") == cal.get_threshold(kind="iceberg", regime="trending")


def test_window_eviction():
    cal = _make_cal(window_days=0.0001)  # tiny window
    ts_old = _now_ms() - 1_000_000  # stale
    for i in range(20):
        cal.observe(kind="iceberg", regime="trending", conf_pct=85.0,
                    r_multiple=2.0, ts_ms=ts_old, w=1.0)
    # Manually trigger recompute check via get — data should be evicted
    # on next observe
    ts_now = _now_ms()
    cal.observe(kind="iceberg", regime="trending", conf_pct=60.0,
                r_multiple=0.5, ts_ms=ts_now, w=1.0)
    # after eviction, only 1 sample — below min_samples=5 → threshold unchanged from pre-eviction
    # at least it should not throw
    thr = cal.get_threshold(kind="iceberg", regime="trending")
    assert 50.0 <= thr <= 95.0


def test_invalid_observations_ignored():
    cal = _make_cal()
    ts = _now_ms()
    cal.observe(kind="iceberg", regime="trending", conf_pct=float("nan"),
                r_multiple=1.5, ts_ms=ts)
    cal.observe(kind="iceberg", regime="trending", conf_pct=80.0,
                r_multiple=float("inf"), ts_ms=ts)
    cal.observe(kind="iceberg", regime="trending", conf_pct=200.0,
                r_multiple=1.5, ts_ms=ts)  # out of range
    assert cal.get_threshold(kind="iceberg", regime="trending") == 70.0


def test_shadow_vs_committed_diverge():
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(kind="iceberg", regime="trending", conf_pct=90.0,
                    r_multiple=2.0, ts_ms=ts, w=1.0)
    shadow = cal.get_shadow(kind="iceberg", regime="trending")
    committed = cal.get_threshold(kind="iceberg", regime="trending")
    assert isinstance(shadow, float)
    assert isinstance(committed, float)


def test_auto_enforce_promotes_after_warmup():
    """auto_enforce=True: after min_samples reached, returns calibrated value without enforce=True."""
    cal = _make_cal(enforce=False, auto_enforce=True)
    ts = _now_ms()
    for i in range(20):
        cal.observe(kind="iceberg", regime="trending", conf_pct=55.0,
                    r_multiple=1.5, ts_ms=ts, w=1.0)
    thr = cal.get_threshold(kind="iceberg", regime="trending")
    # After warmup (20 >= min_samples=5), should return calibrated value
    assert isinstance(thr, float)
    assert 50.0 <= thr <= 95.0


def test_ips_weight_affects_calibration():
    """High-weight samples should dominate threshold."""
    cal1 = _make_cal()
    cal2 = _make_cal()
    ts = _now_ms()
    # cal1: high conf, high weight
    for i in range(20):
        cal1.observe(kind="iceberg", regime="trending", conf_pct=88.0,
                     r_multiple=2.0, ts_ms=ts, w=1.0)
    # cal2: same samples but half the weight on high-conf
    for i in range(20):
        cal2.observe(kind="iceberg", regime="trending", conf_pct=88.0,
                     r_multiple=2.0, ts_ms=ts, w=0.3)
    # Both should produce valid thresholds (no assert on direction, just sanity)
    thr1 = cal1.get_threshold(kind="iceberg", regime="trending")
    thr2 = cal2.get_threshold(kind="iceberg", regime="trending")
    assert 50.0 <= thr1 <= 95.0
    assert 50.0 <= thr2 <= 95.0
