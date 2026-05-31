"""Tests for EntryRRMinCalibrator (W1)."""
from __future__ import annotations

import time
import pytest

from core.entry_rr_min_calibrator import EntryRRMinCalibrator


def _make_cal(**kw) -> EntryRRMinCalibrator:
    defaults = dict(enforce=True, min_samples=5, window_days=1.0,
                    recompute_gap_ms=0, default_rr=1.3, global_floor=1.0)
    defaults.update(kw)
    return EntryRRMinCalibrator(**defaults)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── Basic ─────────────────────────────────────────────────────────────────────

def test_default_when_no_data():
    cal = _make_cal()
    assert cal.get_rr_min(side="LONG", regime="trending") == 1.3


def test_default_when_enforce_off():
    cal = _make_cal(enforce=False, auto_enforce=False)
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=2.0, result="TP", ts_ms=ts)
    assert cal.get_rr_min(side="LONG", regime="trending") == 1.3


def test_only_winners_ingested():
    """Losers (SL/TIMEOUT) should NOT be ingested."""
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=2.0, result="SL", ts_ms=ts)
        cal.observe(side="LONG", regime="trending", r_multiple=0.5, result="TIMEOUT", ts_ms=ts)
    # no winner samples → default
    assert cal.get_rr_min(side="LONG", regime="trending") == 1.3


def test_p25_winner_floor():
    cal = _make_cal()
    ts = _now_ms()
    # 20 winners: R = 1.0, 1.0, ..., 1.0 (10), 2.0, ..., 2.0 (10)
    for i in range(10):
        cal.observe(side="LONG", regime="trending", r_multiple=1.0, result="TP", ts_ms=ts)
    for i in range(10):
        cal.observe(side="LONG", regime="trending", r_multiple=2.0, result="TP", ts_ms=ts)
    rr = cal.get_rr_min(side="LONG", regime="trending")
    # p25 of [1.0×10, 2.0×10] = 1.0 → committed = max(global_floor=1.0, 1.0) = 1.0
    assert rr >= 1.0
    assert rr <= 2.0


def test_global_floor_respected():
    cal = _make_cal(global_floor=1.0)
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=0.5, result="TP", ts_ms=ts)
    rr = cal.get_rr_min(side="LONG", regime="trending")
    assert rr >= 1.0


def test_max_floor_cap():
    cal = _make_cal(max_floor=3.0)
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=10.0, result="TP", ts_ms=ts)
    rr = cal.get_rr_min(side="LONG", regime="trending")
    assert rr <= 3.0


def test_fallback_side_only():
    """(SHORT, *) fallback used when (SHORT, specific_regime) has no data."""
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="SHORT", regime="*", r_multiple=1.5, result="TP", ts_ms=ts)
    rr = cal.get_rr_min(side="SHORT", regime="unknown")
    assert rr == cal.get_rr_min(side="SHORT", regime="*")


def test_fallback_global():
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="*", regime="*", r_multiple=1.8, result="TP", ts_ms=ts)
    rr = cal.get_rr_min(side="LONG", regime="squeeze")
    assert rr == cal.get_rr_min(side="*", regime="*")


def test_buy_sell_normalization():
    """BUY/SELL sides accepted same as LONG/SHORT."""
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="BUY", regime="trending", r_multiple=2.0, result="TP", ts_ms=ts)
    # BUY maps to LONG bucket in calibrator
    assert cal.get_rr_min(side="BUY", regime="trending") == cal.get_rr_min(side="LONG", regime="trending")


def test_snapshot_roundtrip():
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=1.8, result="TP", ts_ms=ts)
    snap = cal.snapshot()
    assert snap["schema_version"] == 1
    assert isinstance(snap["bins"], list)
    assert snap["default_rr"] == 1.3

    cal2 = _make_cal()
    cal2.load_state(snap)
    assert cal2.get_rr_min(side="LONG", regime="trending") == cal.get_rr_min(side="LONG", regime="trending")


def test_not_enough_samples():
    cal = _make_cal(min_samples=100)
    ts = _now_ms()
    for i in range(10):
        cal.observe(side="LONG", regime="trending", r_multiple=2.0, result="TP", ts_ms=ts)
    assert cal.get_rr_min(side="LONG", regime="trending") == 1.3


def test_shadow_rr_min():
    cal = _make_cal()
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=1.5, result="TP", ts_ms=ts)
    shadow = cal.get_shadow(side="LONG", regime="trending")
    assert isinstance(shadow, float)
    assert shadow >= 1.0


def test_window_eviction_losers_only():
    cal = _make_cal(window_days=0.0001)
    ts_old = _now_ms() - 1_000_000
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=2.0, result="TP", ts_ms=ts_old)
    ts_new = _now_ms()
    cal.observe(side="LONG", regime="trending", r_multiple=1.5, result="TP", ts_ms=ts_new)
    # should not throw
    rr = cal.get_rr_min(side="LONG", regime="trending")
    assert rr >= 1.0


def test_auto_enforce_promotes_after_warmup():
    """auto_enforce=True: after min_samples reached, returns calibrated value without enforce=True."""
    cal = _make_cal(enforce=False, auto_enforce=True)
    ts = _now_ms()
    for i in range(20):
        cal.observe(side="LONG", regime="trending", r_multiple=2.5, result="TP", ts_ms=ts)
    rr = cal.get_rr_min(side="LONG", regime="trending")
    # After warmup (20 >= min_samples=5), auto_enforce promotes → calibrated value
    assert rr >= 1.0
    assert rr != 1.3  # differs from default after calibration
