"""Tests for EntrySlippageCapCalibrator — shadow, enforce, auto_enforce, bounds, snapshot."""
from __future__ import annotations

import json
import time

from core.entry_slippage_cap_calibrator import EntrySlippageCapCalibrator, _MIN_CAP, _MAX_CAP


def _warm_cal(
    symbol: str = "BTCUSDT",
    session: str = "asian",
    n: int = 25,
    enforce: bool = False,
    auto_enforce: bool = True,
    min_samples: int = 20,
) -> EntrySlippageCapCalibrator:
    cal = EntrySlippageCapCalibrator(
        enforce=enforce, auto_enforce=auto_enforce, min_samples=min_samples
    )
    ts = int(time.time() * 1000)
    for i in range(n):
        cal.observe(symbol=symbol, session=session, entry_slip_bps=float(i % 15 + 1), ts_ms=ts + i * 1000)
    return cal


# ── Shadow mode (auto_enforce=False, enforce=False) ───────────────────────────

class TestShadowMode:
    def test_cold_returns_none(self):
        cal = EntrySlippageCapCalibrator(enforce=False, auto_enforce=False)
        assert cal.get_cap(symbol="BTCUSDT", session="asian") is None

    def test_warm_still_returns_none_in_shadow(self):
        cal = _warm_cal(auto_enforce=False)
        assert cal.get_cap(symbol="BTCUSDT", session="asian") is None

    def test_no_data_no_mutation(self):
        cal = EntrySlippageCapCalibrator(enforce=False, auto_enforce=False)
        _ = cal.get_cap(symbol="BTCUSDT", session="*")
        assert cal._bins == {}


# ── Auto-enforce: cold → None, warm → calibrated ─────────────────────────────

class TestAutoEnforce:
    def test_cold_returns_none(self):
        cal = EntrySlippageCapCalibrator(enforce=False, auto_enforce=True, min_samples=20)
        assert cal.get_cap(symbol="BTCUSDT", session="*") is None

    def test_warm_returns_positive_cap(self):
        cal = _warm_cal(auto_enforce=True)
        cap = cal.get_cap(symbol="BTCUSDT", session="asian")
        assert cap is not None
        assert cap > 0

    def test_promotes_at_min_samples_threshold(self):
        # Use a specific session to avoid wildcard double-counting via _bucket_keys duplicates.
        cal = EntrySlippageCapCalibrator(enforce=False, auto_enforce=True, min_samples=5,
                                         recompute_gap_ms=0)
        ts = int(time.time() * 1000)
        for i in range(4):
            cal.observe(symbol="ETHUSDT", session="asian", entry_slip_bps=5.0, ts_ms=ts + i * 1000)
        assert cal.get_cap(symbol="ETHUSDT", session="asian") is None
        cal.observe(symbol="ETHUSDT", session="asian", entry_slip_bps=5.0, ts_ms=ts + 5000)
        assert cal.get_cap(symbol="ETHUSDT", session="asian") is not None

    def test_wildcard_session_fallback(self):
        cal = _warm_cal(symbol="SOLUSDT", session="*", auto_enforce=True)
        cap = cal.get_cap(symbol="SOLUSDT", session="european")
        # wildcard session bin should be found
        assert cap is not None


# ── Enforce=True always returns calibrated ────────────────────────────────────

class TestEnforceTrue:
    def test_cold_enforce_returns_none(self):
        cal = EntrySlippageCapCalibrator(enforce=True, auto_enforce=False)
        assert cal.get_cap(symbol="BTCUSDT", session="*") is None

    def test_warm_enforce_returns_cap(self):
        cal = _warm_cal(enforce=True, auto_enforce=False)
        cap = cal.get_cap(symbol="BTCUSDT", session="asian")
        assert cap is not None and cap > 0


# ── Bounds ────────────────────────────────────────────────────────────────────

class TestBounds:
    def test_cap_within_min_max(self):
        cal = _warm_cal(n=50)
        cap = cal.get_cap(symbol="BTCUSDT", session="asian")
        if cap is not None:
            assert _MIN_CAP <= cap <= _MAX_CAP

    def test_negative_slippage_ignored(self):
        cal = EntrySlippageCapCalibrator(enforce=True, auto_enforce=False)
        ts = int(time.time() * 1000)
        cal.observe(symbol="BTCUSDT", session="*", entry_slip_bps=-5.0, ts_ms=ts)
        assert cal._bins == {}

    def test_nan_slippage_ignored(self):
        cal = EntrySlippageCapCalibrator(enforce=True, auto_enforce=False)
        ts = int(time.time() * 1000)
        cal.observe(symbol="BTCUSDT", session="*", entry_slip_bps=float("nan"), ts_ms=ts)
        assert cal._bins == {}


# ── Snapshot / load_state ─────────────────────────────────────────────────────

class TestSnapshot:
    def test_snapshot_has_auto_enforce(self):
        cal = EntrySlippageCapCalibrator(auto_enforce=True)
        snap = cal.snapshot()
        assert snap["auto_enforce"] is True

    def test_load_state_restores_auto_enforce(self):
        cal = _warm_cal(auto_enforce=True)
        snap = cal.snapshot()
        cal2 = EntrySlippageCapCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True

    def test_snapshot_json_roundtrip(self):
        cal = _warm_cal(n=30)
        snap = cal.snapshot()
        data = json.loads(json.dumps(snap))
        cal2 = EntrySlippageCapCalibrator(enforce=True, auto_enforce=False)
        cal2.load_state(data)
        cap = cal2.get_cap(symbol="BTCUSDT", session="asian")
        assert cap is not None and cap > 0

    def test_load_state_without_auto_enforce_preserves_constructor_default(self):
        cal = EntrySlippageCapCalibrator(auto_enforce=False)
        snap = cal.snapshot()
        snap.pop("auto_enforce", None)
        cal2 = EntrySlippageCapCalibrator(auto_enforce=True)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True
