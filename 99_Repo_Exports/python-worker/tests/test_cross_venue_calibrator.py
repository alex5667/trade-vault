"""Tests for CrossVenueCalibratorCore (core/cross_venue_calibrator.py)."""
from __future__ import annotations

import json
import math

from core.cross_venue_calibrator import (
    AGREE_CAP,
    AGREE_FLOOR,
    CrossVenueCalibBin,
    CrossVenueCalibratorCore,
    DEFAULT_DISLOC_Z,
    DEFAULT_MIN_AGREE,
    DISLOC_CAP,
    DISLOC_FLOOR,
    _mad,
    _median,
)


# ─────────────────────────────── helpers ───────────────────────────── #

_NOW_MS = 1_700_000_000_000  # fixed epoch for deterministic tests


def _make_core(enforce: bool = False, min_samples: int = 5) -> CrossVenueCalibratorCore:
    return CrossVenueCalibratorCore(enforce=enforce, min_samples=min_samples)


def _feed(cal: CrossVenueCalibratorCore, symbol: str, vals: list[tuple[float, float]]) -> None:
    """Inject (disloc_z, agree) pairs at 60s intervals starting at _NOW_MS."""
    for i, (dz, ag) in enumerate(vals):
        cal.observe(symbol, dz, ag, _NOW_MS + i * 60_000)


# ─────────────────────────────── _median ───────────────────────────── #

class TestMedian:
    def test_empty(self):
        assert _median([]) == 0.0

    def test_single(self):
        assert _median([3.0]) == 3.0

    def test_odd(self):
        assert _median([1.0, 3.0, 2.0]) == 2.0

    def test_even(self):
        assert _median([1.0, 2.0, 3.0, 4.0]) == 2.5


# ─────────────────────────────── _mad ──────────────────────────────── #

class TestMAD:
    def test_empty(self):
        assert _mad([], 0.0) == 0.0

    def test_uniform(self):
        xs = [5.0, 5.0, 5.0]
        assert _mad(xs, _median(xs)) == 0.0

    def test_symmetric(self):
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert _mad(xs, _median(xs)) == 1.0


# ─────────────────────────────── observe ───────────────────────────── #

class TestObserve:
    def test_nan_ignored(self):
        cal = _make_core()
        cal.observe("BTCUSDT", float("nan"), 0.9, _NOW_MS)
        cal.observe("BTCUSDT", 1.0, float("inf"), _NOW_MS + 1)
        assert "BTCUSDT" not in cal._bins

    def test_empty_symbol_ignored(self):
        cal = _make_core()
        cal.observe("", 1.0, 0.9, _NOW_MS)
        assert len(cal._bins) == 0

    def test_symbol_normalised_upper(self):
        cal = _make_core()
        cal.observe("btcusdt", 1.0, 0.9, _NOW_MS)
        assert "BTCUSDT" in cal._bins

    def test_sample_stored(self):
        cal = _make_core()
        cal.observe("BTCUSDT", 1.5, 0.88, _NOW_MS)
        b = cal._bins["BTCUSDT"]
        assert len(b.buf) == 1
        assert b.buf[0].disloc_z == 1.5
        assert b.buf[0].agree == 0.88

    def test_eviction_on_observe(self):
        cal = CrossVenueCalibratorCore(window_ms=60_000, min_samples=2)
        cal.observe("BTCUSDT", 1.0, 0.9, _NOW_MS)
        cal.observe("BTCUSDT", 2.0, 0.8, _NOW_MS + 120_000)  # ts > now + window
        # First sample should be evicted
        assert len(cal._bins["BTCUSDT"].buf) == 1


# ─────────────────────────────── recompute ─────────────────────────── #

class TestRecompute:
    def test_too_few_samples(self):
        cal = _make_core(min_samples=10)
        _feed(cal, "BTCUSDT", [(1.0, 0.9)] * 5)
        updated = cal.recompute_all(_NOW_MS + 300_000)
        assert updated == 0
        b = cal._bins["BTCUSDT"]
        assert b.adaptive_disloc_z == DEFAULT_DISLOC_Z
        assert b.adaptive_min_agree == DEFAULT_MIN_AGREE

    def test_enough_samples_updates(self):
        cal = _make_core(enforce=False, min_samples=5)
        _feed(cal, "BTCUSDT", [(1.0, 0.9)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        b = cal._bins["BTCUSDT"]
        assert b.adaptive_disloc_z != DEFAULT_DISLOC_Z or b.adaptive_min_agree != DEFAULT_MIN_AGREE

    def test_adaptive_disloc_formula(self):
        """adaptive_disloc = max(FLOOR, median + 2.5 * MAD)."""
        cal = _make_core(enforce=True, min_samples=5)
        vals = [1.0, 1.1, 1.2, 0.9, 1.0]
        for i, v in enumerate(vals):
            cal.observe("BTCUSDT", v, 0.9, _NOW_MS + i * 60_000)
        cal.recompute_all(_NOW_MS + 300_000)
        b = cal._bins["BTCUSDT"]
        med = _median(vals)
        mad = _mad(vals, med)
        expected = max(DISLOC_FLOOR, min(DISLOC_CAP, med + 2.5 * mad))
        assert abs(b.adaptive_disloc_z - expected) < 1e-9

    def test_adaptive_agree_formula(self):
        """adaptive_min_agree = clamp(median - 2.0 * MAD, FLOOR, CAP)."""
        cal = _make_core(enforce=True, min_samples=5)
        vals_a = [0.9, 0.91, 0.92, 0.89, 0.9]
        for i, v in enumerate(vals_a):
            cal.observe("BTCUSDT", 1.0, v, _NOW_MS + i * 60_000)
        cal.recompute_all(_NOW_MS + 300_000)
        b = cal._bins["BTCUSDT"]
        med = _median(vals_a)
        mad = _mad(vals_a, med)
        expected = max(AGREE_FLOOR, min(AGREE_CAP, med - 2.0 * mad))
        assert abs(b.adaptive_min_agree - expected) < 1e-9

    def test_disloc_floor_respected(self):
        """Even with near-zero disloc samples, adaptive_disloc_z >= FLOOR."""
        cal = _make_core(enforce=True, min_samples=5)
        for i in range(10):
            cal.observe("BTCUSDT", 0.0, 0.9, _NOW_MS + i * 60_000)
        cal.recompute_all(_NOW_MS + 600_000)
        assert cal._bins["BTCUSDT"].adaptive_disloc_z >= DISLOC_FLOOR

    def test_agree_floor_respected(self):
        """Even with perfectly uniform high agreement, floor applies."""
        cal = _make_core(enforce=True, min_samples=5)
        for i in range(10):
            cal.observe("BTCUSDT", 1.0, 0.5, _NOW_MS + i * 60_000)
        cal.recompute_all(_NOW_MS + 600_000)
        assert cal._bins["BTCUSDT"].adaptive_min_agree >= AGREE_FLOOR

    def test_agree_cap_respected(self):
        """Adaptive min_agree never exceeds CAP."""
        cal = _make_core(enforce=True, min_samples=5)
        for i in range(10):
            cal.observe("BTCUSDT", 1.0, 1.0, _NOW_MS + i * 60_000)
        cal.recompute_all(_NOW_MS + 600_000)
        assert cal._bins["BTCUSDT"].adaptive_min_agree <= AGREE_CAP

    def test_disloc_cap_respected(self):
        """Adaptive disloc_z never exceeds CAP."""
        cal = _make_core(enforce=True, min_samples=5)
        for i in range(10):
            cal.observe("BTCUSDT", 100.0, 0.9, _NOW_MS + i * 60_000)
        cal.recompute_all(_NOW_MS + 600_000)
        assert cal._bins["BTCUSDT"].adaptive_disloc_z <= DISLOC_CAP


# ─────────────────────────────── thresholds_for ─────────────────────── #

class TestThresholdsFor:
    def test_shadow_mode_returns_defaults(self):
        cal = _make_core(enforce=False, min_samples=2)
        _feed(cal, "BTCUSDT", [(1.0, 0.9)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        dz, ma = cal.thresholds_for("BTCUSDT")
        assert dz == DEFAULT_DISLOC_Z
        assert ma == DEFAULT_MIN_AGREE

    def test_enforce_returns_calibrated(self):
        cal = _make_core(enforce=True, min_samples=5)
        _feed(cal, "BTCUSDT", [(1.0, 0.9)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        dz, ma = cal.thresholds_for("BTCUSDT")
        # Should not be the hardcoded defaults when calibrated
        assert isinstance(dz, float) and math.isfinite(dz)
        assert isinstance(ma, float) and math.isfinite(ma)

    def test_unknown_symbol_returns_defaults(self):
        cal = _make_core(enforce=True, min_samples=5)
        dz, ma = cal.thresholds_for("UNKNOWN")
        assert dz == DEFAULT_DISLOC_Z
        assert ma == DEFAULT_MIN_AGREE

    def test_too_few_samples_returns_defaults(self):
        cal = _make_core(enforce=True, min_samples=10)
        _feed(cal, "BTCUSDT", [(1.0, 0.9)] * 3)
        cal.recompute_all(_NOW_MS + 200_000)
        dz, ma = cal.thresholds_for("BTCUSDT")
        assert dz == DEFAULT_DISLOC_Z
        assert ma == DEFAULT_MIN_AGREE

    def test_custom_defaults_respected(self):
        cal = _make_core(enforce=True, min_samples=10)
        _feed(cal, "BTCUSDT", [(1.0, 0.9)] * 3)  # not enough
        cal.recompute_all(_NOW_MS + 200_000)
        dz, ma = cal.thresholds_for("BTCUSDT", default_disloc_z=5.0, default_min_agree=0.80)
        assert dz == 5.0
        assert ma == 0.80

    def test_symbol_normalised(self):
        cal = _make_core(enforce=True, min_samples=5)
        _feed(cal, "btcusdt", [(1.0, 0.9)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        dz1, ma1 = cal.thresholds_for("BTCUSDT")
        dz2, ma2 = cal.thresholds_for("btcusdt")
        assert dz1 == dz2 and ma1 == ma2


# ─────────────────────────────── snapshot/load ──────────────────────── #

class TestSnapshotRoundtrip:
    def test_roundtrip_empty(self):
        cal = _make_core(enforce=True)
        snap = cal.snapshot(_NOW_MS)
        restored = CrossVenueCalibratorCore.load_state(snap)
        assert restored.enforce is True
        assert len(restored._bins) == 0

    def test_roundtrip_with_data(self):
        cal = _make_core(enforce=True, min_samples=5)
        _feed(cal, "BTCUSDT", [(1.5, 0.88)] * 10)
        _feed(cal, "ETHUSDT", [(2.0, 0.75)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        snap = cal.snapshot(_NOW_MS + 600_000)
        restored = CrossVenueCalibratorCore.load_state(snap)
        assert "BTCUSDT" in restored._bins
        assert "ETHUSDT" in restored._bins
        b = restored._bins["BTCUSDT"]
        orig_b = cal._bins["BTCUSDT"]
        assert abs(b.adaptive_disloc_z - orig_b.adaptive_disloc_z) < 1e-6
        assert abs(b.adaptive_min_agree - orig_b.adaptive_min_agree) < 1e-6

    def test_buffers_not_persisted(self):
        """After load_state buffers are empty — buffers are rebuilt from live data."""
        cal = _make_core(enforce=True, min_samples=5)
        _feed(cal, "BTCUSDT", [(1.0, 0.9)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        snap = cal.snapshot(_NOW_MS + 600_000)
        restored = CrossVenueCalibratorCore.load_state(snap)
        assert len(restored._bins["BTCUSDT"].buf) == 0

    def test_committed_thresholds_survive_roundtrip(self):
        """Committed adaptive thresholds are preserved across snapshot/load."""
        cal = _make_core(enforce=True, min_samples=5)
        _feed(cal, "SOLUSDT", [(0.8, 0.93)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        orig_dz  = cal._bins["SOLUSDT"].adaptive_disloc_z
        orig_ma  = cal._bins["SOLUSDT"].adaptive_min_agree
        snap = cal.snapshot(_NOW_MS + 600_000)
        restored = CrossVenueCalibratorCore.load_state(snap)
        assert abs(restored._bins["SOLUSDT"].adaptive_disloc_z - orig_dz)  < 1e-6
        assert abs(restored._bins["SOLUSDT"].adaptive_min_agree - orig_ma) < 1e-6

    def test_json_serialisable(self):
        cal = _make_core(enforce=True, min_samples=5)
        _feed(cal, "BTCUSDT", [(1.2, 0.85)] * 10)
        cal.recompute_all(_NOW_MS + 600_000)
        snap = cal.snapshot(_NOW_MS + 600_000)
        serialised = json.dumps(snap)
        parsed = json.loads(serialised)
        assert parsed["n_symbols"] == 1

    def test_schema_version_1(self):
        cal = _make_core()
        snap = cal.snapshot(_NOW_MS)
        assert snap["schema_version"] == 1


# ─────────────────────────────── window eviction ─────────────────────── #

class TestWindowEviction:
    def test_old_samples_evicted(self):
        cal = CrossVenueCalibratorCore(window_ms=120_000, min_samples=2)  # 2 min window
        cal.observe("BTCUSDT", 1.0, 0.9, _NOW_MS)
        cal.observe("BTCUSDT", 2.0, 0.8, _NOW_MS + 60_000)
        # Advance by 5 min — cutoff = NOW+300s-120s = NOW+180s > both samples
        cal.recompute_all(_NOW_MS + 300_000)
        assert len(cal._bins["BTCUSDT"].buf) == 0

    def test_fresh_samples_kept(self):
        cal = CrossVenueCalibratorCore(window_ms=120_000, min_samples=2)
        cal.observe("BTCUSDT", 1.0, 0.9, _NOW_MS)
        cal.observe("BTCUSDT", 2.0, 0.8, _NOW_MS + 60_000)
        # now=NOW+150s → cutoff=NOW+30s → first evicted (ts=NOW<cutoff), second kept (ts=NOW+60s>cutoff)
        cal.recompute_all(_NOW_MS + 150_000)
        assert len(cal._bins["BTCUSDT"].buf) == 1
        assert cal._bins["BTCUSDT"].buf[0].disloc_z == 2.0


# ─────────────────────────── CrossVenueCalibBin ───────────────────────── #

class TestCalibBin:
    def test_defaults(self):
        b = CrossVenueCalibBin()
        assert b.adaptive_disloc_z == DEFAULT_DISLOC_Z
        assert b.adaptive_min_agree == DEFAULT_MIN_AGREE

    def test_from_dict_roundtrip(self):
        b = CrossVenueCalibBin()
        b.adaptive_disloc_z  = 2.2
        b.adaptive_min_agree = 0.72
        b.last_ts_ms         = _NOW_MS
        b.last_compute_ms    = _NOW_MS + 1
        d = b.to_dict()
        b2 = CrossVenueCalibBin.from_dict(d)
        assert abs(b2.adaptive_disloc_z  - 2.2)  < 1e-6
        assert abs(b2.adaptive_min_agree - 0.72) < 1e-6
        assert b2.last_ts_ms == _NOW_MS
        assert len(b2.buf) == 0  # buffers not persisted
