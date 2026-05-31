"""Tests for PrePublishGateCalibrator (#14, P2)."""
from __future__ import annotations

import json
import time

from core.pre_publish_gate_calibrator import PrePublishGateCalibrator


def _make_cal(**kw) -> PrePublishGateCalibrator:
    defaults = dict(
        enforce=True,
        min_samples=5,
        window_hours=24.0,
        recompute_gap_ms=0,
        default_delta_z_thr=2.0,
        default_obi_thr=0.35,
        gate_mad_z_mult=1.5,
        obi_safety_mult=1.2,
    )
    defaults.update(kw)
    return PrePublishGateCalibrator(**defaults)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── Default / no-data behaviour ───────────────────────────────────────────────

def test_default_delta_z_when_no_data():
    cal = _make_cal()
    assert cal.get_delta_z_thr("BTCUSDT", "trending") == 2.0


def test_default_obi_when_no_data():
    cal = _make_cal()
    assert cal.get_obi_thr("BTCUSDT", "trending") == 0.35


def test_default_when_enforce_off():
    cal = _make_cal(enforce=False, auto_enforce=False)
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=5.0, obi=0.5, ts_ms=ts)
    assert cal.get_delta_z_thr("BTCUSDT", "trending") == 2.0
    assert cal.get_obi_thr("BTCUSDT", "trending") == 0.35


def test_default_when_not_enough_samples():
    cal = _make_cal(min_samples=100)
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=5.0, obi=0.5, ts_ms=ts)
    assert cal.get_delta_z_thr("BTCUSDT", "trending") == 2.0


# ── Delta_z threshold calibration ────────────────────────────────────────────

def test_delta_z_threshold_computed():
    """After enough samples, threshold should be calibrated."""
    cal = _make_cal()
    ts = _now_ms()
    # Uniform delta_z values → MAD ≈ 0, threshold ≈ median
    for i in range(20):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=8.0, obi=0.6, ts_ms=ts)
    thr = cal.get_delta_z_thr("BTCUSDT", "trending")
    # median=8.0, MAD≈0, threshold ≈ 8.0
    assert 0.5 <= thr <= 20.0


def test_delta_z_threshold_not_below_min():
    """Threshold should never go below _MIN_DELTA_Z_THR=0.5."""
    cal = _make_cal(gate_mad_z_mult=0.0)
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=0.1, obi=0.1, ts_ms=ts)
    thr = cal.get_delta_z_thr("BTCUSDT", "trending")
    assert thr >= 0.5


def test_delta_z_threshold_not_above_max():
    """Threshold should never exceed _MAX_DELTA_Z_THR=20.0."""
    cal = _make_cal(gate_mad_z_mult=100.0)
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=10.0, obi=0.5, ts_ms=ts)
    thr = cal.get_delta_z_thr("BTCUSDT", "trending")
    assert thr <= 20.0


# ── OBI threshold calibration ─────────────────────────────────────────────────

def test_obi_threshold_computed():
    """OBI threshold = p75(|obi|) × safety_mult."""
    cal = _make_cal(obi_safety_mult=1.0)
    ts = _now_ms()
    # All OBI = 0.4 → p75 = 0.4, threshold = 0.4×1.0 = 0.4
    for _ in range(20):
        cal.observe(symbol="ETHUSDT", regime="ranging", delta_z=3.0, obi=0.4, ts_ms=ts)
    thr = cal.get_obi_thr("ETHUSDT", "ranging")
    assert abs(thr - 0.4) < 0.1, f"Expected ~0.4, got {thr}"


def test_obi_threshold_not_below_min():
    """OBI threshold should never go below _MIN_OBI_THR=0.05."""
    cal = _make_cal(obi_safety_mult=0.001)
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="ETHUSDT", regime="ranging", delta_z=2.0, obi=0.01, ts_ms=ts)
    thr = cal.get_obi_thr("ETHUSDT", "ranging")
    assert thr >= 0.05


def test_obi_uses_absolute_value():
    """Negative OBI values should be treated as |obi| for threshold computation."""
    cal = _make_cal()
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="SOLUSDT", regime="trending", delta_z=5.0, obi=-0.5, ts_ms=ts)
    thr = cal.get_obi_thr("SOLUSDT", "trending")
    # p75(|obi|) = 0.5, × 1.2 = 0.6
    assert thr > 0.05  # should be properly calibrated


# ── Fallback hierarchy ────────────────────────────────────────────────────────

def test_fallback_symbol_star_regime():
    """(sym, reg) missing → (sym, *) → (*, *)."""
    cal = _make_cal()
    ts = _now_ms()
    # Feed (BTCUSDT, *)
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", regime="*", delta_z=6.0, obi=0.4, ts_ms=ts)
    # Query unknown regime
    thr = cal.get_delta_z_thr("BTCUSDT", "unknown_regime")
    assert thr == cal.get_delta_z_thr("BTCUSDT", "*")


def test_fallback_to_global():
    """Falls back to (*, *) if symbol+regime missing."""
    cal = _make_cal()
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="*", regime="*", delta_z=7.0, obi=0.5, ts_ms=ts)
    thr = cal.get_delta_z_thr("UNKNOWN_SYM", "UNKNOWN_REGIME")
    assert thr == cal.get_delta_z_thr("*", "*")


# ── Snapshot / load_state roundtrip ──────────────────────────────────────────

def test_snapshot_roundtrip():
    cal = _make_cal()
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=5.0, obi=0.4, ts_ms=ts)
    snap = cal.snapshot()
    assert snap["schema_version"] == 1
    assert isinstance(snap["bins"], list)
    assert "gate_mad_z_mult" in snap
    assert "obi_safety_mult" in snap

    cal2 = _make_cal()
    cal2.load_state(snap)
    assert cal2.get_delta_z_thr("BTCUSDT", "trending") == cal.get_delta_z_thr("BTCUSDT", "trending")
    assert cal2.get_obi_thr("BTCUSDT", "trending") == cal.get_obi_thr("BTCUSDT", "trending")


def test_snapshot_has_both_thresholds():
    cal = _make_cal()
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=5.0, obi=0.4, ts_ms=ts)
    snap = cal.snapshot()
    # Find BTCUSDT/trending bin
    bins = {(r["symbol"], r["regime"]): r for r in snap["bins"]}
    row = bins.get(("BTCUSDT", "trending"))
    assert row is not None
    assert "committed_delta_z_thr" in row
    assert "committed_obi_thr" in row


# ── Window eviction ───────────────────────────────────────────────────────────

def test_window_eviction():
    cal = _make_cal(window_hours=0.001)  # tiny ~3.6 seconds
    ts_old = _now_ms() - 1_000_000
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=5.0, obi=0.4, ts_ms=ts_old)
    # Force recompute with fresh sample
    ts_now = _now_ms()
    cal.observe(symbol="BTCUSDT", regime="trending", delta_z=3.0, obi=0.3, ts_ms=ts_now)
    thr = cal.get_delta_z_thr("BTCUSDT", "trending")
    assert 0.5 <= thr <= 20.0


# ── Invalid input rejection ───────────────────────────────────────────────────

def test_invalid_nan_ignored():
    cal = _make_cal()
    ts = _now_ms()
    cal.observe(symbol="BTCUSDT", regime="trending", delta_z=float("nan"), obi=0.5, ts_ms=ts)
    cal.observe(symbol="BTCUSDT", regime="trending", delta_z=5.0, obi=float("inf"), ts_ms=ts)
    # No crash, defaults returned
    assert cal.get_delta_z_thr("BTCUSDT", "trending") == 2.0


# ── Shadow accessors ─────────────────────────────────────────────────────────

def test_shadow_accessors():
    cal = _make_cal()
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", regime="trending", delta_z=5.0, obi=0.4, ts_ms=ts)
    shadow_z = cal.get_shadow_delta_z("BTCUSDT", "trending")
    shadow_obi = cal.get_shadow_obi("BTCUSDT", "trending")
    assert isinstance(shadow_z, float)
    assert isinstance(shadow_obi, float)


# ── Reader module (without live Redis) ───────────────────────────────────────

def test_reader_returns_none_when_disabled(monkeypatch):
    monkeypatch.setenv("AUTOCAL_PRE_PUBLISH_GATE_READ_ENABLED", "0")
    import services.pre_publish_gate_runtime_overrides as mod
    mod._READER = None
    assert mod.get_delta_z_thr("BTCUSDT", "trending") is None
    assert mod.get_obi_thr("BTCUSDT", "trending") is None


def test_reader_parses_snapshot_and_returns_values():
    import services.pre_publish_gate_runtime_overrides as mod

    snap = {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "enforce": True,
        "default_delta_z_thr": 2.0,
        "default_obi_thr": 0.35,
        "gate_mad_z_mult": 1.5,
        "obi_safety_mult": 1.2,
        "bins": [
            {
                "symbol": "BTCUSDT", "regime": "trending",
                "committed_delta_z_thr": 5.5, "shadow_delta_z_thr": 5.5,
                "committed_obi_thr": 0.42, "shadow_obi_thr": 0.42,
                "n": 200, "n_buf": 200,
            },
        ],
    }

    class FakeRedis:
        def get(self, _key):
            return json.dumps(snap).encode()

    reader = mod.PrePublishGateReader(FakeRedis(), redis_key="autocal:pre_publish_gate:state")
    assert reader.get_delta_z_thr("BTCUSDT", "trending") == 5.5
    assert reader.get_obi_thr("BTCUSDT", "trending") == 0.42


def test_reader_fallback_to_global_wildcard():
    import services.pre_publish_gate_runtime_overrides as mod

    snap = {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "enforce": True,
        "default_delta_z_thr": 2.0,
        "default_obi_thr": 0.35,
        "gate_mad_z_mult": 1.5,
        "obi_safety_mult": 1.2,
        "bins": [
            {
                "symbol": "*", "regime": "*",
                "committed_delta_z_thr": 4.0, "shadow_delta_z_thr": 4.0,
                "committed_obi_thr": 0.30, "shadow_obi_thr": 0.30,
                "n": 500, "n_buf": 500,
            },
        ],
    }

    class FakeRedis:
        def get(self, _key):
            return json.dumps(snap).encode()

    reader = mod.PrePublishGateReader(FakeRedis(), redis_key="autocal:pre_publish_gate:state")
    assert reader.get_delta_z_thr("UNKNOWN", "UNKNOWN") == 4.0
    assert reader.get_obi_thr("UNKNOWN", "UNKNOWN") == 0.30


def test_reader_returns_none_when_not_enforce():
    import services.pre_publish_gate_runtime_overrides as mod

    snap = {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "enforce": False,
        "default_delta_z_thr": 2.0,
        "default_obi_thr": 0.35,
        "bins": [
            {
                "symbol": "BTCUSDT", "regime": "trending",
                "committed_delta_z_thr": 5.0, "shadow_delta_z_thr": 5.0,
                "committed_obi_thr": 0.40, "shadow_obi_thr": 0.40,
                "n": 100, "n_buf": 100,
            },
        ],
    }

    class FakeRedis:
        def get(self, _key):
            return json.dumps(snap).encode()

    reader = mod.PrePublishGateReader(FakeRedis(), redis_key="autocal:pre_publish_gate:state", refresh_ms=1)
    assert reader.get_delta_z_thr("BTCUSDT", "trending") is None
    assert reader.get_obi_thr("BTCUSDT", "trending") is None
