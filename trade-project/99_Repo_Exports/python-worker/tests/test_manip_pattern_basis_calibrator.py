"""Tests for ManipPatternBasisCalibrator — auto_enforce + bounds + snapshot."""
from __future__ import annotations

import json
import time

from core.manip_pattern_basis_calibrator import (
    ManipPatternBasisCalibrator,
    _DEFAULT_BUILD_MULT,
    _DEFAULT_REVERT_FRAC,
    _DEFAULT_REVERT_MS,
    _DEFAULT_QS_MSG_Z,
    _DEFAULT_QS_CANCEL_Z,
    _BUILD_MULT_MIN,
    _BUILD_MULT_MAX,
    _REVERT_FRAC_MIN,
    _REVERT_FRAC_MAX,
    _REVERT_MS_MIN,
    _REVERT_MS_MAX,
    _QS_Z_MIN,
    _QS_Z_MAX,
)


def _defaults() -> dict:
    return {
        "build_mult": _DEFAULT_BUILD_MULT,
        "revert_frac": _DEFAULT_REVERT_FRAC,
        "revert_ms": _DEFAULT_REVERT_MS,
        "qs_msg_z": _DEFAULT_QS_MSG_Z,
        "qs_cancel_z": _DEFAULT_QS_CANCEL_Z,
    }


def _warm_cal(symbol: str = "BTCUSDT", n: int = 250, enforce=False, auto_enforce=True) -> ManipPatternBasisCalibrator:
    """Return a calibrator with n observations above min_samples.
    recompute_gap_ms=0 forces recompute on every observe so test data
    is processed without waiting for wall-clock gap to elapse.
    """
    cal = ManipPatternBasisCalibrator(
        enforce=enforce, auto_enforce=auto_enforce, min_samples=200, recompute_gap_ms=0
    )
    ts = int(time.time() * 1000)
    for i in range(n):
        cal.observe(
            symbol=symbol,
            layering_score=float(i % 10) / 10.0,
            qs_score=float(i % 8) / 8.0,
            build_depth_ratio=1.5 + (i % 5) * 0.1,
            revert_delay_ms=300.0 + (i % 100) * 5.0,
            ts_ms=ts + i * 1000,
        )
    return cal


# ── Shadow (auto_enforce=False, enforce=False) ──────────────────────────────

class TestShadowMode:
    def test_returns_defaults_cold(self):
        cal = ManipPatternBasisCalibrator(enforce=False, auto_enforce=False)
        assert cal.get_params("BTCUSDT") == _defaults()

    def test_returns_defaults_even_after_warmup(self):
        cal = _warm_cal(auto_enforce=False)
        assert cal.get_params("BTCUSDT") == _defaults()

    def test_returns_defaults_for_unknown_symbol(self):
        cal = ManipPatternBasisCalibrator(enforce=False, auto_enforce=False)
        assert cal.get_params("NOTEXIST") == _defaults()


# ── Auto-enforce (cold → default, warm → calibrated) ────────────────────────

class TestAutoEnforce:
    def test_cold_returns_defaults(self):
        cal = ManipPatternBasisCalibrator(enforce=False, auto_enforce=True, min_samples=200)
        assert cal.get_params("BTCUSDT") == _defaults()

    def test_warm_returns_calibrated(self):
        cal = _warm_cal(auto_enforce=True)
        params = cal.get_params("BTCUSDT")
        assert params != _defaults()
        assert params["build_mult"] > 0

    def test_promotes_exactly_at_min_samples(self):
        cal = ManipPatternBasisCalibrator(enforce=False, auto_enforce=True, min_samples=5)
        ts = int(time.time() * 1000)
        for i in range(4):
            cal.observe(symbol="ETHUSDT", layering_score=0.5, qs_score=0.3,
                        build_depth_ratio=2.0, revert_delay_ms=500.0, ts_ms=ts + i * 1000)
        # Still cold
        assert cal.get_params("ETHUSDT") == _defaults()
        # One more — crosses min_samples
        cal.observe(symbol="ETHUSDT", layering_score=0.5, qs_score=0.3,
                    build_depth_ratio=2.0, revert_delay_ms=500.0, ts_ms=ts + 5000)
        params = cal.get_params("ETHUSDT")
        assert params["build_mult"] > 0

    def test_wildcard_fallback_when_no_exact_symbol(self):
        cal = _warm_cal(symbol="BTCUSDT", auto_enforce=True)
        params_other = cal.get_params("SOLUSDT")
        # Should fall back to wildcard bin which was also populated
        assert params_other["build_mult"] > 0

    def test_auto_enforce_flag_on_instance(self):
        cal = ManipPatternBasisCalibrator(auto_enforce=True)
        assert cal.auto_enforce is True
        cal2 = ManipPatternBasisCalibrator(auto_enforce=False)
        assert cal2.auto_enforce is False


# ── Enforce=True always uses calibrated ─────────────────────────────────────

class TestEnforceTrue:
    def test_cold_enforce_returns_defaults(self):
        cal = ManipPatternBasisCalibrator(enforce=True, auto_enforce=False)
        assert cal.get_params("BTCUSDT") == _defaults()

    def test_warm_enforce_returns_calibrated(self):
        cal = _warm_cal(enforce=True, auto_enforce=False)
        params = cal.get_params("BTCUSDT")
        assert params["build_mult"] > 0
        assert params["build_mult"] != _DEFAULT_BUILD_MULT or params["revert_ms"] != _DEFAULT_REVERT_MS


# ── Bounds ───────────────────────────────────────────────────────────────────

class TestBounds:
    def test_build_mult_within_bounds(self):
        cal = _warm_cal(n=400)
        params = cal.get_params("BTCUSDT")
        assert _BUILD_MULT_MIN <= params["build_mult"] <= _BUILD_MULT_MAX

    def test_revert_ms_within_bounds(self):
        cal = _warm_cal(n=400)
        params = cal.get_params("BTCUSDT")
        assert _REVERT_MS_MIN <= params["revert_ms"] <= _REVERT_MS_MAX

    def test_qs_z_within_bounds(self):
        cal = _warm_cal(n=400)
        params = cal.get_params("BTCUSDT")
        assert _QS_Z_MIN <= params["qs_msg_z"] <= _QS_Z_MAX
        assert _QS_Z_MIN <= params["qs_cancel_z"] <= _QS_Z_MAX

    def test_revert_frac_within_bounds(self):
        cal = _warm_cal(n=400)
        params = cal.get_params("BTCUSDT")
        assert _REVERT_FRAC_MIN <= params["revert_frac"] <= _REVERT_FRAC_MAX


# ── Snapshot / load_state roundtrip ─────────────────────────────────────────

class TestSnapshot:
    def test_snapshot_has_auto_enforce(self):
        cal = ManipPatternBasisCalibrator(auto_enforce=True)
        snap = cal.snapshot()
        assert snap["auto_enforce"] is True

    def test_snapshot_auto_enforce_false(self):
        cal = ManipPatternBasisCalibrator(auto_enforce=False)
        snap = cal.snapshot()
        assert snap["auto_enforce"] is False

    def test_load_state_restores_auto_enforce(self):
        cal = _warm_cal(auto_enforce=True)
        snap = cal.snapshot()
        cal2 = ManipPatternBasisCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True

    def test_load_state_restores_committed_values(self):
        cal = _warm_cal(n=400)
        snap = cal.snapshot()
        cal2 = ManipPatternBasisCalibrator(enforce=True, auto_enforce=False)
        cal2.load_state(snap)
        params = cal2.get_params("BTCUSDT")
        assert params["build_mult"] > 0

    def test_snapshot_json_roundtrip(self):
        cal = _warm_cal(n=400)
        snap = cal.snapshot()
        data = json.loads(json.dumps(snap))
        cal2 = ManipPatternBasisCalibrator(enforce=True, auto_enforce=False)
        cal2.load_state(data)
        assert cal2.get_params("BTCUSDT")["build_mult"] > 0

    def test_load_state_without_auto_enforce_key_preserves_default(self):
        cal = ManipPatternBasisCalibrator(auto_enforce=True)
        snap = cal.snapshot()
        snap.pop("auto_enforce", None)
        cal2 = ManipPatternBasisCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        # Should preserve the constructor default (False) since key absent
        assert cal2.auto_enforce is False


# ── Observe NaN / inf guard ──────────────────────────────────────────────────

class TestObserveGuard:
    def test_nan_score_ignored(self):
        cal = ManipPatternBasisCalibrator()
        ts = int(time.time() * 1000)
        cal.observe(symbol="BTCUSDT", layering_score=float("nan"), qs_score=0.5,
                    build_depth_ratio=1.5, revert_delay_ms=500.0, ts_ms=ts)
        assert cal._bins == {}

    def test_inf_score_ignored(self):
        cal = ManipPatternBasisCalibrator()
        ts = int(time.time() * 1000)
        cal.observe(symbol="BTCUSDT", layering_score=float("inf"), qs_score=0.5,
                    build_depth_ratio=1.5, revert_delay_ms=500.0, ts_ms=ts)
        assert cal._bins == {}
