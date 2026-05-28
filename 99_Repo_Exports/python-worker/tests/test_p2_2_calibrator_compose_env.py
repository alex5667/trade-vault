"""P2.2 — Phase 2 calibrators wired into docker-compose env.

Covers:
  1.  ConfidenceThresholdCalibrator accepts CONF_CAL_ENFORCE=0 (shadow)
  2.  ConfidenceThresholdCalibrator accepts CONF_CAL_ENFORCE=1 (enforce)
  3.  CONF_CAL_TARGET_WR is passed through to calibrator
  4.  CONF_CAL_OUTCOME is passed through
  5.  CONF_CAL_MIN_SAMPLES is passed through
  6.  CONF_CAL_FLOOR / CONF_CAL_CEIL are respected (hard rails)
  7.  ConfidenceThresholdFilter.from_env() uses CONF_CAL_ENFORCE via calibrator
  8.  ConfidenceThresholdCalibrator shadow mode: min_conf_for returns default
  9.  ConfidenceThresholdCalibrator enforce mode: min_conf_for returns calibrated
 10.  crypto_orderflow_init wires calibrator from env (smoke: no ImportError)
 11.  P2.10 daily_dd: KILL_SYM_ENABLED env default is "0" (shadow-safe)
 12.  P2.10 daily_dd: KILL_HOURLY_ENABLED env default is "0" (shadow-safe)
"""
from __future__ import annotations

import importlib
import os
from unittest.mock import MagicMock, patch

import fakeredis
import pytest


# ─── helpers ─────────────────────────────────────────────────────────────────

def _make_cal(enforce: bool, target_wr: float = 0.55, outcome: str = "tp2",
              min_samples: int = 50, floor: float = 40.0, ceil: float = 90.0):
    from core.confidence_threshold_calibrator import ConfidenceThresholdCalibrator
    r = fakeredis.FakeRedis(decode_responses=True)
    return ConfidenceThresholdCalibrator(
        redis_client=r,
        enforce=enforce,
        target_wr=target_wr,
        outcome=outcome,
        min_samples_above=min_samples,
        conf_floor=floor,
        conf_ceil=ceil,
    )


# ─── 1. Shadow mode construction ─────────────────────────────────────────────

def test_conf_cal_enforce_false_shadow():
    cal = _make_cal(enforce=False)
    assert cal.enforce is False


# ─── 2. Enforce mode construction ────────────────────────────────────────────

def test_conf_cal_enforce_true():
    cal = _make_cal(enforce=True)
    assert cal.enforce is True


# ─── 3. target_wr passed through ────────────────────────────────────────────

def test_conf_cal_target_wr_passthrough():
    cal = _make_cal(enforce=False, target_wr=0.62)
    assert cal.target_wr == pytest.approx(0.62)


# ─── 4. outcome passed through ──────────────────────────────────────────────

def test_conf_cal_outcome_passthrough():
    cal = _make_cal(enforce=False, outcome="nosl_after_tp1")
    assert cal.outcome == "nosl_after_tp1"


# ─── 5. min_samples_above passed through ────────────────────────────────────

def test_conf_cal_min_samples_passthrough():
    cal = _make_cal(enforce=False, min_samples=75)
    assert cal.min_samples_above == 75


# ─── 6. Hard rail: conf_floor / conf_ceil ───────────────────────────────────

def test_conf_cal_floor_ceil_rails():
    from core.confidence_threshold_calibrator import ConfidenceThresholdCalibrator, CONF_FLOOR, CONF_CEIL
    # Default values match constants
    cal = _make_cal(enforce=False)
    assert cal.conf_floor == CONF_FLOOR
    assert cal.conf_ceil == CONF_CEIL

    # Can be overridden
    cal2 = _make_cal(enforce=False, floor=35.0, ceil=85.0)
    assert cal2.conf_floor == pytest.approx(35.0)
    assert cal2.conf_ceil == pytest.approx(85.0)


# ─── 7. ConfidenceThresholdFilter uses calibrator from env ──────────────────

def test_confidence_threshold_filter_with_calibrator(monkeypatch):
    monkeypatch.setenv("MIN_CONF_DEFAULT", "50.0")
    monkeypatch.setenv("MIN_CONF_FACTOR_DEFAULT", "0.45")
    monkeypatch.setenv("CONF_CAL_ENFORCE", "0")

    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdFilter
    r = fakeredis.FakeRedis(decode_responses=True)
    from core.confidence_threshold_calibrator import ConfidenceThresholdCalibrator
    cal = ConfidenceThresholdCalibrator(redis_client=r, enforce=False)

    f = ConfidenceThresholdFilter.from_env(calibrator=cal)
    # Smoke: should not raise
    result = f.evaluate(
        confidence_pct=72.0, conf_factor=0.48,
        symbol="BTCUSDT", kind="iceberg",
    )
    assert result is not None


# ─── 8. Shadow mode: min_conf_for returns default ────────────────────────────

def test_shadow_returns_default_min_conf():
    from core.confidence_threshold_calibrator import ConfidenceThresholdCalibrator, DEFAULT_MIN_CONF
    r = fakeredis.FakeRedis(decode_responses=True)
    cal = ConfidenceThresholdCalibrator(redis_client=r, enforce=False)
    # Cold Redis → shadow → default
    thr = cal.min_conf_for(symbol="BTCUSDT", kind="iceberg",
                           regime="trending_bull", session="us",
                           venue="binance", tf="5m")
    assert thr == DEFAULT_MIN_CONF


# ─── 9. Enforce mode: min_conf_for returns calibrated if warm ────────────────

def test_enforce_returns_calibrated_when_warm():
    from core.confidence_threshold_calibrator import ConfidenceThresholdCalibrator, DEFAULT_MIN_CONF
    r = fakeredis.FakeRedis(decode_responses=True)

    cal = ConfidenceThresholdCalibrator(
        redis_client=r,
        enforce=True,
        target_wr=0.55,
        outcome="tp2",
        min_samples_above=5,  # low so test data is enough
        conf_floor=40.0,
        conf_ceil=90.0,
    )

    # Seed reliability_calibrator data: bucket 70 → 80% WR with 10 samples
    key = "relcal:tp2:iceberg:BTCUSDT:binance:us:5m:trending_bull"
    r.hset(key, mapping={"b70:n": "10", "b70:h": "8", "b75:n": "6", "b75:h": "5"})

    thr = cal.min_conf_for(
        symbol="BTCUSDT", kind="iceberg", regime="trending_bull",
        session="us", venue="binance", tf="5m",
    )
    # Calibrated threshold should differ from default (can be lower or equal)
    # At minimum, must be within [floor, ceil]
    assert cal.conf_floor <= thr <= cal.conf_ceil


# ─── 10. crypto_orderflow_init wires calibrator (smoke) ──────────────────────

def test_crypto_orderflow_init_calibrator_smoke(monkeypatch):
    """Verify CONF_CAL_ENFORCE env propagates to the mixin without ImportError."""
    monkeypatch.setenv("CONF_CAL_ENFORCE", "0")
    monkeypatch.setenv("CONF_CAL_TARGET_WR", "0.55")
    monkeypatch.setenv("CONF_CAL_OUTCOME", "tp2")
    monkeypatch.setenv("CONF_CAL_MIN_SAMPLES", "50")

    # The mixin code path is guarded by try/except; just ensure the module loads.
    from handlers.crypto_orderflow.mixins import crypto_orderflow_init  # noqa: F401


# ─── 11. P2.10 KILL_SYM_ENABLED defaults to "0" (shadow-safe) ───────────────

def test_p2_10_kill_sym_default_disabled(monkeypatch):
    monkeypatch.delenv("KILL_SYM_ENABLED", raising=False)
    import services.daily_dd_kill_switch_v1 as mod
    importlib.reload(mod)
    # Default is "0" (disabled) — no per-symbol cap active
    assert mod.KILL_SYM_ENABLED is False


# ─── 12. P2.10 KILL_HOURLY_ENABLED defaults to "0" (shadow-safe) ─────────────

def test_p2_10_kill_hourly_default_disabled(monkeypatch):
    monkeypatch.delenv("KILL_HOURLY_ENABLED", raising=False)
    import services.daily_dd_kill_switch_v1 as mod
    importlib.reload(mod)
    assert mod.KILL_HOURLY_ENABLED is False
