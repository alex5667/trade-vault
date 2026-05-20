"""Tests for tp_sl_trailing autocal + runtime overrides reader + ENFORCE branch.

Coverage:
  1. _knob_lift counterfactual math per knob kind
  2. evaluate_window aggregates per-knob, dwell tracking
  3. publish_state writes HMAC-signed JSON to Redis
  4. TpSlTrailOverridesReader reads + verifies HMAC + respects enforce flag
  5. trailing_profiles._env_atr_mult reads autocal override > ENV > default
  6. Reader is disabled when AUTOCAL_TP_SL_TRAIL_READ_ENABLED=0 (default)
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any

import pytest


# ──────────────────────────────────────────────────────────────────────────
# autocal service unit tests
# ──────────────────────────────────────────────────────────────────────────

def test_knob_lift_tp1_partial_below_05r_returns_zero() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import _knob_lift
    trade = {"mfe_r": 0.2, "pnl_r": -1.0, "sl_dist": 100.0, "tp_hits": 0.0, "regime": "trend"}
    assert _knob_lift("tp1_target_r", trade) == 0.0


def test_knob_lift_tp1_partial_at_mfe1r_positive_when_pnl_negative() -> None:
    """If MFE 1R but actual pnl -1R, partial+remain math should improve outcome."""
    from orderflow_services.tp_sl_trailing_autocal_v1 import _knob_lift
    trade = {"mfe_r": 1.0, "pnl_r": -1.0, "sl_dist": 100.0, "tp_hits": 0.0, "regime": "trend"}
    lift = _knob_lift("tp1_target_r", trade)
    # cf = 0.25 + 0.5*-1 = -0.25; original pnl_r = -1.0; delta = 0.75
    assert lift == pytest.approx(0.75, rel=1e-3)


def test_knob_lift_arm_threshold_eligible_only_for_losing_with_mfe_in_band() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import _knob_lift
    # mfe 0.3 (in band), pnl -1.0 (losing) — should give some positive lift
    trade = {"mfe_r": 0.3, "pnl_r": -1.0, "sl_dist": 100.0, "tp_hits": 0.0, "regime": "trend"}
    lift = _knob_lift("arm_threshold_r", trade)
    assert lift > 0
    # mfe 0.3, but pnl_r positive — no benefit
    trade2 = {"mfe_r": 0.3, "pnl_r": 0.5, "sl_dist": 100.0, "tp_hits": 1.0, "regime": "trend"}
    assert _knob_lift("arm_threshold_r", trade2) == 0.0


def test_knob_lift_trail_mult_reduces_giveback_proportional() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import _knob_lift
    # TP1 hit, MFE 2R, actual pnl 0.5R → giveback 1.5R; rocket_v1 1.2→1.0
    trade = {"mfe_r": 2.0, "pnl_r": 0.5, "sl_dist": 100.0, "tp_hits": 1.0, "regime": "trend"}
    lift = _knob_lift("atr_mult_rocket_v1", trade)
    # ratio = (1.2-1.0)/1.2 = 0.1667; est = 1.5 * 0.1667 * 0.5 ≈ 0.125
    assert lift == pytest.approx(0.125, rel=1e-2)


def test_evaluate_window_pass_requires_min_trades() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import Cfg, evaluate_window
    cfg = Cfg(
        enable=True, enforce=False, interval_sec=60, window_h=24.0,
        min_trades=10, lift_r=0.05, tol_r=0.10, dwell_h=24.0,
        hmac_secret="", prom_port=9999, stream="trades:closed",
        redis_url="redis://localhost:6379/0",
    )
    # 3 winning trades with mfe 1R but actual -1R (good for tp1_target_r)
    trades = [
        {"mfe_r": 1.0, "pnl_r": -1.0, "sl_dist": 100.0, "tp_hits": 0.0, "regime": "trend"}
        for _ in range(3)
    ]
    out = evaluate_window(trades, cfg, {}, int(time.time() * 1000))
    # tp1_target_r got lift 0.75 each, but n=3 < min_trades=10 → does not pass
    assert out["tp1_target_r"]["passes"] == 0
    assert out["tp1_target_r"]["enforce"] == 0


def test_evaluate_window_passes_with_enough_trades_and_lift() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import Cfg, evaluate_window
    cfg = Cfg(
        enable=True, enforce=True, interval_sec=60, window_h=24.0,
        min_trades=10, lift_r=0.05, tol_r=0.10, dwell_h=0.0,  # dwell=0 to test immediate enforce
        hmac_secret="", prom_port=9999, stream="trades:closed",
        redis_url="redis://localhost:6379/0",
    )
    trades = [
        {"mfe_r": 1.0, "pnl_r": -1.0, "sl_dist": 100.0, "tp_hits": 0.0, "regime": "trend"}
        for _ in range(20)
    ]
    now_ms = int(time.time() * 1000)
    # Simulate prior pass to satisfy dwell.
    prev = {"tp1_target_r": {"dwell_h": 0.0, "last_pass_ms": now_ms - 1000}}
    out = evaluate_window(trades, cfg, prev, now_ms)
    assert out["tp1_target_r"]["passes"] == 1
    assert out["tp1_target_r"]["enforce"] == 1
    assert out["tp1_target_r"]["value"] == 0.5


def test_parse_trade_derives_mfe_r_from_mfe_pnl() -> None:
    """mfe_r absent in trades:closed — must be derived from mfe_pnl / one_r_money."""
    from orderflow_services.tp_sl_trailing_autocal_v1 import _parse_trade
    fields = {
        "r_multiple": "0.5",
        "one_r_money": "2.0",
        "mfe_pnl": "3.0",   # mfe_r = 3.0/2.0 = 1.5
        "tp_hits": "1",
        "regime": "trend",
    }
    t = _parse_trade(fields)
    assert t is not None
    assert abs(t["mfe_r"] - 1.5) < 1e-9
    assert t["pnl_r"] == 0.5
    assert t["tp_hits"] == 1.0


def test_parse_trade_direct_mfe_r_takes_priority() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import _parse_trade
    fields = {
        "r_multiple": "0.3",
        "mfe_r": "0.8",
        "one_r_money": "1.0",
        "mfe_pnl": "999.0",  # should be ignored
        "tp_hits": "0",
        "regime": "na",
    }
    t = _parse_trade(fields)
    assert t is not None
    assert abs(t["mfe_r"] - 0.8) < 1e-9


def test_parse_trade_mfe_r_zero_when_one_r_missing() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import _parse_trade
    fields = {"r_multiple": "-0.5", "tp_hits": "0"}
    t = _parse_trade(fields)
    assert t is not None
    assert t["mfe_r"] == 0.0


def test_publish_state_signs_with_hmac() -> None:
    from orderflow_services.tp_sl_trailing_autocal_v1 import Cfg, publish_state

    class _FakeRedis:
        def __init__(self) -> None:
            self.stored: dict[str, str] = {}

        def set(self, k: str, v: str, ex: int | None = None) -> None:
            self.stored[k] = v

    cfg = Cfg(
        enable=True, enforce=False, interval_sec=60, window_h=24.0,
        min_trades=10, lift_r=0.05, tol_r=0.10, dwell_h=24.0,
        hmac_secret="testsecret", prom_port=9999, stream="trades:closed",
        redis_url="redis://localhost:6379/0",
    )
    fake = _FakeRedis()
    knobs = {"tp1_target_r": {"value": 0.5, "enforce": 1, "lift_r": 0.07, "n": 250}}
    assert publish_state(fake, knobs, cfg, n_trades=300) is True  # type: ignore
    payload = json.loads(fake.stored["autocal:tp_sl_trailing:state"])
    assert "sig" in payload
    # Verify sig
    sig = payload.pop("sig")
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(b"testsecret", canon, hashlib.sha256).hexdigest()
    assert hmac.compare_digest(sig, expected)


# ──────────────────────────────────────────────────────────────────────────
# runtime overrides reader tests
# ──────────────────────────────────────────────────────────────────────────

class _FakeRedis:
    def __init__(self, payload: str | None = None) -> None:
        self._payload = payload

    def get(self, _key: str) -> str | None:
        return self._payload


def _build_signed_payload(knobs: dict[str, dict[str, Any]], secret: str) -> str:
    data = {
        "ts_ms": int(time.time() * 1000),
        "window_hours": 72.0,
        "n_trades": 500,
        "knobs": knobs,
    }
    canon = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    data["sig"] = hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()
    return json.dumps(data)


def test_reader_returns_default_when_no_payload() -> None:
    from services.tp_sl_trailing_runtime_overrides import TpSlTrailOverridesReader
    rdr = TpSlTrailOverridesReader(_FakeRedis(None))
    assert rdr.get_override("tp1_target_r", 0.0) == 0.0


def test_reader_returns_default_when_enforce_zero() -> None:
    from services.tp_sl_trailing_runtime_overrides import TpSlTrailOverridesReader
    knobs = {"tp1_target_r": {"value": 0.5, "enforce": 0}}
    fake = _FakeRedis(json.dumps({
        "ts_ms": int(time.time() * 1000),
        "knobs": knobs,
    }))
    rdr = TpSlTrailOverridesReader(fake, hmac_secret="")
    assert rdr.get_override("tp1_target_r", 0.0) == 0.0


def test_reader_returns_value_when_enforce_one_no_hmac() -> None:
    from services.tp_sl_trailing_runtime_overrides import TpSlTrailOverridesReader
    knobs = {"atr_mult_rocket_v1": {"value": 1.0, "enforce": 1}}
    fake = _FakeRedis(json.dumps({
        "ts_ms": int(time.time() * 1000),
        "knobs": knobs,
    }))
    rdr = TpSlTrailOverridesReader(fake, hmac_secret="")
    assert rdr.get_override("atr_mult_rocket_v1", 1.2) == 1.0


def test_reader_rejects_invalid_hmac() -> None:
    from services.tp_sl_trailing_runtime_overrides import TpSlTrailOverridesReader
    knobs = {"tp1_target_r": {"value": 0.5, "enforce": 1}}
    payload = _build_signed_payload(knobs, secret="goodsecret")
    # Tamper sig
    obj = json.loads(payload)
    obj["sig"] = "deadbeef" * 8
    fake = _FakeRedis(json.dumps(obj))
    rdr = TpSlTrailOverridesReader(fake, hmac_secret="goodsecret")
    # HMAC mismatch → snapshot ignored → default returned
    assert rdr.get_override("tp1_target_r", 0.0) == 0.0


def test_reader_accepts_valid_hmac() -> None:
    from services.tp_sl_trailing_runtime_overrides import TpSlTrailOverridesReader
    knobs = {"tp1_target_r": {"value": 0.5, "enforce": 1}}
    payload = _build_signed_payload(knobs, secret="goodsecret")
    fake = _FakeRedis(payload)
    rdr = TpSlTrailOverridesReader(fake, hmac_secret="goodsecret")
    assert rdr.get_override("tp1_target_r", 0.0) == 0.5


def test_reader_stale_snapshot_returns_default() -> None:
    from services.tp_sl_trailing_runtime_overrides import TpSlTrailOverridesReader
    # ts_ms 2 days ago — outside stale window (default 30 min)
    old_ts = int(time.time() * 1000) - 2 * 24 * 60 * 60 * 1000
    knobs = {"tp1_target_r": {"value": 0.5, "enforce": 1}}
    fake = _FakeRedis(json.dumps({"ts_ms": old_ts, "knobs": knobs}))
    rdr = TpSlTrailOverridesReader(fake, hmac_secret="")
    assert rdr.get_override("tp1_target_r", 0.0) == 0.0


def test_get_reader_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    from services import tp_sl_trailing_runtime_overrides as mod
    mod.reset_reader_for_tests()
    monkeypatch.delenv("AUTOCAL_TP_SL_TRAIL_READ_ENABLED", raising=False)
    assert mod.get_reader() is None
    assert mod.get_override("any", 42) == 42


# ──────────────────────────────────────────────────────────────────────────
# trailing_profiles integration (uses runtime overrides path)
# ──────────────────────────────────────────────────────────────────────────

def test_trailing_profiles_env_atr_mult_returns_env_when_no_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services import tp_sl_trailing_runtime_overrides as mod
    mod.reset_reader_for_tests()
    monkeypatch.delenv("AUTOCAL_TP_SL_TRAIL_READ_ENABLED", raising=False)
    monkeypatch.setenv("TRAILING_PROFILE_ATR_MULT_ROCKET_V1", "0.9")
    from services.trailing_profiles import TrailingProfilesRegistry
    val = TrailingProfilesRegistry._env_atr_mult("rocket_v1", 1.2)
    assert val == 0.9


def test_trailing_profiles_env_atr_mult_returns_default_without_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services import tp_sl_trailing_runtime_overrides as mod
    mod.reset_reader_for_tests()
    monkeypatch.delenv("AUTOCAL_TP_SL_TRAIL_READ_ENABLED", raising=False)
    monkeypatch.delenv("TRAILING_PROFILE_ATR_MULT_ROCKET_V1", raising=False)
    from services.trailing_profiles import TrailingProfilesRegistry
    assert TrailingProfilesRegistry._env_atr_mult("rocket_v1", 1.2) == 1.2
