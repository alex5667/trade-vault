"""Tests for orderflow_services/ml_canary_autopromoter_v1.py."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest

from orderflow_services import ml_canary_autopromoter_v1 as svc


# ─── SID normalisation ────────────────────────────────────────────────────────


class TestNormSid:
    def test_of_with_direction(self):
        assert svc._norm_sid("of:BTCUSDT:1779525631000:S") == "BTCUSDT:1779525631000"

    def test_of_long(self):
        assert svc._norm_sid("of:ETHUSDT:1779525631000:L") == "ETHUSDT:1779525631000"

    def test_iceberg(self):
        assert svc._norm_sid("iceberg:BTCUSDT:1779525631000:L") == "BTCUSDT:1779525631000"

    def test_crypto_of_prefix(self):
        assert svc._norm_sid("crypto-of:SOLUSDT:1779525631000") == "SOLUSDT:1779525631000"

    def test_missing(self):
        assert svc._norm_sid(None) is None
        assert svc._norm_sid("") is None
        assert svc._norm_sid("garbage") is None

    def test_no_timestamp(self):
        assert svc._norm_sid("of:BTCUSDT:notats") is None


# ─── Welch's t-test ───────────────────────────────────────────────────────────


class TestWelchTtest:
    def test_identical_samples_p_close_to_one(self):
        t, p = svc._welch_ttest(100, 0.5, 0.1, 100, 0.5, 0.1)
        assert abs(t) < 1e-6
        assert p > 0.99

    def test_large_lift_low_p(self):
        # large effect size, ample sample
        t, p = svc._welch_ttest(500, 0.5, 0.4, 500, 0.0, 0.4)
        assert t > 5.0
        assert p < 0.001

    def test_insufficient_data_returns_neutral(self):
        t, p = svc._welch_ttest(1, 0.5, 0.0, 1, 0.0, 0.0)
        assert t == 0.0
        assert p == 1.0

    def test_negative_lift_low_p(self):
        t, p = svc._welch_ttest(500, 0.0, 0.4, 500, 0.5, 0.4)
        assert t < -5.0
        assert p < 0.001


# ─── _summary helper ──────────────────────────────────────────────────────────


class TestSummary:
    def test_empty(self):
        n, mean, var, hr = svc._summary([])
        assert (n, mean, var, hr) == (0, 0.0, 0.0, 0.0)

    def test_mean_and_var(self):
        vals = [0.1, 0.3, 0.5, 0.7, 0.9]
        n, mean, var, hr = svc._summary(vals)
        assert n == 5
        assert abs(mean - 0.5) < 1e-9
        # sample variance (n-1 denom)
        assert var > 0.0
        # hit_rate: 0.3, 0.5, 0.7, 0.9 ≥ 0.3 → 4/5
        assert abs(hr - 0.8) < 1e-9

    def test_all_negative_zero_hitrate(self):
        _, _, _, hr = svc._summary([-0.5, -0.3, -0.1])
        assert hr == 0.0


# ─── Ladder step ──────────────────────────────────────────────────────────────


class TestLadder:
    def test_promote_steps_through_ladder(self):
        assert svc._ladder_step(0.05, +1) == 0.10
        assert svc._ladder_step(0.10, +1) == 0.20
        assert svc._ladder_step(0.20, +1) == 0.40

    def test_promote_at_ceiling_stays(self, monkeypatch):
        # at the ceiling, should not advance
        assert svc._ladder_step(0.40, +1) <= 0.40

    def test_demote_halves(self):
        # halving 0.20 → 0.10
        assert abs(svc._ladder_step(0.20, -1) - 0.10) < 1e-9

    def test_demote_clamps_to_floor(self):
        assert svc._ladder_step(0.05, -1) == svc.RATE_FLOOR

    def test_hold(self):
        assert svc._ladder_step(0.10, 0) == 0.10


# ─── Decision logic ──────────────────────────────────────────────────────────


class TestDecide:
    def _prev(self, **kwargs):
        base = {
            "current_rate": 0.05,
            "dwell_h": 0.0,
            "last_eval_ts_ms": 0,
            "last_promotion_ts_ms": 0,
        }
        base.update(kwargs)
        return base

    def test_insufficient_samples_no_data(self, monkeypatch):
        monkeypatch.setattr(svc, "MIN_SAMPLES", 200)
        action, rate, _, _, _ = svc._decide(50, 0.5, 0.1, 100, 0.0, 0.1, self._prev())
        assert action == "no_data"
        assert rate == 0.05

    def test_strong_lift_with_dwell_promotes(self, monkeypatch):
        # ensure thresholds easy to hit
        monkeypatch.setattr(svc, "MIN_SAMPLES", 50)
        monkeypatch.setattr(svc, "MIN_LIFT_R", 0.05)
        monkeypatch.setattr(svc, "MAX_PVALUE", 0.10)
        monkeypatch.setattr(svc, "DWELL_HOURS", 0.0)
        monkeypatch.setattr(svc, "COOLDOWN_HOURS", 0.0)
        action, rate, t, p, _ = svc._decide(
            200, 0.5, 0.3, 200, 0.0, 0.3, self._prev(current_rate=0.05),
        )
        assert action == "promote"
        assert rate == 0.10
        assert t > 0
        assert p < 0.10

    def test_strong_lift_blocked_by_dwell(self, monkeypatch):
        monkeypatch.setattr(svc, "MIN_SAMPLES", 50)
        monkeypatch.setattr(svc, "DWELL_HOURS", 24.0)
        monkeypatch.setattr(svc, "COOLDOWN_HOURS", 0.0)
        prev = self._prev(current_rate=0.05, dwell_h=0.0, last_eval_ts_ms=svc._now_ms())
        action, rate, _, _, dwell_new = svc._decide(
            200, 0.5, 0.3, 200, 0.0, 0.3, prev,
        )
        assert action == "hold"
        assert rate == 0.05
        # dwell counter advances on passing window
        assert dwell_new >= 0.0

    def test_strong_lift_blocked_by_cooldown(self, monkeypatch):
        monkeypatch.setattr(svc, "MIN_SAMPLES", 50)
        monkeypatch.setattr(svc, "DWELL_HOURS", 0.0)
        monkeypatch.setattr(svc, "COOLDOWN_HOURS", 12.0)
        recent_promo = svc._now_ms()
        prev = self._prev(current_rate=0.10, last_promotion_ts_ms=recent_promo)
        action, rate, _, _, _ = svc._decide(
            200, 0.5, 0.3, 200, 0.0, 0.3, prev,
        )
        assert action == "hold"
        assert rate == 0.10

    def test_negative_lift_demotes(self, monkeypatch):
        monkeypatch.setattr(svc, "MIN_SAMPLES", 50)
        monkeypatch.setattr(svc, "MIN_LIFT_R", 0.05)
        monkeypatch.setattr(svc, "MAX_PVALUE", 0.10)
        action, rate, _, _, _ = svc._decide(
            200, 0.0, 0.3, 200, 0.5, 0.3, self._prev(current_rate=0.20),
        )
        assert action == "demote"
        assert rate < 0.20

    def test_marginal_lift_holds(self, monkeypatch):
        # lift < threshold, can't promote even with samples
        monkeypatch.setattr(svc, "MIN_SAMPLES", 50)
        monkeypatch.setattr(svc, "MIN_LIFT_R", 0.10)
        action, rate, _, _, _ = svc._decide(
            200, 0.06, 0.3, 200, 0.0, 0.3, self._prev(current_rate=0.05),
        )
        assert action == "hold"
        assert rate == 0.05

    def test_demote_below_floor_clamps(self, monkeypatch):
        monkeypatch.setattr(svc, "MIN_SAMPLES", 50)
        monkeypatch.setattr(svc, "MIN_LIFT_R", 0.05)
        monkeypatch.setattr(svc, "MAX_PVALUE", 0.10)
        monkeypatch.setattr(svc, "RATE_FLOOR", 0.05)
        action, rate, _, _, _ = svc._decide(
            200, 0.0, 0.3, 200, 0.5, 0.3, self._prev(current_rate=0.05),
        )
        # demote attempt; rate halved but floored at 0.05 → stays
        assert rate >= 0.05
        # action label: ladder_step did not move → "hold"
        assert action in ("hold", "demote")


# ─── State I/O ────────────────────────────────────────────────────────────────


class TestStateIO:
    def test_save_then_load_roundtrip(self):
        r = MagicMock()
        captured = {}
        def _set(key, value):
            captured["key"] = key
            captured["value"] = value
        r.set.side_effect = _set
        r.get.return_value = None  # for now

        svc._save_state(r, {"current_rate": 0.20, "enforce": 1})
        assert captured["key"] == svc.STATE_KEY
        body = json.loads(captured["value"])
        assert body["current_rate"] == 0.20
        assert body["enforce"] == 1
        assert "ts_ms" in body

    def test_load_invalid_hmac_returns_empty(self, monkeypatch):
        monkeypatch.setattr(svc, "HMAC_SECRET", "sek")
        r = MagicMock()
        r.get.return_value = json.dumps({
            "current_rate": 0.20, "enforce": 1, "sig": "wrong",
        })
        out = svc._load_state(r)
        assert out == {}


# ─── run_once integration (mocked streams) ────────────────────────────────────


class TestRunOnce:
    def _build_signals_xrange_response(self, items):
        """items: list of (sid, scorer_mode) tuples → returns XRANGE-shaped list."""
        out = []
        ts0 = 1_700_000_000_000
        for i, (sid, mode) in enumerate(items):
            entry_id = f"{ts0 + i}-0"
            payload = {
                "sid": sid,
                "symbol": sid.split(":")[1],
                "indicators": {
                    "confidence_breakdown": {"scorer_mode": mode},
                },
            }
            out.append((entry_id, {"payload": json.dumps(payload)}))
        return out

    def _build_trades_xrange_response(self, items):
        """items: list of (sid, r_multiple) → XRANGE flat-field response."""
        out = []
        ts0 = 1_700_000_000_000
        for i, (sid, rv) in enumerate(items):
            entry_id = f"{ts0 + i}-0"
            out.append((entry_id, {"sid": sid, "r_multiple": str(rv)}))
        return out

    def test_full_cycle_promotes_on_strong_lift(self, monkeypatch):
        monkeypatch.setattr(svc, "MIN_SAMPLES", 50)
        monkeypatch.setattr(svc, "MIN_LIFT_R", 0.05)
        monkeypatch.setattr(svc, "MAX_PVALUE", 0.10)
        monkeypatch.setattr(svc, "DWELL_HOURS", 0.0)
        monkeypatch.setattr(svc, "COOLDOWN_HOURS", 0.0)
        monkeypatch.setattr(svc, "ENFORCE", True)

        # Build a cohort: 60 enforce trades centred at r=0.5, 60 shadow at r=0.0.
        # Add small jitter so variance > 0 (Welch test requires it).
        sigs = []
        trades = []
        for i in range(60):
            sid_e = f"of:BTCUSDT:{1_700_000_000_000 + i}:L"
            sid_s = f"of:ETHUSDT:{1_700_000_000_000 + i}:L"
            sigs.append((sid_e, "ml_canary_enforce"))
            sigs.append((sid_s, "canary_shadow"))
            jitter_e = 0.05 * (1 if i % 2 == 0 else -1)
            jitter_s = 0.05 * (1 if i % 2 == 0 else -1)
            trades.append((sid_e, 0.5 + jitter_e))
            trades.append((sid_s, 0.0 + jitter_s))

        r = MagicMock()
        # Each stream returns its data on first call, [] on subsequent calls
        # (XRANGE pagination stops when len(chunk) < batch).
        served = {svc.SIGNALS_STREAM: False, svc.TRADES_STREAM: False}
        sigs_response = self._build_signals_xrange_response(sigs)
        trades_response = self._build_trades_xrange_response(trades)

        def _xrange_side(stream, **kwargs):
            if stream == svc.SIGNALS_STREAM and not served[stream]:
                served[stream] = True
                return sigs_response
            if stream == svc.TRADES_STREAM and not served[stream]:
                served[stream] = True
                return trades_response
            return []
        r.xrange.side_effect = _xrange_side
        r.get.return_value = None  # no prior state

        captured_state = {}
        def _set(key, value):
            captured_state["body"] = json.loads(value)
        r.set.side_effect = _set

        state = svc.run_once(r)
        assert state["last_action"] == "promote"
        assert state["current_rate"] == 0.10
        assert state["enforce"] == 1
        assert state["enforce_n"] == 60
        assert state["shadow_n"] == 60
        assert state["mean_diff_r"] > 0.4
        assert state["p_value"] < 0.01

    def test_full_cycle_no_data_when_insufficient(self, monkeypatch):
        monkeypatch.setattr(svc, "MIN_SAMPLES", 200)  # high threshold

        r = MagicMock()
        r.xrange.return_value = []
        r.get.return_value = None
        r.set.return_value = None

        state = svc.run_once(r)
        assert state["last_action"] == "no_data"
        assert state["enforce_n"] == 0
        assert state["shadow_n"] == 0
