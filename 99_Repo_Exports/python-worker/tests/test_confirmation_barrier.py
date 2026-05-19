"""Tests for ``core.confirmation_barrier``."""
from __future__ import annotations

import pytest

from core.confirmation_barrier import (
    BarrierConfig,
    ConfirmationBarrier,
    _signed_bps,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(mode="enforce", **cfg_kw) -> ConfirmationBarrier:
    cfg = BarrierConfig(**{
        "timeout_ms": 1_000,
        "min_progress_bps": 5.0,
        "max_adverse_bps": 10.0,
        "min_observations": 1,
        **cfg_kw,
    })
    return ConfirmationBarrier(config=cfg, mode=mode)


def _submit(b: ConfirmationBarrier, *, sid="s1", symbol="BTCUSDT", side="LONG",
            trigger=100.0, t0=1_000):
    return b.submit(
        signal_id=sid, symbol=symbol, side=side,
        trigger_price=trigger, trigger_ts_ms=t0,
    )


# ---------------------------------------------------------------------------
# _signed_bps helper
# ---------------------------------------------------------------------------

def test_signed_bps_long():
    assert _signed_bps("LONG", 100.0, 101.0) == pytest.approx(100.0)  # +1% = +100 bp
    assert _signed_bps("LONG", 100.0, 99.0) == pytest.approx(-100.0)


def test_signed_bps_short():
    assert _signed_bps("SHORT", 100.0, 99.0) == pytest.approx(100.0)
    assert _signed_bps("SHORT", 100.0, 101.0) == pytest.approx(-100.0)


def test_signed_bps_zero_ref_returns_zero():
    assert _signed_bps("LONG", 0.0, 100.0) == 0.0
    assert _signed_bps("LONG", -1.0, 100.0) == 0.0


# ---------------------------------------------------------------------------
# mode=off — no-op
# ---------------------------------------------------------------------------

def test_off_mode_returns_allow_immediately():
    b = _make(mode="off")
    assert b.submit(signal_id="s1", symbol="BTCUSDT", side="LONG",
                    trigger_price=100.0, trigger_ts_ms=1_000) == "ALLOW"
    assert len(b) == 0  # nothing stored


def test_off_mode_poll_is_empty():
    b = _make(mode="off")
    b.submit(signal_id="s1", symbol="BTCUSDT", side="LONG",
             trigger_price=100.0, trigger_ts_ms=1_000)
    b.observe(symbol="BTCUSDT", ts_ms=2_000, price=110.0)
    assert b.poll(now_ms=10_000) == []


# ---------------------------------------------------------------------------
# Submit edge cases
# ---------------------------------------------------------------------------

def test_submit_unknown_side_allows():
    b = _make()
    assert _submit(b, side="WHATEVER") == "ALLOW"
    assert len(b) == 0


def test_submit_bad_price_allows():
    b = _make()
    assert _submit(b, trigger=0.0) == "ALLOW"
    assert _submit(b, trigger=-1.0) == "ALLOW"


def test_submit_empty_id_allows():
    b = _make()
    assert _submit(b, sid="") == "ALLOW"


def test_submit_returns_none_when_pending():
    b = _make()
    assert _submit(b) is None
    assert "s1" in b.pending_ids()


def test_duplicate_submit_replaces():
    b = _make()
    _submit(b, sid="s1", trigger=100.0, t0=1_000)
    _submit(b, sid="s1", trigger=200.0, t0=2_000)
    assert len(b) == 1


# ---------------------------------------------------------------------------
# Resolution at deadline
# ---------------------------------------------------------------------------

def test_allow_after_progress():
    b = _make(min_progress_bps=5.0)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    # +0.10% = 10 bps favorable, well above 5 bp threshold (avoids IEEE
    # float boundary at 100.05).
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=100.10)
    out = b.poll(now_ms=2_001)  # past deadline (1000+1000)
    assert len(out) == 1
    sid, dec, reason, _ = out[0]
    assert sid == "s1"
    assert dec == "ALLOW"
    assert "confirmed_progress" in reason
    assert len(b) == 0  # cleared


def test_drop_when_no_progress():
    b = _make(min_progress_bps=5.0)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    # +0.01% = 1 bp — below threshold
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=100.01)
    out = b.poll(now_ms=2_001)
    assert len(out) == 1
    assert out[0][1] == "DROP"
    assert "no_progress" in out[0][2]


def test_drop_when_no_observations():
    b = _make(min_observations=1)
    _submit(b, t0=1_000)
    # No observe() call before deadline.
    out = b.poll(now_ms=2_001)
    assert out[0][1] == "DROP"
    assert "insufficient_obs" in out[0][2]


def test_min_observations_threshold():
    b = _make(min_observations=3, min_progress_bps=1.0)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=100.5)
    b.observe(symbol="BTCUSDT", ts_ms=1_600, price=100.5)
    # only 2 obs, need 3
    out = b.poll(now_ms=2_001)
    assert out[0][1] == "DROP"
    assert "insufficient_obs=2" in out[0][2]


# ---------------------------------------------------------------------------
# Early flip veto
# ---------------------------------------------------------------------------

def test_early_drop_on_adverse_move():
    b = _make(max_adverse_bps=10.0, timeout_ms=10_000)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    # −0.2% = 20 bp adverse → instant DROP at next poll
    b.observe(symbol="BTCUSDT", ts_ms=1_100, price=99.8)
    # We're far from deadline, but early flip resolves now.
    out = b.poll(now_ms=1_200)
    assert len(out) == 1
    assert out[0][1] == "DROP"
    assert "early_flip" in out[0][2]


def test_short_side_early_flip():
    b = _make(max_adverse_bps=10.0)
    _submit(b, side="SHORT", trigger=100.0, t0=1_000)
    b.observe(symbol="BTCUSDT", ts_ms=1_100, price=100.2)  # +20 bps adverse for SHORT
    out = b.poll(now_ms=1_200)
    assert out[0][1] == "DROP"


def test_early_flip_overrides_later_progress():
    """Once an early flip fires, later favourable ticks cannot rescue."""
    b = _make(max_adverse_bps=10.0, timeout_ms=10_000)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    b.observe(symbol="BTCUSDT", ts_ms=1_100, price=99.8)   # flip
    b.observe(symbol="BTCUSDT", ts_ms=1_200, price=101.0)  # recovery
    out = b.poll(now_ms=1_300)
    assert out[0][1] == "DROP"
    assert "early_flip" in out[0][2]


# ---------------------------------------------------------------------------
# Shadow mode
# ---------------------------------------------------------------------------

def test_shadow_mode_returns_shadow_decisions():
    b = _make(mode="shadow")
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=100.10)
    out = b.poll(now_ms=2_001)
    assert out[0][1] == "SHADOW_ALLOW"

    b.cancel("s1")  # clean
    _submit(b, side="LONG", trigger=100.0, t0=3_000)
    # No observation — drop path
    out = b.poll(now_ms=4_001)
    assert out[0][1] == "SHADOW_DROP"


# ---------------------------------------------------------------------------
# Observations rejected pre-trigger
# ---------------------------------------------------------------------------

def test_observation_before_trigger_ignored():
    b = _make()
    _submit(b, side="LONG", trigger=100.0, t0=2_000)
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=200.0)  # before t0
    out = b.poll(now_ms=3_001)
    # No counted observations → DROP for insufficient_obs.
    assert out[0][1] == "DROP"
    assert "insufficient_obs=0" in out[0][2]


def test_observation_for_unknown_symbol_ignored():
    b = _make()
    _submit(b, symbol="BTCUSDT")
    b.observe(symbol="ETHUSDT", ts_ms=1_500, price=200.0)
    out = b.poll(now_ms=2_001)
    assert out[0][1] == "DROP"


def test_observation_with_bad_price_ignored():
    b = _make(min_observations=1, min_progress_bps=1.0)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=0.0)
    b.observe(symbol="BTCUSDT", ts_ms=1_600, price=-5.0)
    out = b.poll(now_ms=2_001)
    assert out[0][1] == "DROP"
    assert "insufficient_obs" in out[0][2]


# ---------------------------------------------------------------------------
# Concurrent signals on multiple symbols
# ---------------------------------------------------------------------------

def test_multiple_symbols_independent():
    b = _make()
    _submit(b, sid="btc1", symbol="BTCUSDT", side="LONG", trigger=100.0, t0=1_000)
    _submit(b, sid="eth1", symbol="ETHUSDT", side="LONG", trigger=2000.0, t0=1_000)
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=100.10)  # +10 bp BTC
    b.observe(symbol="ETHUSDT", ts_ms=1_500, price=2000.0)  # 0 bp ETH
    out = b.poll(now_ms=2_001)
    decisions = {sid: dec for sid, dec, _, _ in out}
    assert decisions["btc1"] == "ALLOW"
    assert decisions["eth1"] == "DROP"
    assert len(b) == 0


def test_poll_keeps_unexpired():
    b = _make(timeout_ms=10_000)
    _submit(b)
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=100.10)
    assert b.poll(now_ms=2_000) == []  # before deadline
    assert len(b) == 1


# ---------------------------------------------------------------------------
# Cancel & expire_symbol
# ---------------------------------------------------------------------------

def test_cancel():
    b = _make()
    _submit(b)
    assert b.cancel("s1") is True
    assert len(b) == 0
    assert b.cancel("nonexistent") is False


def test_expire_symbol():
    b = _make()
    _submit(b, sid="a", symbol="BTCUSDT")
    _submit(b, sid="b", symbol="BTCUSDT")
    _submit(b, sid="c", symbol="ETHUSDT")
    out = list(b.expire_symbol("BTCUSDT", reason="restart"))
    assert {sid for sid, _, _, _ in out} == {"a", "b"}
    for _, dec, reason, _ in out:
        assert dec == "DROP"
        assert reason.startswith("forced_expire:restart")
    assert b.pending_ids() == ["c"]


# ---------------------------------------------------------------------------
# Payload pass-through
# ---------------------------------------------------------------------------

def test_payload_passed_through():
    b = _make()
    pl = {"foo": "bar"}
    b.submit(signal_id="s1", symbol="BTCUSDT", side="LONG",
             trigger_price=100.0, trigger_ts_ms=1_000, payload=pl)
    b.observe(symbol="BTCUSDT", ts_ms=1_500, price=100.10)
    out = b.poll(now_ms=2_001)
    assert out[0][3] is pl


# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------

def test_config_from_env(monkeypatch):
    monkeypatch.setenv("CONFIRMATION_BARRIER_TIMEOUT_MS", "5000")
    monkeypatch.setenv("CONFIRMATION_BARRIER_MIN_PROGRESS_BPS", "3.5")
    monkeypatch.setenv("CONFIRMATION_BARRIER_MAX_ADVERSE_BPS", "12")
    monkeypatch.setenv("CONFIRMATION_BARRIER_MIN_OBSERVATIONS", "4")
    cfg = BarrierConfig.from_env()
    assert cfg.timeout_ms == 5_000
    assert cfg.min_progress_bps == 3.5
    assert cfg.max_adverse_bps == 12.0
    assert cfg.min_observations == 4


def test_config_from_env_defaults(monkeypatch):
    for k in (
        "CONFIRMATION_BARRIER_TIMEOUT_MS",
        "CONFIRMATION_BARRIER_MIN_PROGRESS_BPS",
        "CONFIRMATION_BARRIER_MAX_ADVERSE_BPS",
        "CONFIRMATION_BARRIER_MIN_OBSERVATIONS",
    ):
        monkeypatch.delenv(k, raising=False)
    cfg = BarrierConfig.from_env()
    assert cfg.timeout_ms == 15_000
    assert cfg.min_progress_bps == 1.0


def test_mode_resolution_from_env(monkeypatch):
    monkeypatch.setenv("CONFIRMATION_BARRIER_MODE", "ENFORCE")
    assert ConfirmationBarrier().mode == "enforce"
    monkeypatch.setenv("CONFIRMATION_BARRIER_MODE", "garbage")
    assert ConfirmationBarrier().mode == "off"
    monkeypatch.delenv("CONFIRMATION_BARRIER_MODE", raising=False)
    assert ConfirmationBarrier().mode == "off"


# ---------------------------------------------------------------------------
# Regression — the TIMEOUT/MFE-MAE scenario from audit
# ---------------------------------------------------------------------------

def test_regression_chop_pattern_dropped():
    """Audit MFE +1.22 / MAE −1.43: price oscillates around trigger without
    decisive progress. Barrier should DROP that signal."""
    b = _make(min_progress_bps=10.0, max_adverse_bps=20.0, timeout_ms=2_000)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    # chop: +5 bp, −5 bp, +3 bp, −2 bp, …
    for i, delta_bp in enumerate([5, -5, 3, -2, 4, -3]):
        b.observe(symbol="BTCUSDT", ts_ms=1_100 + i * 100, price=100.0 * (1 + delta_bp / 10_000))
    out = b.poll(now_ms=3_001)
    assert out[0][1] == "DROP"
    assert "no_progress" in out[0][2]


def test_regression_clean_breakout_allowed():
    """A clean follow-through bar should pass the barrier."""
    b = _make(min_progress_bps=10.0, max_adverse_bps=20.0)
    _submit(b, side="LONG", trigger=100.0, t0=1_000)
    # Monotonic +5, +10, +15 bp
    for i, delta_bp in enumerate([5, 10, 15]):
        b.observe(symbol="BTCUSDT", ts_ms=1_100 + i * 100, price=100.0 * (1 + delta_bp / 10_000))
    out = b.poll(now_ms=2_001)
    assert out[0][1] == "ALLOW"
