from __future__ import annotations

import os

import pytest

from domain.models import PositionState
from domain.handlers import _should_start_trailing_after_tp1, _maybe_start_trailing_after_tp1


def _mk_pos(*, allow: bool, profile: str = "rocket_v1") -> PositionState:
    # Fill only fields that are used by the tested functions
    pos = PositionState(
        id="oid-1",
        sid="sid-1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1700000000000,
        lot=1.0,
        remaining_qty=1.0,
        sl=95.0,
        tp_levels=[101.0, 102.0, 103.0],
        signal_payload={"trail_profile": profile},
        entry_tag="",
        trail_profile=profile,
        trailing_min_lock_r=0.5,
        min_lock_price=102.0,  # above current SL to test clamp on start
        baseline_mode="tp_sl",
        baseline_horizon_ms=0,
        baseline_sl=95.0,
        baseline_tp1=101.0,
        baseline_tp2=102.0,
        baseline_tp3=103.0,
    )
    pos.tp1_hit = True
    pos.tp_hits = 1
    pos.trail_after_tp1 = bool(allow)
    pos.trail_after_tp1_reason = "TEST"
    return pos


def test_should_start_trailing_respects_conditional_flag(monkeypatch):
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.setenv("FORCE_TRAIL_AFTER_TP1", "1")  # legacy would allow, but cond must override

    pos_no = _mk_pos(allow=False)
    assert _should_start_trailing_after_tp1(pos_no) is False

    pos_yes = _mk_pos(allow=True)
    assert _should_start_trailing_after_tp1(pos_yes) is True


def test_maybe_start_trailing_after_tp1_skips_and_emits_event(monkeypatch):
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")

    pos = _mk_pos(allow=False)
    ev = _maybe_start_trailing_after_tp1(pos, ts_ms=1700000001234)
    assert ev is not None
    assert ev.event_type == "TRAILING_SKIP"
    assert pos.trailing_started is False


def test_maybe_start_trailing_after_tp1_starts_and_clamps_sl_for_rocket(monkeypatch):
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")

    pos = _mk_pos(allow=True, profile="rocket_v1")
    old_sl = float(pos.sl)
    ev = _maybe_start_trailing_after_tp1(pos, ts_ms=1700000001234)
    assert ev is not None
    assert ev.event_type == "TRAILING_START"
    assert pos.trailing_started is True
    assert pos.trailing_active is True
    # apply_trailing_update() clamps SL to min_lock_price for rocket_v1
    assert float(pos.sl) >= float(pos.min_lock_price) >= old_sl