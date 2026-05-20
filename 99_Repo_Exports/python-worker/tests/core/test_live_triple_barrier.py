"""tests/core/test_live_triple_barrier.py — Unit tests for G6 Live Triple-Barrier.

Covers:
  * LiveBarrierTracker: TP hit, SL hit, TIMEOUT, horizon expiry, idempotency,
    bounded path, NO_TICKS edge case
  * spec_from_pos(): valid / missing SL / missing TP / degenerate / horizon priority
  * G6TripleBarrierExitGate: open/close lifecycle, shadow vs enforce mode,
    push_tick routing, callback, tracker_count
"""
from __future__ import annotations

import types
from typing import Any

import pytest

from core.live_triple_barrier import LiveBarrierTracker, spec_from_pos
from core.triple_barrier import BarrierOutcome


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

T0 = 1_700_000_000_000  # arbitrary epoch ms
ONE_MIN = 60_000
ONE_H = 3_600_000


def _make_pos(
    *,
    entry_price: float = 100.0,
    entry_ts_ms: int = T0,
    direction: str = "LONG",
    sl: float = 99.0,          # 100 bps SL
    tp_levels: list[float] | None = None,
    baseline_horizon_ms: int = 4 * ONE_H,
    sid: str = "sid-test-001",
    symbol: str = "BTCUSDT",
    signal_payload: dict | None = None,
    hold_target_ms: int = 0,
) -> Any:
    if tp_levels is None:
        tp_levels = [102.0]  # 200 bps TP

    ns = types.SimpleNamespace(
        sid=sid,
        id=sid,
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        entry_ts_ms=entry_ts_ms,
        sl=sl,
        tp_levels=tp_levels,
        baseline_horizon_ms=baseline_horizon_ms,
        hold_target_ms=hold_target_ms,
        baseline_sl=0.0,
        signal_payload=signal_payload or {},
    )
    return ns


def _tracker(pos) -> LiveBarrierTracker:
    spec = spec_from_pos(pos)
    assert spec is not None
    return LiveBarrierTracker(
        sid=pos.sid,
        entry_px=pos.entry_price,
        entry_ts_ms=pos.entry_ts_ms,
        direction=pos.direction,
        spec=spec,
    )


# ---------------------------------------------------------------------------
# spec_from_pos
# ---------------------------------------------------------------------------


class TestSpecFromPos:
    def test_valid_long(self):
        pos = _make_pos()
        spec = spec_from_pos(pos)
        assert spec is not None
        assert abs(spec.tp_bps - 200.0) < 0.1
        assert abs(spec.sl_bps - 100.0) < 0.1
        assert spec.h_ms == 4 * ONE_H

    def test_valid_short(self):
        pos = _make_pos(
            direction="SHORT",
            entry_price=100.0,
            sl=101.0,            # 100 bps SL above entry
            tp_levels=[98.0],    # 200 bps TP below entry
        )
        spec = spec_from_pos(pos)
        assert spec is not None
        assert abs(spec.tp_bps - 200.0) < 0.1
        assert abs(spec.sl_bps - 100.0) < 0.1

    def test_missing_entry_price(self):
        pos = _make_pos(entry_price=0.0)
        assert spec_from_pos(pos) is None

    def test_missing_sl(self):
        pos = _make_pos(sl=0.0)
        assert spec_from_pos(pos) is None

    def test_missing_tp(self):
        pos = _make_pos(tp_levels=[])
        assert spec_from_pos(pos) is None

    def test_degenerate_tp_below_entry_long(self):
        # tp_levels=[99.0] for LONG entry at 100 → negative tp_bps < 0.5
        pos = _make_pos(tp_levels=[99.0])
        spec = spec_from_pos(pos)
        # absolute distance is 100 bps, but spec doesn't know direction here —
        # it computes abs() distance so 100 bps → passes degenerate check
        # (it's direction-check that happens in process_tick, not in spec_from_pos)
        assert spec is not None  # spec is valid; bad TP caught by process_tick

    def test_baseline_horizon_takes_priority_over_hold_target(self):
        pos = _make_pos(baseline_horizon_ms=2 * ONE_H, hold_target_ms=6 * ONE_H)
        spec = spec_from_pos(pos)
        assert spec is not None
        assert spec.h_ms == 2 * ONE_H

    def test_hold_target_used_when_no_baseline(self):
        pos = _make_pos(baseline_horizon_ms=0, hold_target_ms=3 * ONE_H)
        spec = spec_from_pos(pos)
        assert spec is not None
        assert spec.h_ms == 3 * ONE_H

    def test_signal_payload_tb_horizon_h(self):
        pos = _make_pos(
            baseline_horizon_ms=0,
            hold_target_ms=0,
            signal_payload={"tb_horizon_h": 2.5},
        )
        spec = spec_from_pos(pos)
        assert spec is not None
        assert spec.h_ms == int(2.5 * ONE_H)

    def test_custom_cost_bps(self):
        pos = _make_pos()
        spec = spec_from_pos(pos, cost_bps=12.0)
        assert spec is not None
        assert spec.cost_bps == 12.0


# ---------------------------------------------------------------------------
# LiveBarrierTracker
# ---------------------------------------------------------------------------


class TestLiveBarrierTrackerTP:
    def test_tp_hit_long(self):
        pos = _make_pos(entry_price=100.0, tp_levels=[102.0], sl=99.0)
        t = _tracker(pos)
        # Drive price toward TP
        for i in range(5):
            r = t.push_tick(T0 + i * ONE_MIN, 100.0 + i * 0.5)
        # At 100.5*i=2.0 → 102.0 → TP
        r = t.push_tick(T0 + 5 * ONE_MIN, 102.01)
        assert r.outcome == BarrierOutcome.TP_HIT
        assert t.done

    def test_tp_idempotent_after_done(self):
        pos = _make_pos(entry_price=100.0, tp_levels=[102.0], sl=99.0)
        t = _tracker(pos)
        t.push_tick(T0 + ONE_MIN, 102.5)
        assert t.done
        r1 = t.push_tick(T0 + 2 * ONE_MIN, 103.0)
        r2 = t.push_tick(T0 + 3 * ONE_MIN, 104.0)
        assert r1 is r2  # same cached result

    def test_mfe_tracked(self):
        pos = _make_pos(entry_price=100.0, tp_levels=[102.0], sl=99.0)
        t = _tracker(pos)
        t.push_tick(T0 + ONE_MIN, 101.0)
        t.push_tick(T0 + 2 * ONE_MIN, 101.5)
        r = t.push_tick(T0 + 3 * ONE_MIN, 102.1)
        assert r.mfe_bps >= 100.0   # 100 bps peak seen
        assert r.outcome == BarrierOutcome.TP_HIT


class TestLiveBarrierTrackerSL:
    def test_sl_hit_long(self):
        pos = _make_pos(entry_price=100.0, sl=99.0, tp_levels=[102.0])
        t = _tracker(pos)
        r = t.push_tick(T0 + ONE_MIN, 98.99)  # -101 bps → SL at -100
        assert r.outcome == BarrierOutcome.SL_HIT
        assert t.done

    def test_sl_hit_short(self):
        pos = _make_pos(
            entry_price=100.0,
            direction="SHORT",
            sl=101.0,
            tp_levels=[98.0],
        )
        t = _tracker(pos)
        r = t.push_tick(T0 + ONE_MIN, 101.01)
        assert r.outcome == BarrierOutcome.SL_HIT
        assert t.done


class TestLiveBarrierTrackerTimeout:
    def test_no_barrier_hit_returns_timeout(self):
        pos = _make_pos(entry_price=100.0, sl=99.0, tp_levels=[102.0])
        t = _tracker(pos)
        r = t.push_tick(T0 + ONE_MIN, 100.5)
        assert r.outcome == BarrierOutcome.TIMEOUT
        assert not t.done

    def test_horizon_expiry_freezes_tracker(self):
        pos = _make_pos(
            entry_price=100.0, sl=99.0, tp_levels=[102.0],
            baseline_horizon_ms=2 * ONE_H,
        )
        t = _tracker(pos)
        # Tick within horizon
        t.push_tick(T0 + ONE_H, 100.5)
        assert not t.done
        # Tick beyond horizon
        r = t.push_tick(T0 + 2 * ONE_H + 1, 100.5)
        assert t.done
        # Push after horizon → still done, same result
        r2 = t.push_tick(T0 + 3 * ONE_H, 101.0)
        assert r is r2

    def test_is_horizon_expired(self):
        pos = _make_pos(baseline_horizon_ms=ONE_H)
        t = _tracker(pos)
        assert not t.is_horizon_expired(T0 + ONE_H - 1)
        assert t.is_horizon_expired(T0 + ONE_H + 1)

    def test_current_result_none_before_any_tick(self):
        pos = _make_pos()
        t = _tracker(pos)
        assert t.current_result() is None


class TestLiveBarrierTrackerBoundedPath:
    def test_maxlen_does_not_raise(self, monkeypatch):
        monkeypatch.setattr("core.live_triple_barrier.MAX_PATH_TICKS", 10)
        pos = _make_pos(
            entry_price=100.0, sl=99.0, tp_levels=[102.0],
            baseline_horizon_ms=24 * ONE_H,
        )
        spec = spec_from_pos(pos)
        assert spec is not None
        t = LiveBarrierTracker(
            sid=pos.sid,
            entry_px=pos.entry_price,
            entry_ts_ms=pos.entry_ts_ms,
            direction=pos.direction,
            spec=spec,
        )
        # Push 100 ticks — deque should stay at 10
        for i in range(100):
            t.push_tick(T0 + i * ONE_MIN, 100.1)
        assert t.path_len <= 10


class TestLiveBarrierTrackerNoTicks:
    def test_empty_path_before_first_push(self):
        pos = _make_pos()
        t = _tracker(pos)
        assert t.path_len == 0
        assert t.current_result() is None
        assert not t.done


# ---------------------------------------------------------------------------
# G6TripleBarrierExitGate
# ---------------------------------------------------------------------------


class TestG6Gate:
    @pytest.fixture(autouse=True)
    def _patch_enabled(self, monkeypatch):
        monkeypatch.setenv("TB_EXIT_ENABLED", "1")
        monkeypatch.setenv("TB_EXIT_MODE", "shadow")
        import services.trade_monitor.triple_barrier_exit_policy as m
        monkeypatch.setattr(m, "_ENABLED", True)
        monkeypatch.setattr(m, "_MODE", "shadow")
        yield
        monkeypatch.setattr(m, "_ENABLED", False)

    def _gate(self):
        from services.trade_monitor.triple_barrier_exit_policy import G6TripleBarrierExitGate
        return G6TripleBarrierExitGate()

    def test_open_creates_tracker(self):
        gate = self._gate()
        pos = _make_pos()
        ok = gate.open_position(pos)
        assert ok
        assert gate.tracker_count == 1

    def test_open_duplicate_is_noop(self):
        gate = self._gate()
        pos = _make_pos()
        gate.open_position(pos)
        gate.open_position(pos)
        assert gate.tracker_count == 1

    def test_close_removes_tracker(self):
        gate = self._gate()
        pos = _make_pos()
        gate.open_position(pos)
        gate.close_position(pos.sid)
        assert gate.tracker_count == 0

    def test_push_tick_shadow_no_close(self):
        gate = self._gate()
        pos = _make_pos(entry_price=100.0, sl=99.0, tp_levels=[102.0])
        gate.open_position(pos)
        decision = gate.push_tick(pos, T0 + ONE_MIN, 100.5)
        assert decision is not None
        assert not decision.should_close
        assert decision.mode == "shadow"

    def test_push_tick_timeout_shadow_no_close(self):
        pos = _make_pos(
            entry_price=100.0, sl=99.0, tp_levels=[102.0],
            baseline_horizon_ms=ONE_H,
        )
        gate = self._gate()
        gate.open_position(pos)
        # Send tick past horizon
        decision = gate.push_tick(pos, T0 + ONE_H + 1, 100.3)
        assert decision is not None
        assert decision.outcome == BarrierOutcome.TIMEOUT.value
        assert not decision.should_close  # shadow → no close

    def test_push_tick_timeout_enforce_triggers_callback(self, monkeypatch):
        import services.trade_monitor.triple_barrier_exit_policy as m
        monkeypatch.setattr(m, "_MODE", "enforce")
        monkeypatch.setattr(m, "_is_enforce", lambda: True)

        gate = self._gate()
        pos = _make_pos(
            entry_price=100.0, sl=99.0, tp_levels=[102.0],
            baseline_horizon_ms=ONE_H,
        )
        gate.open_position(pos)

        called = []
        def cb(sid, result):
            called.append((sid, result))

        decision = gate.push_tick(pos, T0 + ONE_H + 1, 100.3, on_timeout_close=cb)
        assert decision is not None
        assert decision.should_close
        assert decision.close_reason == "g6_tb_timeout"
        assert len(called) == 1
        assert called[0][0] == pos.sid

    def test_push_tick_tp_hit_shadow_no_close(self):
        gate = self._gate()
        pos = _make_pos(entry_price=100.0, sl=99.0, tp_levels=[102.0])
        gate.open_position(pos)
        decision = gate.push_tick(pos, T0 + ONE_MIN, 102.5)
        assert decision is not None
        assert not decision.should_close
        assert decision.outcome == BarrierOutcome.TP_HIT.value
        assert decision.close_reason == "g6_tb_tp"

    def test_push_tick_disabled_returns_none(self, monkeypatch):
        import services.trade_monitor.triple_barrier_exit_policy as m
        monkeypatch.setattr(m, "_ENABLED", False)
        gate = self._gate()
        pos = _make_pos()
        gate.open_position(pos)
        decision = gate.push_tick(pos, T0 + ONE_MIN, 100.5)
        assert decision is None

    def test_push_tick_lazy_register_if_no_open_position(self):
        gate = self._gate()
        pos = _make_pos()
        # Don't call open_position
        decision = gate.push_tick(pos, T0 + ONE_MIN, 100.3)
        assert decision is not None
        assert gate.tracker_count == 1

    def test_open_pos_with_no_tp_skipped(self):
        gate = self._gate()
        pos = _make_pos(tp_levels=[])
        ok = gate.open_position(pos)
        assert not ok
        assert gate.tracker_count == 0

    def test_tracker_sids_returns_list(self):
        gate = self._gate()
        pos = _make_pos()
        gate.open_position(pos)
        assert pos.sid in gate.tracker_sids()
