from __future__ import annotations

from types import SimpleNamespace

# IMPORTANT:
# If your pytest runs with python-worker as root, this import is correct:
from domain.handlers import _should_start_trailing_after_tp1, _arm_trailing_after_tp1


def _mk_pos(*, trail_after_tp1: bool, profile: str = "rocket_v1"):
    # Minimal fields used by apply_trailing_update + our helper
    return SimpleNamespace(
        closed=False,
        id="oid1",
        sid="sid1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=95.0,
        tp_levels=[101.0, 102.0, 103.0],
        tp_hits=1,  # TP1 already hit
        trailing_started=False,
        trailing_active=False,
        trailing_distance=0.0,
        trailing_point=0.0,
        trail_profile=profile,
        signal_payload={
            "trail_profile": profile,
            "trail_after_tp1": trail_after_tp1,
            "trail_after_tp1_reason": "TEST_REASON",
        },
        # rocket_v1 optional fields used inside apply_trailing_update
        trailing_min_lock_r=1.0,
        min_lock_price=0.0,
    )


def test_should_start_trailing_reads_signal_payload(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")

    pos = _mk_pos(trail_after_tp1=False)
    assert _should_start_trailing_after_tp1(pos) is False

    pos2 = _mk_pos(trail_after_tp1=True)
    assert _should_start_trailing_after_tp1(pos2) is True


def test_arm_trailing_after_tp1_sets_flags_and_emits_event(monkeypatch):
    # allow trailing
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")
    monkeypatch.setenv("TRAIL_CLEAR_FUTURE_TPS_ON_START", "1")

    pos = _mk_pos(trail_after_tp1=True, profile="scalp_v1")
    ev = _arm_trailing_after_tp1(pos, ts_ms=123)

    assert ev is not None
    assert ev.event_type == "TRAILING_SYNC"
    assert pos.trailing_started is True
    assert pos.trailing_active is True


def test_arm_trailing_after_tp1_keeps_tps_for_rocket(monkeypatch):
    monkeypatch.setenv("TRAIL_CLEAR_FUTURE_TPS_ON_START", "1")
    pos = _mk_pos(trail_after_tp1=True, profile="rocket_v1")
    _ = _arm_trailing_after_tp1(pos, ts_ms=123)
    # rocket_v1 must keep TP2/TP3 levels to "count hits without closing qty"
    assert len(pos.tp_levels) == 3
