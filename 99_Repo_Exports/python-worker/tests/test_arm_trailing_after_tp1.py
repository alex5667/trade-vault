from __future__ import annotations

from types import SimpleNamespace

from domain.handlers import _arm_trailing_after_tp1


def _mk_pos(profile="scalp_v1"):
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
        tp_hits=1,
        trailing_started=False,
        trailing_active=False,
        trailing_distance=0.0,
        trailing_point=0.0,
        trail_profile=profile,
        signal_payload={"trail_profile": profile, "trail_after_tp1_reason": "TEST"},
        trailing_min_lock_r=1.0,
        min_lock_price=0.0,
    )


def test_arm_trailing_sets_started_and_active():
    pos = _mk_pos("scalp_v1")
    ev = _arm_trailing_after_tp1(pos, ts_ms=12345)
    assert ev is not None
    assert ev.event_type == "TRAILING_SYNC"
    assert pos.trailing_started is True
    assert pos.trailing_active is True
