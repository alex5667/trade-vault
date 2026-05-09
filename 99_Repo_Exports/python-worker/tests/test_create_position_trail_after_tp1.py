from __future__ import annotations

from types import SimpleNamespace

from domain.handlers import create_position


class _SpecStub:
    def risk_money(self, entry, sl, lot, direction):
        return abs(entry - sl) * lot


def _mk_signal(payload):
    # Minimal SignalNorm-like object
    return SimpleNamespace(
        sid="sid1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1000,
        lot=1.0,
        sl=95.0,
        tp_levels=[101.0, 102.0, 103.0],
        trail_profile=str((payload or {}).get("trail_profile") or ""),
        payload=payload,
        entry_tag="",
        # Optional typed fields (if you added to SignalNorm dataclass)
        trail_after_tp1=None,
        trail_after_tp1_reason=None,
    )


def test_create_position_trail_after_tp1_from_payload_zero():
    sig = _mk_signal({"trail_after_tp1": "0", "trail_after_tp1_reason": "NO_MOMO", "trail_profile": "rocket_v1"})
    pos = create_position(sig, _SpecStub())
    assert pos.trail_after_tp1 is False
    assert pos.trail_after_tp1_reason == "NO_MOMO"


def test_create_position_trail_after_tp1_default_true_when_missing():
    sig = _mk_signal({"trail_profile": "rocket_v1"})
    pos = create_position(sig, _SpecStub())
    assert pos.trail_after_tp1 is True
