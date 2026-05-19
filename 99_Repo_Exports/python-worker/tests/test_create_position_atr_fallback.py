"""Regression: pos.atr must fall back to indicators.atr when no labeled
atr_used_for_levels / atr_at_entry is present in payload.

Without this fallback, signals from sources that only populate
indicators.atr (e.g. iceberg detector, manipulation-gate context) write
pos.atr=0.0 → order hash gets atr_used_for_levels=0.0 →
trade_metrics_service.avg_sl_atr / avg_tp_atr render as "0.00 ATR" in the
periodic report (observed 2026-05-18 on ETHUSDT canary).
"""
from __future__ import annotations

from types import SimpleNamespace

from domain.handlers import create_position


class _SpecStub:
    def risk_money(self, entry, sl, lot, direction, symbol=None):
        return abs(entry - sl) * lot


def _mk_signal(payload):
    return SimpleNamespace(
        sid="iceberg:ETHUSDT:1779129093297:S",
        strategy="cryptoorderflow",
        source="CryptoOrderFlow",
        symbol="ETHUSDT",
        tf="tick",
        direction="SHORT",
        entry_price=2087.18,
        entry_ts_ms=1779129094000,
        lot=0.24,
        sl=2108.06,
        tp_levels=[2066.30, 2045.42, 2024.54],
        trail_profile="",
        payload=payload,
        entry_tag="",
        trail_after_tp1=None,
        trail_after_tp1_reason=None,
    )


def test_create_position_falls_back_to_indicators_atr():
    sig = _mk_signal({
        "indicators": {"atr": 2.60616484, "atr_src": "atr_string"},
    })
    pos = create_position(sig, _SpecStub())
    assert pos.atr == 2.60616484


def test_create_position_prefers_labeled_over_generic():
    sig = _mk_signal({
        "indicators": {
            "atr": 0.674,  # generic, e.g. 1m fallback
            "atr_used_for_levels": 2.5,  # labeled level-time ATR
        },
    })
    pos = create_position(sig, _SpecStub())
    assert pos.atr == 2.5


def test_create_position_zero_when_no_atr_anywhere():
    sig = _mk_signal({"indicators": {"atr_src": "na"}})
    pos = create_position(sig, _SpecStub())
    assert pos.atr == 0.0


def test_create_position_top_level_atr_used_for_levels_wins():
    sig = _mk_signal({
        "atr_used_for_levels": 3.0,
        "indicators": {"atr": 0.5},
    })
    pos = create_position(sig, _SpecStub())
    assert pos.atr == 3.0
