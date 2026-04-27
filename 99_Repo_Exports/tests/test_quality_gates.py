from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.utils.quality_gates import (
    DataQualityGate,
    RegimeSessionLiquidityGate,
    SignalConsistencyGate,
)


def _ctx(**kwargs):
    return SimpleNamespace(**kwargs)


def _of(**kwargs):
    return SimpleNamespace(**kwargs)


def test_regime_liquidity_gate_blocks_breakout_in_range(monkeypatch):
    # Enable and apply to breakout
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_APPLY_KINDS", "breakout")
    monkeypatch.setenv("QUALITY_ALLOW_REGIMES__BREAKOUT", "trending_bull,trending_bear,expansion")
    # Liquidity defaults
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS_DEFAULT", "8")

    gate = RegimeSessionLiquidityGate.from_env()
    ctx = _ctx(
        of=_of(regime="range", spread_bps=2.0, depth_bid_5=100, depth_ask_5=100, burst_flip_ratio=0.1),
        session="us_main",
        ts_event_ms=int(time.time() * 1000),
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_REGIME_NOT_ALLOWED"


def test_regime_liquidity_gate_blocks_wide_spread(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_APPLY_KINDS", "breakout")
    monkeypatch.setenv("QUALITY_ALLOW_REGIMES__BREAKOUT", "range,trending_bull,trending_bear,expansion")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS_DEFAULT", "5.0")

    gate = RegimeSessionLiquidityGate.from_env()
    ctx = _ctx(
        of=_of(regime="trending_bull", spread_bps=9.0, depth_bid_5=100, depth_ask_5=100, burst_flip_ratio=0.1),
        session="us_main",
        ts_event_ms=int(time.time() * 1000),
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_SPREAD_TOO_WIDE"


def test_consistency_gate_breakout_requires_microshift_and_obi(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "breakout")
    monkeypatch.setenv("BREAKOUT_REQUIRE_OBI", "1")
    monkeypatch.setenv("BREAKOUT_REQUIRE_OBI20", "1")
    monkeypatch.setenv("BREAKOUT_MIN_MICROPRICE_SHIFT_BPS", "0.2")
    monkeypatch.setenv("CONS_BREAKOUT_MIN_Z", "2.0")
    monkeypatch.setenv("CONS_BREAKOUT_MIN_OBI", "0.5")

    gate = SignalConsistencyGate.from_env()
    ctx = _ctx(
        of=_of(
            z_delta=3.0,
            obi=0.2,         # too weak
            obi_20=0.25,     # ok sign
            microprice_shift_bps_20=0.10,  # too low
        )
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code in {"VETO_BREAKOUT_OBI_TOO_WEAK", "VETO_BREAKOUT_MICROSHIFT_TOO_LOW"}


def test_data_quality_gate_out_of_order(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_REQUIRE_EPOCH_TS", "1")
    monkeypatch.setenv("DATA_OUT_OF_ORDER_TOL_MS", "1000")
    monkeypatch.setenv("DATA_MAX_EVENT_LAG_MS", "999999")  # do not veto by lag here
    monkeypatch.setenv("DATA_MAX_FUTURE_SKEW_MS", "999999")
    monkeypatch.setenv("DATA_QUARANTINE_VETO", "0")
    monkeypatch.setenv("DATA_STRICT_MISSING_ATR_TS", "0")

    gate = DataQualityGate.from_env()
    now = int(time.time() * 1000)
    last = now
    # Event comes too far behind last watermark (beyond tolerance)
    ctx = _ctx(ts_event_ms=now - 10_000)
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=last)
    assert dec.veto is True
    assert dec.reason_code == "VETO_OUT_OF_ORDER"
