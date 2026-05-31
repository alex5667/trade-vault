from __future__ import annotations

import types

from handlers.confirmations.engine import ConfirmationsEngine
from signal_scoring.reason_registry import reason_code_to_u16


class _Metrics:
    def __init__(self) -> None:
        self.inc_calls = []
        self.gauge_calls = []
    def inc(self, name, value=1, tags=None):
        self.inc_calls.append((name, value, dict(tags or {})))
    def gauge(self, name, value, tags=None):
        self.gauge_calls.append((name, float(value), dict(tags or {})))


class _BreakoutValidator:
    def confirm(self, *, ctx, l2, side, level_price):
        # force wall veto (structured)
        return types.SimpleNamespace(veto=True, score01=0.0, reason="near_big_wall", reason_code="VETO_WALL_NEAR", reason_u16=0, flags={}, reasons=["near_big_wall"])


def test_validate_sets_structured_reason_code_and_u16_and_metrics():
    m = _Metrics()
    eng = ConfirmationsEngine(breakout_validator=_BreakoutValidator(), metrics=m, strict_reason_codes=False)
    ctx = types.SimpleNamespace(symbol="BTCUSDT", ts_ms=10_000, l2_ts_ms=10_000, spread_bps=1.0, side=1)
    v = eng.validate(kind="breakout", ctx=ctx, l2=object(), l3=None, level_price=100.0)
    assert v.veto is True
    assert v.reason_code == "VETO_WALL_NEAR"
    assert v.reason_u16 == reason_code_to_u16("VETO_WALL_NEAR")
    assert any(c[0] == "signals_veto_total" and c[2].get("reason") == "VETO_WALL_NEAR" for c in m.inc_calls)


def test_validate_exports_l2_stale_metrics_when_stale():
    m = _Metrics()
    eng = ConfirmationsEngine(breakout_validator=_BreakoutValidator(), metrics=m)
    # stale: ts_ms - l2_ts_ms > 1500
    ctx = types.SimpleNamespace(symbol="BTCUSDT", ts_ms=10_000, l2_ts_ms=7_000, spread_bps=1.0, side=1)
    _ = eng.validate(kind="breakout", ctx=ctx, l2=object(), l3=None, level_price=100.0)
    assert any(c[0] == "l2_checks_total" for c in m.inc_calls)
    assert any(c[0] == "l2_stale_hits_total" for c in m.inc_calls)
    assert any(c[0] == "l2_stale_rate" for c in m.gauge_calls)
