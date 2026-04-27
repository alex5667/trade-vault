from __future__ import annotations

from types import SimpleNamespace

from handlers.confirmations.l2_confirmations import l2_confirm_breakout, l2_confirm_absorption
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level
from common.reason_codes import ReasonCode


def test_wrapper_breakout_spread_wide_veto_has_structured_code():
    ctx = SimpleNamespace(spread_bps=50.0, microprice_shift_bps_20=0.0, ts_ms=1000.0, l2_ts_ms=1000.0, price=100.0)
    l2 = L2Snapshot(bids=[], asks=[])
    r = l2_confirm_breakout(ctx=ctx, l2=l2, level_price=100.0, side="buy", max_spread_bps=8.0)
    assert r.veto is True
    assert r.reason_code == ReasonCode.VETO_SPREAD_WIDE.value
    assert r.reason_u16 > 0


def test_wrapper_absorption_taker_low_veto_has_structured_code():
    ctx = SimpleNamespace(taker_rate_ema=0.001, ts_ms=1000.0, l2_ts_ms=1000.0, price=100.0)
    l2 = L2Snapshot(bids=[], asks=[])
    r = l2_confirm_absorption(ctx=ctx, l2=l2, level_price=100.0, side="buy", min_taker_rate=0.05)
    assert r.veto is True
    assert r.reason_code == ReasonCode.VETO_TAKER_RATE_LOW.value
