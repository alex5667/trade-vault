import math
from types import SimpleNamespace

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st

from common.signal_log_one_json import build_signal_one_json_obj


finite_floats = st.floats(allow_nan=True, allow_infinity=True, width=64)


@given(
    spread=finite_floats,
    obi_avg=finite_floats,
    mps=finite_floats,
    c2t=finite_floats,
    taker=finite_floats,
    regime=finite_floats,
    geo=finite_floats,
    cf=finite_floats,
)
def test_signal_log_never_raises_on_nan_inf(
    spread, obi_avg, mps, c2t, taker, regime, geo, cf
):
    ctx = SimpleNamespace(
        spread_bps=spread,
        obi_avg=obi_avg,
        microprice_shift_bps_20=mps,
        cancel_to_trade_bid_5s=c2t,
        taker_rate_ema=taker,
        market_regime_score=regime,
        geometry_score=geo,
    )
    payload = {
        "kind": "breakout",
        "side": 1,
        "symbol": "BTCUSDT",
        "ts": 1,
        "signal_id": "sid",
        "raw_score": 1.0,
        "final_score": 0.1,
    }
    obj = build_signal_one_json_obj(payload=payload, ctx=ctx, emitted=True, emit_ok=True, conf_factor01=cf)
    # Invariants: schema stable, conf_factor01 in [0..1] or None
    assert "conf_factor01" in obj
    v = obj["conf_factor01"]
    if v is not None:
        assert 0.0 <= float(v) <= 1.0
