from __future__ import annotations

from orderflow_services.of_hawkes_vpin_v1 import (
    HawkesVPINState,
    _tick_taker_qty,
    _tick_ts_ms,
)


def test_tick_ts_ms_prefers_event_time():
    assert _tick_ts_ms({"event_time_ms": "1000", "ts": "2000"}) == 1000.0
    assert _tick_ts_ms({"ts": "2000"}) == 2000.0


def test_tick_taker_qty_from_side_buy():
    buy, sell = _tick_taker_qty({"side": "BUY", "qty": "1.5"})
    assert buy == 1.5
    assert sell == 0.0


def test_tick_taker_qty_from_side_sell():
    buy, sell = _tick_taker_qty({"side": "SELL", "quantity": "2"})
    assert buy == 0.0
    assert sell == 2.0


def test_tick_taker_qty_legacy_fields_win():
    buy, sell = _tick_taker_qty(
        {"taker_buy_qty": "3", "taker_sell_qty": "1", "side": "BUY", "qty": "9"}
    )
    assert buy == 3.0
    assert sell == 1.0


def test_hawkes_state_nonzero_after_buy_ticks():
    st = HawkesVPINState("BTCUSDT", beta=0.0333, alpha=0.5, vpin_alpha=0.05)
    out = st.update(1.0, taker_buy_qty=1.0, taker_sell_qty=0.0)
    out2 = st.update(1.1, taker_buy_qty=0.5, taker_sell_qty=0.5)
    assert out2["hawkes_taker_buy_lam"] > 0.0 or out2["hawkes_taker_sell_lam"] > 0.0
    assert out2["vpin_tox_ema"] != 0.5 or out["vpin_tox_ema"] != 0.5
