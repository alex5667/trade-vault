from __future__ import annotations

from core.microbar import MicroBarAggregator


def test_time_bars_close_on_bucket_change():
    agg = MicroBarAggregator(symbol="BTCUSDT", mode="time", tf_ms=1000)

    # Tick 1: 100ms
    out0 = agg.push_tick({"ts": 100, "price": 10, "qty": 1, "side": "BUY"}, cvd_current=1)

    # Tick 2: 900ms (same bar)
    out1 = agg.push_tick({"ts": 900, "price": 11, "qty": 2, "side": "BUY"}, cvd_current=3)

    # Tick 3: 1000ms (new bar starts, closes previous)
    out2 = agg.push_tick({"ts": 1000, "price": 9, "qty": 1, "side": "SELL"}, cvd_current=2)

    assert out0 == []
    assert out1 == []
    assert len(out2) == 1

    b = out2[0]
    assert b.open == 10.0
    assert b.high == 11.0
    assert b.low == 10.0
    assert b.close == 11.0
    assert abs(b.vol - 3.0) < 1e-9
    # delta_sum: +1 + 2 = +3
    assert abs(b.delta_sum - 3.0) < 1e-9
    assert b.cvd_close == 3.0


def test_vwap_and_mid_spread():
    agg = MicroBarAggregator(symbol="BTCUSDT", mode="time", tf_ms=1000)

    # Tick 1: p=10, q=1
    agg.push_tick({"ts": 10, "price": 10, "qty": 1, "side": "BUY", "bid": 9.9, "ask": 10.1}, cvd_current=1)
    # Tick 2: p=20, q=1
    agg.push_tick({"ts": 20, "price": 20, "qty": 1, "side": "BUY", "bid": 19.9, "ask": 20.1}, cvd_current=2)

    # Trigger close
    out = agg.push_tick({"ts": 1000, "price": 30, "qty": 1, "side": "BUY"}, cvd_current=3)

    b = out[0]
    # VWAP = (10*1 + 20*1) / 2 = 15.0
    assert abs(b.vwap - 15.0) < 1e-9
    assert abs(b.mid_last - 20.0) < 1e-9
    assert abs(b.spread_last - 0.2) < 1e-9


def test_volume_bars_close_on_target():
    agg = MicroBarAggregator(symbol="BTCUSDT", mode="volume", volume_target=3.0)

    out0 = agg.push_tick({"ts": 1, "price": 10, "qty": 1, "side": "BUY"}, cvd_current=1)
    # reaches target of 3.0
    out1 = agg.push_tick({"ts": 2, "price": 11, "qty": 2, "side": "BUY"}, cvd_current=3)

    assert out0 == []
    assert len(out1) == 1

    b = out1[0]
    assert abs(b.vol - 3.0) < 1e-9
    assert b.end_ts_ms == 2
