from __future__ import annotations

from datetime import datetime, timezone

from core.tick_cvd import TickCVDState
from core.crypto_orderflow_detectors import classify_signed_qty


def _ms(y, m, d, hh=0, mm=0, ss=0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp() * 1000)


def test_classify_signed_qty_binance():
    # is_buyer_maker=False => taker BUY => positive
    t1 = {"qty": 1.5, "is_buyer_maker": False}
    # is_buyer_maker=True => taker SELL => negative
    t2 = {"qty": 2.5, "is_buyer_maker": True}
    
    assert classify_signed_qty(t1) == 1.5
    assert classify_signed_qty(t2) == -2.5


def test_classify_signed_qty_generic():
    t1 = {"qty": 10.0, "side": "BUY"}
    t2 = {"volume": 5.0, "side": "sell"}
    t3 = {"qty": 1.0, "side": "unknown"}
    
    assert classify_signed_qty(t1) == 10.0
    assert classify_signed_qty(t2) == -5.0
    assert classify_signed_qty(t3) == 0.0


def test_tick_cvd_accumulation():
    st = TickCVDState(symbol="BTCUSDT", reset_mode="none", ema_period_delta=5, ema_period_cvd=5)
    
    # Tick 1
    st.update({"ts": _ms(2026, 1, 10, 10, 0, 0), "qty": 100.0, "side": "BUY"})
    assert st.cvd_tick == 100.0
    assert st.ema_delta == 100.0
    
    # Tick 2
    st.update({"ts": _ms(2026, 1, 10, 10, 0, 1), "qty": 50.0, "side": "SELL"})
    assert st.cvd_tick == 50.0
    # alpha = 2 / (5+1) = 1/3
    # ema_delta = 100 + (1/3)*(-50 - 100) = 100 - 50 = 50
    assert abs(st.ema_delta - 50.0) < 1e-7


def test_tick_cvd_daily_reset():
    st = TickCVDState(symbol="BTCUSDT", reset_mode="day")
    
    # Day 1
    st.update({"ts": _ms(2026, 1, 10, 23, 59, 59), "qty": 10.0, "side": "BUY"})
    assert st.cvd_tick == 10.0
    assert st.reset_count == 0
    
    # Day 2
    st.update({"ts": _ms(2026, 1, 11, 0, 0, 1), "qty": 5.0, "side": "BUY"})
    assert st.reset_count == 1
    assert st.cvd_tick == 5.0  # Reset to 0 then +5
    assert st.ema_delta == 5.0


def test_tick_cvd_robust_stats():
    st = TickCVDState(symbol="BTCUSDT", reset_mode="none", robust_window=100)
    
    # Fill with some data
    for _ in range(10):
        st.update({"ts": 1000, "qty": 1.0, "side": "BUY"})
    for _ in range(10):
        st.update({"ts": 1000, "qty": 1.0, "side": "SELL"})
        
    # Median of 10x 1.0 and 10x -1.0 should be 0.0
    # last_delta is -1.0
    snap = st.robust_snapshot()
    assert snap["delta_med"] == 0.0
    assert snap["delta_mad"] == 1.0
    # mad_scale * 1.0 = 1.4826
    # rz = (-1.0 - 0.0) / 1.4826 = -0.6744
    assert abs(snap["delta_robust_z"] - (-0.6744)) < 0.01


def test_tick_cvd_bad_time_handling():
    st = TickCVDState(symbol="BTCUSDT", reset_mode="day")
    
    # Valid time
    st.update({"ts": _ms(2026, 1, 10, 10, 0, 0), "qty": 1.0, "side": "BUY"})
    assert st.reset_skipped_bad_time == 0
    
    # Bad time
    st.update({"ts": "invalid", "qty": 1.0, "side": "BUY"})
    assert st.reset_skipped_bad_time == 1
    assert st.cvd_tick == 2.0  # Still accumulates
