import pytest
from services.l3_lite_tracker import L3LiteTracker

def test_taker_flow_imb_calculation():
    tracker = L3LiteTracker(alpha=0.1, min_dt_ms=10)
    # first snap to init
    tracker.on_book(ts=1000, depth_bid_5=100.0, depth_ask_5=100.0)
    
    # some trades
    tracker.on_trade(ts=1050, qty=10.0, side=1)  # buy
    tracker.on_trade(ts=1080, qty=5.0, side=-1)  # sell
    
    # next snap
    tracker.on_book(ts=1100, depth_bid_5=100.0, depth_ask_5=100.0)
    
    assert tracker.snap.taker_flow_imb > 0.0
    assert tracker.snap.taker_flow_imb_mad_ema >= 0.0
    
    ctx = type("Ctx", (), {})()
    tracker.attach_to_context(ctx)
    assert getattr(ctx, "taker_flow_imb", 0.0) > 0.0
    assert getattr(ctx, "taker_flow_imb_z", 0.0) != 0.0
