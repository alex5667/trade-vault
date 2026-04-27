import pytest
from services.orderflow.metrics import (
    trade_taker_buy_rate_ema, trade_taker_sell_rate_ema,
    trade_cancel_bid_rate_ema, trade_cancel_ask_rate_ema,
    trade_cancel_to_trade_bid, trade_cancel_to_trade_ask,
    trade_taker_flow_imb_z, trade_book_churn_score, trade_book_churn_hi
)

def test_flow_intensity_metrics_exist():
    # Verify the metrics are correctly instantiated as Prometheus gauges
    assert trade_taker_buy_rate_ema is not None
    assert trade_taker_sell_rate_ema is not None
    assert trade_cancel_bid_rate_ema is not None
    assert trade_cancel_ask_rate_ema is not None
    assert trade_cancel_to_trade_bid is not None
    assert trade_cancel_to_trade_ask is not None
    assert trade_taker_flow_imb_z is not None
    assert trade_book_churn_score is not None
    assert trade_book_churn_hi is not None
    
    # Basic set test to ensure labels work properly without raising exceptions
    trade_taker_buy_rate_ema.labels(sym="TEST_SYM", bucket="B1").set(1.5)
    trade_cancel_to_trade_bid.labels(sym="TEST_SYM", bucket="B1").set(2.0)
