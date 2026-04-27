import pytest
from unittest.mock import MagicMock, patch
from services.binance_executor import BinanceExecutor

@pytest.fixture
def executor():
    import os
    os.environ["BINANCE_DEMO_API_KEY"] = "fake"
    os.environ["BINANCE_DEMO_API_SECRET"] = "fake"
    ex = BinanceExecutor()
    ex.client = MagicMock()
    ex.demo_client = MagicMock()
    # Mock to bypass full execution
    ex._quantize = MagicMock(return_value=(0.001, "50000"))
    ex._get_available_balance = MagicMock(return_value=1000.0)
    ex._submit_plain_order_with_reconcile = MagicMock(return_value={"orderId": 12345})
    ex._wait_fill = MagicMock(return_value={"status": "FILLED", "executedQty": 0.001, "avgPrice": 50000})
    ex._place_protective = MagicMock(return_value={})
    return ex

def test_range_regime_tp_ratio_override(executor):
    """Verify that a custom tp_ratio (e.g., from range regime) is passed to _place_protective"""

    # In a range regime, we might only have 1 TP and want to close 100% at TP1
    payload = {
        "sid": "test_range_tp",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "qty": 1.0, 
        "entry": 50000,
        "sl": 49000,
        "tp_levels": [51000],
        # Range regime override: 100% close at TP1
        "tp_ratio": [1.0], 
        "is_virtual": False
    }
    
    res = executor.handle_open(payload)
    
    # Assert _place_protective was called with the overridden tp_ratio
    executor._place_protective.assert_called_once()
    kwargs = executor._place_protective.call_args.kwargs
    
    assert "tp_ratio" in kwargs
    assert kwargs["tp_ratio"] == [1.0]

def test_default_tp_ratio(executor):
    """Verify that when no tp_ratio is provided, it passes None/default to _place_protective"""

    payload = {
        "sid": "test_default_tp",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "qty": 1.0, 
        "entry": 50000,
        "sl": 49000,
        "tp_levels": [51000, 52000, 53000],
        "is_virtual": False
    }
    
    res = executor.handle_open(payload)
    
    executor._place_protective.assert_called_once()
    kwargs = executor._place_protective.call_args.kwargs
    
    # Should be None if not provided in payload, so executor will fallback to config defaults
    assert kwargs.get("tp_ratio") is None
