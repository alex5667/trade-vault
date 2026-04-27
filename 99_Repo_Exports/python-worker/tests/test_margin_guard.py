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
    return ex

def test_margin_guard_skips_when_ratio_low(executor):
    """Verify handle_open skips trade if margin ratio < 4.0"""
    
    # Mock balance to 30 USDT
    executor._get_available_balance = MagicMock(return_value=30.0)
    
    # Mock quantize to avoid filters error
    executor._quantize = MagicMock(return_value=(0.001, "50000"))
    
    # Payload requests 10 USDT of margin
    payload = {
        "sid": "test_sid_margin_1",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "qty": 1.0, # Not strictly used since margin explicitly given
        "margin": 10.0,
        "entry": 50000,
        "is_virtual": False
    }
    
    # Ratio = 30.0 / 10.0 = 3.0 < 4.0 -> should skip
    res = executor.handle_open(payload)
    
    assert res == {"status": "skipped", "reason": "margin_guard", "ratio": 3.0}

def test_margin_guard_allows_when_ratio_high(executor):
    """Verify handle_open proceeds if margin ratio >= 4.0"""
    
    # Mock balance to 50 USDT
    executor._get_available_balance = MagicMock(return_value=50.0)
    
    # Mock quantize to bypass execution exceptions
    executor._quantize = MagicMock(return_value=(0.001, "50001"))
    
    # Mock _submit_plain_order
    def fake_submit(*args, **kwargs):
        raise RuntimeError("Submit reached") # We just want to ensure it passed the guard
    
    executor._submit_plain_order_with_reconcile = fake_submit
    
    payload = {
        "sid": "test_sid_margin_2",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "qty": 1.0,
        "margin": 10.0, # Ratio = 50.0 / 10.0 = 5.0 >= 4.0 -> should pass
        "entry": 50000,
        "is_virtual": False
    }
    
    with pytest.raises(RuntimeError, match="Submit reached"):
        executor.handle_open(payload)
