import pytest
from unittest.mock import MagicMock, patch
from services.orderflow.components.book_processor import BookProcessor
from services.orderflow.runtime import SymbolRuntime

@pytest.fixture
def runtime():
    rt = SymbolRuntime(symbol="BTCUSDT", config={})
    rt.dynamic_cfg = {}
    return rt

@patch('services.orderflow.components.book_processor.BookProcessor.__init__', return_value=None)
def test_process_book_happy_path(mock_init, runtime):
    processor = BookProcessor()
    # Manually configure since we mocked __init__
    processor.logger = MagicMock()
    processor.book_rate_ema_gauge = MagicMock()
    
    # Valid book payload
    payload = {
        "m": {"ts_ms": 1600000000000},
        "bids": [[ "60000.0", "1.0" ], [ "59999.0", "2.0" ]],
        "asks": [[ "60001.0", "1.5" ], [ "60002.0", "1.5" ]],
        "u": 12345
    }
    
    ingest_ts_ms = 1600000000050
    result = processor.process_book(runtime, payload, ingest_ts_ms)
    
    assert result is True
    assert runtime.last_book_ts_ms == 1600000000000
    # verify P0 fix was applied via _book_health_initialized
    assert getattr(runtime, "_book_health_initialized", False) is True
    assert runtime.last_book_health_ok == 1
    assert runtime.last_book_health == "OK"

@patch('services.orderflow.components.book_processor.BookProcessor.__init__', return_value=None)
def test_process_book_error_handling(mock_init, runtime):
    processor = BookProcessor()
    processor.logger = MagicMock()
    
    # Invalid book payload - will raise exception during process_book
    payload = {
        "m": {"ts_ms": 1600000000000},
        "bids": "this shouldn't be a string",
        "asks": "this shouldn't be a string",
    }
    
    ingest_ts_ms = 1600000000050
    result = processor.process_book(runtime, payload, ingest_ts_ms)
    
    # Should safely catch error, log metric, and return False
    assert result is False
