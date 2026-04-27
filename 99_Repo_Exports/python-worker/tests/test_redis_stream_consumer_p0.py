"""
P0 sanity tests for redis_stream_consumer.py (no duplicate _parse_xpending_consumers).
"""
import pytest


def test_no_duplicate_parse_xpending_consumers():
    """Test that _parse_xpending_consumers is defined only once."""
    import inspect
    from core import redis_stream_consumer
    funcs = [name for name, obj in inspect.getmembers(redis_stream_consumer, inspect.isfunction)]
    # Count occurrences of _parse_xpending_consumers
    count = funcs.count("_parse_xpending_consumers")
    assert count == 1, f"_parse_xpending_consumers defined {count} times (should be 1)"


def test_parse_xpending_consumers():
    """Test _parse_xpending_consumers handles different formats."""
    from core.redis_stream_consumer import _parse_xpending_consumers
    
    # Test dict format
    res_dict = {
        "consumers": [
            {"name": "consumer1", "pending": 5},
            {"name": "consumer2", "pending": 10}
        ]
    }
    result = _parse_xpending_consumers(res_dict)
    assert result == {"consumer1": 5, "consumer2": 10}
    
    # Test tuple format
    res_tuple = (15, "0-0", "1-0", [["consumer1", 5], ["consumer2", 10]])
    result = _parse_xpending_consumers(res_tuple)
    assert result == {"consumer1": 5, "consumer2": 10}
    
    # Test None
    assert _parse_xpending_consumers(None) == {}

