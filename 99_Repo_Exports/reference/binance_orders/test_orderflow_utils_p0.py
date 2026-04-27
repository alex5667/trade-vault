"""
P0 sanity tests for orderflow/utils.py (no duplicate functions).
"""
import pytest


def test_no_duplicate_functions():
    """Test that hour_of_week_utc, session_utc, fmt_utc_dow_hour are defined only once."""
    import inspect
    from services.orderflow import utils
    funcs = [name for name, obj in inspect.getmembers(utils, inspect.isfunction)]
    
    # Check for duplicates
    for func_name in ["hour_of_week_utc", "session_utc", "fmt_utc_dow_hour"]:
        count = funcs.count(func_name)
        assert count == 1, f"{func_name} defined {count} times (should be 1)"


def test_hour_of_week_utc():
    """Test hour_of_week_utc function."""
    from services.orderflow.utils import hour_of_week_utc
    from datetime import datetime, timezone
    
    # Test with known timestamp (2024-01-01 12:00 UTC = Monday, hour 12)
    # Monday = weekday 0, so hour_of_week = 0 * 24 + 12 = 12
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    result = hour_of_week_utc(ts_ms)
    assert 0 <= result <= 167  # 0..167 range


def test_session_utc():
    """Test session_utc function."""
    from services.orderflow.utils import session_utc
    from datetime import datetime, timezone
    
    # Test ASIA session (0-8 UTC)
    dt = datetime(2024, 1, 1, 4, 0, 0, tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    assert session_utc(ts_ms) == "ASIA"
    
    # Test EU session (8-14 UTC)
    dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    assert session_utc(ts_ms) == "EU"
    
    # Test NY session (14-21 UTC)
    dt = datetime(2024, 1, 1, 16, 0, 0, tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    assert session_utc(ts_ms) == "NY"
    
    # Test OFF session (21-24 UTC)
    dt = datetime(2024, 1, 1, 22, 0, 0, tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    assert session_utc(ts_ms) == "OFF"


def test_fmt_utc_dow_hour():
    """Test fmt_utc_dow_hour function."""
    from services.orderflow.utils import fmt_utc_dow_hour
    from datetime import datetime, timezone
    
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    result = fmt_utc_dow_hour(ts_ms)
    assert "UTC" in result
    assert ":" in result

