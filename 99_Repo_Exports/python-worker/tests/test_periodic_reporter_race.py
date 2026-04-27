
import pytest
from unittest.mock import MagicMock, patch, ANY, call
import time
import json
import fakeredis
import threading

# We cannot import PeriodicReporter at top level because it imports services needing 'requests'
# and we need to patch requests first.

@pytest.fixture
def reporter(monkeypatch):
    # Patch sys.modules to include mocked requests and other services
    with patch.dict("sys.modules", {
        "services.edge_gate_reporter": MagicMock(),
        "analyze_trailing_vs_baseline_postgres": MagicMock(),
        "services.trailing_size_recommender": MagicMock(),
        "requests": MagicMock(),
    }):
        # Now we can import safely
        from services.periodic_reporter import PeriodicReporter
        
        # Use fakeredis for the reporter
        fake_r = fakeredis.FakeRedis(decode_responses=True)
        if not hasattr(fake_r, "ping"):
            fake_r.ping = MagicMock(return_value=True)
        
        # We need to patch get_redis inside the module (which is now imported)
        monkeypatch.setattr("services.periodic_reporter.get_redis", lambda: fake_r)
        monkeypatch.setattr("redis.from_url", lambda url, **kwargs: fake_r)
        
        rep = PeriodicReporter()
        rep.redis = fake_r
        # Mock internal sending method to avoid complex setup
        rep._generate_and_send_report_internal = MagicMock()
        
        yield rep, fake_r

def test_check_and_trigger_report_double_checked_locking(reporter, monkeypatch):
    """
    Simulate a race condition where two threads try to trigger the report at the same time.
    Verify that _generate_and_send_report_internal is called ONLY ONCE.
    """
    rep, r = reporter
    
    # Setup context
    src = "CryptoOrderFlow"
    sym = "ALL"
    
    # Mock time to a fixed hour
    import datetime
    fixed_now = datetime.datetime(2026, 1, 15, 12, 10, 0, tzinfo=datetime.timezone.utc)
    
    hour_key = f"report_last_hourly_hour:CryptoOrderFlow:ALL"
    # Ensure key is missing/different
    r.set(hour_key, "2026-01-15-11") 
    
    with patch("services.periodic_reporter.datetime") as mock_dt:
        mock_dt.fromtimestamp.return_value = fixed_now
        mock_dt.now.return_value = fixed_now
        
        # Test Case 1: First caller wins lock and sends report
        
        rep._acquire_lock = MagicMock(return_value=True)
        rep._release_lock = MagicMock()
        
        rep._check_and_trigger_report(src, sym)
        
        rep._acquire_lock.assert_called()
        rep._generate_and_send_report_internal.assert_called_once()
        assert r.get(hour_key) == "2026-01-15-12"
        
        # Reset
        rep._generate_and_send_report_internal.reset_mock()
        rep._acquire_lock.reset_mock()
        rep._release_lock.reset_mock()
        
        # Test Case 2: Second caller calls AFTER key is set (normal exclusion)
        
        rep._check_and_trigger_report(src, sym)
        rep._acquire_lock.assert_not_called()
        rep._generate_and_send_report_internal.assert_not_called()
        
        # Test Case 3: Race Condition - Key is NOT set initially, but IS set when inside lock
        
        # Reset key to previous hour
        r.set(hour_key, "2026-01-15-11")
        
        # Original get to allow first call
        original_get = r.get
        
        # But we patch acquire_lock to update Redis "simulating other thread work"
        def side_effect_acquire(*args, **kwargs):
            # Simulate another thread finished just now
            r.set(hour_key, "2026-01-15-12")
            return True
        
        rep._acquire_lock = MagicMock(side_effect=side_effect_acquire)
        
        # Check trigger
        rep._check_and_trigger_report(src, sym)
        
        # Assertions
        rep._acquire_lock.assert_called()
        rep._generate_and_send_report_internal.assert_not_called()
        rep._release_lock.assert_called()
        
        print("Race condition double-check test passed!")
