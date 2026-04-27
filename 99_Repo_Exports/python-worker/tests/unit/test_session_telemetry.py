import pytest
from datetime import datetime, timezone
from services.orderflow.utils import hour_of_week_utc, session_utc, fmt_utc_dow_hour
from core.session_telemetry import HourOfWeekScaleTracker, PassRateBySessionEma

def test_utc_helpers():
    # Monday 10:00 UTC
    # Mon = 0
    # 0 * 24 + 10 = 10
    ts = int(datetime(2024, 1, 22, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert hour_of_week_utc(ts) == 10
    assert session_utc(ts) == "EU"
    assert fmt_utc_dow_hour(ts) == "Mon 10:00 UTC"

    # Sunday 23:59
    # Sun = 6
    # 6 * 24 + 23 = 144 + 23 = 167
    ts_sun = int(datetime(2024, 1, 21, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)
    assert hour_of_week_utc(ts_sun) == 167
    assert session_utc(ts_sun) == "OFF"
    assert fmt_utc_dow_hour(ts_sun) == "Sun 23:00 UTC"

def test_how_scale_tracker():
    tracker = HourOfWeekScaleTracker(
        alpha=0.1,
        min_bucket_n=5,
        min_global_n=10
    )
    
    ts = int(datetime(2024, 1, 22, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    
    # Initial scale 1.0 (not enough samples)
    assert tracker.scale(ts) == 1.0
    
    # Update global activity
    for _ in range(20):
        tracker.update(ts, 100.0) # Bucket 10
        
    # Global EMA ~ 100
    # Bucket 10 EMA ~ 100
    # Scale should be 1.0
    assert pytest.approx(tracker.scale(ts), 0.01) == 1.0
    
    # To test scaling, we need global EMA to be different from bucket EMA.
    # We can do this by updating OTHER buckets.
    ts_other = int(datetime(2024, 1, 22, 11, 0, tzinfo=timezone.utc).timestamp() * 1000)
    for _ in range(10):
        tracker.update(ts_other, 200.0) # Bucket 11
    
    ts_other2 = int(datetime(2024, 1, 22, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
    for _ in range(100):
        tracker.update(ts_other2, 10.0) # This brings global EMA down
        
    s11 = tracker.scale(ts_other)
    assert s11 > 1.0
    
    # Clamping
    assert tracker.scale(ts_other) == 2.0 # clamped high (bucket EMA ~1000, global EMA low)

def test_pass_rate_ema():
    tracker = PassRateBySessionEma(alpha=0.5)
    ts = int(datetime(2024, 1, 22, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    
    # Session EU, Tier 1
    # First update: 1.0 if passed
    p1 = tracker.update(ts, 1, True)
    assert p1 == 1.0
    
    # Second update: (1-0.5)*1.0 + 0.5*0.0 = 0.5
    p2 = tracker.update(ts, 1, False)
    assert p2 == 0.5
    
    assert tracker.get("EU", 1) == 0.5
    assert tracker.get("ASIA", 1) == 0.0
