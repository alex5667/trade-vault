from __future__ import annotations

from services.entry_policy_ab_report_service import EntryPolicyABReportService


def test_ret_bps_sign():
    """Verify return calculation sign correctness."""
    svc = EntryPolicyABReportService(redis_url="redis://localhost:6379/0")
    # LONG wins if price goes up
    r1 = svc._ret_bps(entry_px=100.0, now_px=101.0, side="LONG")
    r2 = svc._ret_bps(entry_px=100.0, now_px=101.0, side="SHORT")
    assert r1 > 0
    assert r2 < 0


def test_ret_bps_magnitude():
    """Verify return calculation magnitude."""
    svc = EntryPolicyABReportService(redis_url="redis://localhost:6379/0")
    # 1% move should be ~100 bps
    r = svc._ret_bps(entry_px=100.0, now_px=101.0, side="LONG")
    assert 95 < r < 105  # Allow small rounding variance


def test_pid_uniqueness():
    """Verify pending ID generation is unique per entry."""
    svc = EntryPolicyABReportService(redis_url="redis://localhost:6379/0")
    p1 = svc._pid({"symbol": "BTCUSDT", "side": "LONG", "arm": "A", "ts_ms": 1000})
    p2 = svc._pid({"symbol": "BTCUSDT", "side": "LONG", "arm": "A", "ts_ms": 2000})
    p3 = svc._pid({"symbol": "BTCUSDT", "side": "LONG", "arm": "B", "ts_ms": 1000})
    assert p1 != p2  # Different timestamp
    assert p1 != p3  # Different arm
