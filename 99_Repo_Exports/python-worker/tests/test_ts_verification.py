import pytest
from common.tick_time import verify_bucketed_ts

def test_verify_bucketed_ts_ok():
    # Identical buckets: diff=0, tol=5
    res = verify_bucketed_ts(actual_ts_ms=1003, expected_ts_ms=1007, bucket_ms=10)
    assert res.ok is True
    assert res.severity == "ok"
    assert res.meta["diff_ms"] == 0

def test_verify_bucketed_ts_boundary_ok():
    # actual bucket = 1000, expected bucket = 1010, diff = 10
    # tol = 10. Strict > means 10 > 10 is False -> OK.
    res = verify_bucketed_ts(actual_ts_ms=1000, expected_ts_ms=1010, bucket_ms=10, tol_ms=10)
    assert res.ok is True
    assert res.severity == "ok"
    assert res.meta["diff_ms"] == 10

def test_verify_bucketed_ts_warn():
    # actual bucket = 1000, expected bucket = 1020, diff = 20
    # tol = 10, hard = 30. 20 > 10 is True, 20 > 30 is False -> Warn.
    res = verify_bucketed_ts(actual_ts_ms=1000, expected_ts_ms=1020, bucket_ms=10, tol_ms=10, hard_ms=30)
    assert res.ok is False
    assert res.severity == "warn"
    assert res.reason == "replay_ts_mismatch"
    assert res.meta["diff_ms"] == 20

def test_verify_bucketed_ts_severe():
    # actual bucket = 1000, expected bucket = 1040, diff = 40
    # tol = 10, hard = 30. 40 > 30 is True -> Severe.
    res = verify_bucketed_ts(actual_ts_ms=1000, expected_ts_ms=1040, bucket_ms=10, tol_ms=10, hard_ms=30)
    assert res.ok is False
    assert res.severity == "severe"
    assert res.reason == "replay_ts_mismatch_severe"
    assert res.meta["diff_ms"] == 40

def test_verify_bucketed_ts_default_tol():
    # bucket_ms = 100 => tol = 50, hard = 150
    # diff = 100. 100 > 50 is True -> Warn.
    res = verify_bucketed_ts(actual_ts_ms=1000, expected_ts_ms=1100, bucket_ms=100)
    assert res.ok is False
    assert res.severity == "warn"
    assert res.meta["tol_ms"] == 50
    assert res.meta["hard_ms"] == 150

def test_verify_bucketed_ts_bucket_disabled():
    res = verify_bucketed_ts(actual_ts_ms=1000, expected_ts_ms=2000, bucket_ms=0)
    assert res.ok is True
    assert res.reason == "bucket_disabled"
