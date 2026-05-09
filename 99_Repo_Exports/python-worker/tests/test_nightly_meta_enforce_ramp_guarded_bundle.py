"""Tests for nightly_meta_enforce_ramp_guarded_bundle.py"""

import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from tools.nightly_meta_enforce_ramp_guarded_bundle import main, now_ms, sign


def test_sign():
    """Test HMAC signature generation."""
    bid = "abc123"
    secret = "test_secret"
    sig = sign(bid, secret)
    assert len(sig) == 8
    assert isinstance(sig, str)
    # Deterministic
    assert sign(bid, secret) == sign(bid, secret)


def test_now_ms():
    """Test timestamp generation."""
    ts1 = now_ms()
    assert ts1 > 0
    assert isinstance(ts1, int)
    time.sleep(0.01)
    ts2 = now_ms()
    assert ts2 > ts1


@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.redis.Redis")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.subprocess.check_call")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.os.makedirs")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.time.strftime")
def test_main_streak_gate(mock_strftime, mock_makedirs, mock_subprocess, mock_redis_class):
    """Test that streak gate blocks ramp if streak is insufficient."""
    mock_r = MagicMock()
    mock_redis_class.from_url.return_value = mock_r

    # Streak gate fails
    mock_r.get.side_effect = lambda k: {
        "sre:regress:pass_streak": "2",  # < 3
        "sre:regress:last_status": "PASS",
        "sre:regress:last_ts_ms": str(now_ms() - 1000),
    }.get(k, "0")

    with patch.dict(os.environ, {
        "CANARY_SYMBOLS": "BTCUSDT",
        "META_ENFORCE_MIN_STREAK": "3",
        "META_ENFORCE_RAMP_NOTIFY_ON_SKIP": "1",
    }):
        main()

    # Should notify and return early (no bundle created)
    assert mock_r.xadd.called
    assert not mock_r.set.called  # No bundle created


@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.redis.Redis")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.subprocess.check_call")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.os.makedirs")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.time.strftime")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.open", create=True)
def test_main_missing_ramp_ts(mock_open, mock_strftime, mock_makedirs, mock_subprocess, mock_redis_class):
    """Test that missing ramp_ts blocks ramp."""
    mock_r = MagicMock()
    mock_redis_class.from_url.return_value = mock_r

    # Streak passes, but ramp_ts missing
    mock_r.get.side_effect = lambda k: {
        "sre:regress:pass_streak": "3",
        "sre:regress:last_status": "PASS",
        "sre:regress:last_ts_ms": str(now_ms() - 1000),
        "sre:of_gate:emergency:last_ms": "0",
        "config:orderflow:BTCUSDT": None,
        "meta:ramp:last_applied_ms": "0",  # Missing
    }.get(k, "0")
    mock_r.hget.return_value = "0.10"  # Current share

    mock_strftime.return_value = "20240101_120000"
    mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
        "decision": {"ok_to_ramp": True}
    })

    with patch.dict(os.environ, {
        "CANARY_SYMBOLS": "BTCUSDT",
        "META_ENFORCE_MIN_STREAK": "3",
        "META_ENFORCE_RAMP_NOTIFY_ON_SKIP": "1",
        "META_RAMP_LAST_APPLIED_MS_KEY": "meta:ramp:last_applied_ms",
    }):
        main()

    # Should notify about missing ramp_ts
    assert mock_r.xadd.called
    assert not mock_r.set.called  # No bundle created


@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.redis.Redis")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.subprocess.check_call")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.os.makedirs")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.time.strftime")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.open", create=True)
def test_main_did_block(mock_open, mock_strftime, mock_makedirs, mock_subprocess, mock_redis_class):
    """Test that DiD evaluation blocks ramp if worst-case fails."""
    mock_r = MagicMock()
    mock_redis_class.from_url.return_value = mock_r

    ramp_ts = now_ms() - 100000
    mock_r.get.side_effect = lambda k: {
        "sre:regress:pass_streak": "3",
        "sre:regress:last_status": "PASS",
        "sre:regress:last_ts_ms": str(now_ms() - 1000),
        "sre:of_gate:emergency:last_ms": "0",
        "config:orderflow:BTCUSDT": None,
        "meta:ramp:last_applied_ms": str(ramp_ts),
    }.get(k, "0")
    mock_r.hget.return_value = "0.10"  # Current share

    mock_strftime.return_value = "20240101_120000"

    # DiD evaluation fails
    mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
        "decision": {"ok_to_ramp": False, "reason": "worst_case_failed"},
        "failed_cells": 2,
        "failed_top": [{"cell": "BTCUSDT|trend", "reasons": ["did_tail_p95_not_ok"]}],
        "skipped_top": [],
    })

    with patch.dict(os.environ, {
        "CANARY_SYMBOLS": "BTCUSDT",
        "META_ENFORCE_MIN_STREAK": "3",
        "META_ENFORCE_RAMP_NOTIFY_ON_SKIP": "1",
        "META_RAMP_LAST_APPLIED_MS_KEY": "meta:ramp:last_applied_ms",
        "OUT_DIR": "/tmp/test_out",
    }):
        main()

    # Should notify about DiD block
    assert mock_r.xadd.called
    assert not mock_r.set.called  # No bundle created


@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.redis.Redis")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.subprocess.check_call")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.os.makedirs")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.time.strftime")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.open", create=True)
def test_main_per_regime_bundle(mock_open, mock_strftime, mock_makedirs, mock_subprocess, mock_redis_class):
    """Test that per-regime shares are set correctly when enabled."""
    mock_r = MagicMock()
    mock_redis_class.from_url.return_value = mock_r

    ramp_ts = now_ms() - 100000
    mock_r.get.side_effect = lambda k: {
        "sre:regress:pass_streak": "3",
        "sre:regress:last_status": "PASS",
        "sre:regress:last_ts_ms": str(now_ms() - 1000),
        "sre:of_gate:emergency:last_ms": "0",
        "config:orderflow:BTCUSDT": None,
        "meta:ramp:last_applied_ms": str(ramp_ts),
    }.get(k, "0")
    mock_r.hget.side_effect = lambda k, f: {
        ("config:orderflow:BTCUSDT", "meta_enforce_share_trend"): "0.10",
        ("config:orderflow:BTCUSDT", "meta_enforce_share_range"): "0.10",
    }.get((k, f))

    mock_strftime.return_value = "20240101_120000"

    # DiD evaluation passes
    mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
        "decision": {"ok_to_ramp": True, "reason": "all_cells_passed"},
        "evaluated_cells": 4,
        "failed_cells": 0,
        "skipped_cells": 0,
    })

    with patch.dict(os.environ, {
        "CANARY_SYMBOLS": "BTCUSDT",
        "META_ENFORCE_MIN_STREAK": "3",
        "META_ENFORCE_RAMP_NOTIFY_ON_SKIP": "1",
        "META_RAMP_LAST_APPLIED_MS_KEY": "meta:ramp:last_applied_ms",
        "META_ENFORCE_PER_REGIME": "1",
        "OUT_DIR": "/tmp/test_out",
        "RECS_HMAC_SECRET": "test_secret",
    }):
        main()

    # Should create bundle with per-regime shares
    assert mock_r.set.called
    bundle_call = [c for c in mock_r.set.call_args_list if "recs:bundle:" in str(c[0])]
    assert len(bundle_call) > 0

    bundle_json = bundle_call[0][0][1]
    bundle = json.loads(bundle_json)

    # Check per-regime ops
    ops = bundle["ops"]
    trend_ops = [op for op in ops if op.get("field") == "meta_enforce_share_trend"]
    range_ops = [op for op in ops if op.get("field") == "meta_enforce_share_range"]
    news_ops = [op for op in ops if op.get("field") == "meta_enforce_share_news"]

    assert len(trend_ops) > 0
    assert len(range_ops) > 0
    assert len(news_ops) > 0
    assert news_ops[0]["value"] == "0.00"  # News always 0.00


@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.redis.Redis")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.subprocess.check_call")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.os.makedirs")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.time.strftime")
@patch("tools.nightly_meta_enforce_ramp_guarded_bundle.open", create=True)
def test_main_legacy_share_bundle(mock_open, mock_strftime, mock_makedirs, mock_subprocess, mock_redis_class):
    """Test that legacy share is used when per-regime is disabled."""
    mock_r = MagicMock()
    mock_redis_class.from_url.return_value = mock_r

    ramp_ts = now_ms() - 100000
    mock_r.get.side_effect = lambda k: {
        "sre:regress:pass_streak": "3",
        "sre:regress:last_status": "PASS",
        "sre:regress:last_ts_ms": str(now_ms() - 1000),
        "sre:of_gate:emergency:last_ms": "0",
        "config:orderflow:BTCUSDT": None,
        "meta:ramp:last_applied_ms": str(ramp_ts),
    }.get(k, "0")
    mock_r.hget.return_value = "0.10"  # Current share

    mock_strftime.return_value = "20240101_120000"

    # DiD evaluation passes
    mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
        "decision": {"ok_to_ramp": True, "reason": "all_cells_passed"},
        "evaluated_cells": 4,
        "failed_cells": 0,
        "skipped_cells": 0,
    })

    with patch.dict(os.environ, {
        "CANARY_SYMBOLS": "BTCUSDT",
        "META_ENFORCE_MIN_STREAK": "3",
        "META_ENFORCE_RAMP_NOTIFY_ON_SKIP": "1",
        "META_RAMP_LAST_APPLIED_MS_KEY": "meta:ramp:last_applied_ms",
        "META_ENFORCE_PER_REGIME": "0",  # Disabled
        "OUT_DIR": "/tmp/test_out",
        "RECS_HMAC_SECRET": "test_secret",
    }):
        main()

    # Should create bundle with legacy share
    assert mock_r.set.called
    bundle_call = [c for c in mock_r.set.call_args_list if "recs:bundle:" in str(c[0])]
    assert len(bundle_call) > 0

    bundle_json = bundle_call[0][0][1]
    bundle = json.loads(bundle_json)

    # Check legacy share op
    ops = bundle["ops"]
    share_ops = [op for op in ops if op.get("field") == "meta_enforce_share"]
    assert len(share_ops) > 0
    assert float(share_ops[0]["value"]) > 0.10  # Next in schedule


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

