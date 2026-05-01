from __future__ import annotations
"""Tests for ML threshold proposer v1."""

import json
import os
import time
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List

import pytest
import redis

from tools.ml_threshold_proposer_v1 import (
    now_ms,
    sign,
    notify,
    make_bundle_hset,
    write_bundle,
    _f,
    _i,
    filter_rows,
    impact,
    main,
)
from tools.ml_metrics_agg_v3 import agg_health_ml_confirm, pick_threshold
from core.share_map import parse_map, dump_map, merge_updates


def test_now_ms():
    """Test now_ms returns milliseconds."""
    t1 = now_ms()
    time.sleep(0.01)
    t2 = now_ms()
    assert t2 > t1
    assert isinstance(t1, int)
    assert isinstance(t2, int)


def test_sign():
    """Test HMAC signature generation."""
    secret = "test_secret"
    bid = "test_bundle_id"
    sig1 = sign(bid, secret)
    sig2 = sign(bid, secret)
    assert sig1 == sig2
    assert len(sig1) == 8
    sig3 = sign("other_id", secret)
    assert sig3 != sig1


def test_notify(mocker):
    """Test notify function sends to Telegram stream."""
    mock_redis = MagicMock()
    mock_xadd = MagicMock()
    mock_redis.xadd = mock_xadd
    
    notify(mock_redis, "Test message")
    assert mock_xadd.called
    call_args = mock_xadd.call_args
    assert call_args[0][0] == os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    assert "text" in call_args[0][1]
    assert call_args[0][1]["text"] == "Test message"


def test_notify_with_buttons(mocker):
    """Test notify with inline buttons."""
    mock_redis = MagicMock()
    mock_xadd = MagicMock()
    mock_redis.xadd = mock_xadd
    
    buttons = [[{"text": "Test", "callback": "test:callback"}]]
    notify(mock_redis, "Test", buttons)
    call_args = mock_xadd.call_args
    assert "buttons" in call_args[0][1]
    buttons_json = json.loads(call_args[0][1]["buttons"])
    assert len(buttons_json) == 1


def test_make_bundle_hset():
    """Test bundle creation for HSET operations."""
    with patch.dict(os.environ, {"RECS_HMAC_SECRET": "test_secret"}):
        bid, sig, bundle = make_bundle_hset("cfg:ml_confirm", {"field1": "value1"}, "test_user", 3600)
        assert isinstance(bid, str)
        assert len(bid) > 0
        assert isinstance(sig, str)
        assert len(sig) == 8
        assert bundle["id"] == bid
        assert bundle["who"] == "test_user"
        assert bundle["ttl_sec"] == 3600
        assert len(bundle["ops"]) == 1
        assert bundle["ops"][0]["op"] == "HSET"
        assert bundle["ops"][0]["key"] == "cfg:ml_confirm"
        assert bundle["ops"][0]["field"] == "field1"
        assert bundle["ops"][0]["value"] == "value1"


def test_write_bundle():
    """Test writing bundle to Redis."""
    mock_redis = MagicMock()
    mock_set = MagicMock()
    mock_redis.set = mock_set
    
    bundle = {"id": "test_bid", "ops": []}
    write_bundle(mock_redis, "test_bid", bundle, 3600)
    assert mock_set.call_count == 2
    # Check bundle write
    bundle_call = [c for c in mock_set.call_args_list if "recs:bundle:" in str(c[0][0])]
    assert len(bundle_call) == 1
    # Check status write
    status_call = [c for c in mock_set.call_args_list if "recs:status:" in str(c[0][0])]
    assert len(status_call) == 1


def test_f():
    """Test float conversion."""
    assert _f("1.5") == 1.5
    assert _f(1.5) == 1.5
    assert _f(None, 0.0) == 0.0
    assert _f("invalid", 1.0) == 1.0


def test_i():
    """Test int conversion."""
    assert _i("5") == 5
    assert _i(5.7) == 5
    assert _i(None, 0) == 0
    assert _i("invalid", 1) == 1


def test_filter_rows():
    """Test filtering rows by bucket and symbol."""
    rows = [
        {"bucket": "trend", "symbol": "BTCUSDT", "p_edge": "0.6"},
        {"bucket": "range", "symbol": "BTCUSDT", "p_edge": "0.5"},
        {"bucket": "trend", "symbol": "ETHUSDT", "p_edge": "0.7"},
        {"bucket": "trend", "symbol": "btcusdt", "p_edge": "0.65"},  # lowercase
    ]
    filtered = filter_rows(rows, "trend", "BTCUSDT")
    assert len(filtered) == 2
    assert all(r["bucket"].lower() == "trend" for r in filtered)
    assert all(r["symbol"].upper() == "BTCUSDT" for r in filtered)


def test_impact():
    """Test impact calculation from ml_confirm rows."""
    rows = [
        {"bucket": "trend", "symbol": "BTCUSDT", "enforce": "1", "ok_rule": "1", "missing": "0", "p_edge": "0.5"},
        {"bucket": "trend", "symbol": "BTCUSDT", "enforce": "1", "ok_rule": "1", "missing": "0", "p_edge": "0.6"},
        {"bucket": "trend", "symbol": "BTCUSDT", "enforce": "1", "ok_rule": "1", "missing": "0", "p_edge": "0.7"},
        {"bucket": "trend", "symbol": "BTCUSDT", "enforce": "0", "ok_rule": "1", "missing": "0", "p_edge": "0.4"},  # enforce=0
        {"bucket": "range", "symbol": "BTCUSDT", "enforce": "1", "ok_rule": "1", "missing": "0", "p_edge": "0.5"},  # wrong bucket
    ]
    result = impact(rows, "trend", "BTCUSDT", 0.55, 0.65)
    assert result["total"] == 3
    assert result["blocked_old"] == 1  # p_edge < 0.55
    assert result["blocked_new"] == 2  # p_edge < 0.65
    assert result["delta_block"] == 1


def test_impact_empty():
    """Test impact with no matching rows."""
    rows = [
        {"bucket": "range", "symbol": "ETHUSDT", "enforce": "1", "ok_rule": "1", "missing": "0", "p_edge": "0.5"},
    ]
    result = impact(rows, "trend", "BTCUSDT", 0.5, 0.6)
    assert result["total"] == 0
    assert result["blocked_old"] == 0
    assert result["blocked_new"] == 0
    assert result["delta_block"] == 0


@pytest.mark.skipif(os.getenv("SKIP_INTEGRATION_TESTS") == "1", reason="Integration test")
def test_main_health_gate_fail(mocker):
    """Test main() exits early when health checks fail."""
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}
    mock_redis.get.return_value = None  # no pending
    
    # Mock read_recent_stream to return empty health metrics
    def mock_read(stream, since_ms, max_scan):
        return []
    
    with patch("tools.ml_threshold_proposer_v1.redis.Redis.from_url", return_value=mock_redis):
        with patch("tools.ml_threshold_proposer_v1.read_recent_stream", side_effect=mock_read):
            # Should return early due to health check failure
            main()
            # Should not have called xadd for notifications
            assert not hasattr(mock_redis, "xadd") or not mock_redis.xadd.called


def test_main_pending_skip(mocker):
    """Test main() skips when pending proposal exists."""
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}
    mock_redis.get.return_value = '{"bundle_id":"test","kind":"pmin_proposal"}'  # pending exists
    
    with patch("tools.ml_threshold_proposer_v1.redis.Redis.from_url", return_value=mock_redis):
        main()
        # Should return early, no further processing
        assert not mock_redis.xadd.called

