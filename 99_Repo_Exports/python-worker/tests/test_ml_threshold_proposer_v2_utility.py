from __future__ import annotations
"""Tests for ML threshold proposer v2 utility (v7: utility + drift-guard)."""

import json
import os
import time
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List

import pytest
import redis

from tools.ml_threshold_proposer_v2_utility import (
    now_ms,
    notify,
    make_bundle_hset,
    write_bundle,
    _f,
    _i,
    filter_rows,
    ece,
    brier,
    selected_stats,
    impact,
    main,
)


def test_now_ms():
    """Test now_ms returns milliseconds."""
    t1 = now_ms()
    time.sleep(0.01)
    t2 = now_ms()
    assert t2 > t1
    assert isinstance(t1, int)
    assert isinstance(t2, int)


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
    
    write_bundle(mock_redis, "test_bid", {"id": "test_bid"}, 3600)
    assert mock_set.call_count == 2
    # Check bundle storage
    bundle_call = [c for c in mock_set.call_args_list if "recs:bundle:" in str(c[0][0])]
    assert len(bundle_call) == 1
    # Check status storage
    status_call = [c for c in mock_set.call_args_list if "recs:status:" in str(c[0][0])]
    assert len(status_call) == 1


def test_f():
    """Test _f converts to float."""
    assert _f("1.5") == 1.5
    assert _f(2.0) == 2.0
    assert _f(None, 0.0) == 0.0
    assert _f("invalid", 1.0) == 1.0


def test_i():
    """Test _i converts to int."""
    assert _i("5") == 5
    assert _i(3.7) == 3
    assert _i(None, 0) == 0
    assert _i("invalid", 1) == 1


def test_filter_rows():
    """Test filter_rows filters by bucket and symbol."""
    rows = [
        {"bucket": "trend", "symbol": "BTCUSDT", "p_edge": 0.6},
        {"bucket": "trend", "symbol": "ETHUSDT", "p_edge": 0.7},
        {"bucket": "range", "symbol": "BTCUSDT", "p_edge": 0.5},
    ]
    filtered = filter_rows(rows, "trend", "BTCUSDT")
    assert len(filtered) == 1
    assert filtered[0]["symbol"] == "BTCUSDT"
    assert filtered[0]["bucket"] == "trend"


def test_ece():
    """Test ECE (Expected Calibration Error) calculation."""
    rows = [
        {"p_edge": 0.5, "y": 0},
        {"p_edge": 0.5, "y": 1},
        {"p_edge": 0.8, "y": 1},
        {"p_edge": 0.8, "y": 1},
    ]
    e = ece(rows, n_bins=10, thr=0.0)
    assert isinstance(e, float)
    assert e >= 0.0
    assert e <= 1.0


def test_brier():
    """Test Brier score calculation."""
    rows = [
        {"p_edge": 0.5, "y": 0},
        {"p_edge": 0.5, "y": 1},
        {"p_edge": 0.8, "y": 1},
    ]
    b = brier(rows, thr=0.0)
    assert isinstance(b, float)
    assert b >= 0.0
    assert b <= 1.0


def test_selected_stats():
    """Test selected_stats computes utility metrics."""
    rows = [
        {"p_edge": 0.6, "r_mult": 0.5, "y": 1},
        {"p_edge": 0.7, "r_mult": -1.5, "y": 0},
        {"p_edge": 0.5, "r_mult": 0.3, "y": 1},
    ]
    stats = selected_stats(rows, thr=0.55)
    assert stats["n"] == 2
    assert "meanR" in stats
    assert "tail_rate" in stats
    assert "es05" in stats
    assert "win_rate" in stats


def test_impact():
    """Test impact calculation for threshold change."""
    rows_confirm = [
        {"bucket": "trend", "symbol": "BTCUSDT", "p_edge": 0.5, "enforce": 1, "ok_rule": 1, "missing": 0},
        {"bucket": "trend", "symbol": "BTCUSDT", "p_edge": 0.6, "enforce": 1, "ok_rule": 1, "missing": 0},
        {"bucket": "trend", "symbol": "BTCUSDT", "p_edge": 0.7, "enforce": 1, "ok_rule": 1, "missing": 0},
    ]
    imp = impact(rows_confirm, "trend", "BTCUSDT", 0.55, 0.65)
    assert "total" in imp
    assert "blocked_old" in imp
    assert "blocked_new" in imp
    assert "delta_block" in imp
    assert imp["total"] == 3


def test_main_health_gate_fails(mocker):
    """Test main exits early if health gate fails."""
    mock_redis = MagicMock()
    mock_hgetall = MagicMock(return_value={})
    mock_get = MagicMock(return_value=None)
    mock_redis.hgetall = mock_hgetall
    mock_redis.get = mock_get
    
    with patch("tools.ml_threshold_proposer_v2_utility.redis.Redis.from_url", return_value=mock_redis):
        with patch("tools.ml_threshold_proposer_v2_utility.read_recent_stream", return_value=[]):
            with patch("tools.ml_threshold_proposer_v2_utility.agg_health_ml_confirm", return_value={"n": 0}):
                main()
                # Should exit early, no bundle created
                assert not hasattr(mock_redis, "set") or not any("recs:bundle:" in str(c) for c in (mock_redis.set.call_args_list if hasattr(mock_redis.set, "call_args_list") else []))


def test_main_no_proposals(mocker):
    """Test main exits if no proposals found."""
    mock_redis = MagicMock()
    mock_hgetall = MagicMock(return_value={})
    mock_get = MagicMock(return_value=None)
    mock_redis.hgetall = mock_hgetall
    mock_redis.get = mock_get
    
    def mock_health_ok():
        return {"n": 200, "missing_rate": 0.01, "err_rate": 0.005, "lat_p99_ms": 5.0}
    
    with patch("tools.ml_threshold_proposer_v2_utility.redis.Redis.from_url", return_value=mock_redis):
        with patch("tools.ml_threshold_proposer_v2_utility.read_recent_stream", return_value=[]):
            with patch("tools.ml_threshold_proposer_v2_utility.agg_health_ml_confirm", return_value=mock_health_ok()):
                main()
                # Should exit early, no bundle created
                assert not hasattr(mock_redis, "set") or not any("recs:bundle:" in str(c) for c in (mock_redis.set.call_args_list if hasattr(mock_redis.set, "call_args_list") else []))

