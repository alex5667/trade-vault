"""Tests for ML decision cache functionality."""

import json
import time
from unittest.mock import Mock, patch

import pytest
import redis

from services.ml_confirm_gate import cache_ml_decision


def test_cache_ml_decision():
    """Test caching ML decision to Redis."""
    mock_r = Mock(spec=redis.Redis)
    
    sid = "crypto-of:BTCUSDT:1234567890"
    symbol = "BTCUSDT"
    bucket = "trend"
    p_edge = 0.65
    enforce = 1
    ok_rule = 1
    missing = 0
    model_ver = "v1.0"
    
    cache_ml_decision(
        mock_r,
        sid=sid,
        symbol=symbol,
        bucket=bucket,
        p_edge=p_edge,
        enforce=enforce,
        ok_rule=ok_rule,
        missing=missing,
        model_ver=model_ver,
    )
    
    # Verify Redis set was called
    assert mock_r.set.called
    
    # Extract arguments
    call_args = mock_r.set.call_args
    key = call_args[0][0]
    value = call_args[0][1]
    ex = call_args[1].get("ex")
    
    assert key == f"ml:dec:{sid}"
    assert ex == 7 * 24 * 3600  # 7 days default TTL
    
    # Verify payload structure
    payload = json.loads(value)
    assert payload["sid"] == sid
    assert payload["symbol"] == symbol.upper()
    assert payload["bucket"] == bucket.lower()
    assert payload["p_edge"] == p_edge
    assert payload["enforce"] == enforce
    assert payload["ok_rule"] == ok_rule
    assert payload["missing"] == missing
    assert payload["model_ver"] == model_ver
    assert "ts_ms" in payload


def test_cache_ml_decision_custom_ttl():
    """Test caching with custom TTL."""
    mock_r = Mock(spec=redis.Redis)
    
    cache_ml_decision(
        mock_r,
        sid="test_sid",
        symbol="BTCUSDT",
        bucket="trend",
        p_edge=0.5,
        enforce=0,
        ok_rule=1,
        missing=0,
        model_ver="v1.0",
        ttl_sec=3600,
    )
    
    call_args = mock_r.set.call_args
    ex = call_args[1].get("ex")
    assert ex == 3600


def test_cache_ml_decision_fail_open():
    """Test that cache failures don't raise exceptions."""
    mock_r = Mock(spec=redis.Redis)
    mock_r.set.side_effect = Exception("Redis error")
    
    # Should not raise
    cache_ml_decision(
        mock_r,
        sid="test_sid",
        symbol="BTCUSDT",
        bucket="trend",
        p_edge=0.5,
        enforce=0,
        ok_rule=1,
        missing=0,
        model_ver="v1.0",
    )

