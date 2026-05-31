from utils.time_utils import get_ny_time_millis

# -*- coding: utf-8 -*-
"""
Tests for ATR source selector v2 (periodic selector for best ATR source/TF).
"""
import json
import os
from unittest.mock import MagicMock, patch

import pytest
import redis

from core.atr_source_selector_v2 import ATRCandidate, ATRSourceSelector


@pytest.fixture
def mock_redis():
    """Create a mock Redis client."""
    r = MagicMock(spec=redis.Redis)
    r.get = MagicMock(return_value=None)
    r.hgetall = MagicMock(return_value={})
    r.pipeline = MagicMock(return_value=MagicMock())
    return r


@pytest.fixture
def selector(mock_redis):
    """Create ATRSourceSelector with mocked Redis."""
    with patch.dict(os.environ, {
        "ATR_SELECTOR_ENABLE": "1",
        "ATR_SELECTOR_MAX_AGE_MS": "900000",
        "ATR_SELECTOR_HOLD_DOWN_MS": "1800000",
        "ATR_SELECTOR_SWITCH_MARGIN": "0.05",
        "ATR_BPS_MIN_SANITY": "2",
        "ATR_BPS_MAX_SANITY": "800",
        "ATR_JUMP_MAX_REL": "0.8",
        "ATR_SELECTOR_TFS": "1m,5m,15m",
    }):
        return ATRSourceSelector(mock_redis)


def test_selector_disabled():
    """Test that selector returns None when disabled."""
    mock_redis = MagicMock()
    with patch.dict(os.environ, {"ATR_SELECTOR_ENABLE": "0"}):
        sel = ATRSourceSelector(mock_redis)
        result = sel.select("BTCUSDT", px=50000.0)
        assert result is None


def test_selector_zero_price():
    """Test that selector returns None for zero price."""
    mock_redis = MagicMock()
    with patch.dict(os.environ, {"ATR_SELECTOR_ENABLE": "1"}):
        sel = ATRSourceSelector(mock_redis)
        result = sel.select("BTCUSDT", px=0.0)
        assert result is None


def test_selector_hash_candidate(selector, mock_redis):
    """Test reading ATR from hash candidate."""
    now_ms = get_ny_time_millis()
    # hgetall returns dict with bytes keys, but values can be bytes or str
    mock_redis.hgetall.return_value = {
        b"atr": b"50.0",
        b"ts_ms": str(now_ms - 10000).encode(),
    }
    # _read_sel_meta will call get() for cfg:atr_sel_meta:BTCUSDT
    mock_redis.get.return_value = None

    result = selector._read_hash_candidate("BTCUSDT", "1m", 50000.0)

    assert result is not None
    assert result.tf == "1m"
    assert result.src == "ATR_HASH"
    assert result.atr == 50.0
    assert result.age_ms >= 10000  # Allow small time difference


def test_selector_json_candidate(selector, mock_redis):
    """Test reading ATR from JSON candidate."""
    now_ms = get_ny_time_millis()
    data = {"atr": 45.0, "ts_ms": now_ms - 5000}
    # First call: atr:json:BTCUSDT:5m, second call: cfg:atr_sel_meta:BTCUSDT (from _read_sel_meta)
    mock_redis.get.side_effect = [
        json.dumps(data).encode(),  # atr:json:BTCUSDT:5m
        None,  # cfg:atr_sel_meta:BTCUSDT
    ]

    result = selector._read_json_candidate("BTCUSDT", "5m", 50000.0)

    assert result is not None
    assert result.tf == "5m"
    assert result.src == "atr_json"
    assert result.atr == 45.0
    assert result.age_ms >= 5000  # Allow small time difference


def test_selector_string_candidate(selector, mock_redis):
    """Test reading ATR from string candidate."""
    now_ms = get_ny_time_millis()
    # Calls: atr:BTCUSDT:15m, atr:BTCUSDT:15m:ts_ms, cfg:atr_sel_meta:BTCUSDT (from _read_sel_meta)
    mock_redis.get.side_effect = [
        b"40.0",  # atr:BTCUSDT:15m value
        str(now_ms - 3000).encode(),  # atr:BTCUSDT:15m:ts_ms
        None,  # cfg:atr_sel_meta:BTCUSDT (from _read_sel_meta in _score)
    ]

    result = selector._read_string_candidate("BTCUSDT", "15m", 50000.0)

    assert result is not None
    assert result.tf == "15m"
    assert result.src == "atr_string"
    assert result.atr == 40.0
    assert "no_ts_penalty" in result.reason  # Should have penalty for weak metadata


def test_selector_fallback_candidate(selector, mock_redis):
    """Test reading ATR from fallback candidate."""
    now_ms = get_ny_time_millis()
    # Calls: ta:last:atr:BTCUSDT, ta:last:atr_ts_ms:BTCUSDT, cfg:atr_sel_meta:BTCUSDT (from _read_sel_meta)
    mock_redis.get.side_effect = [
        b"35.0",  # ta:last:atr:BTCUSDT
        str(now_ms - 2000).encode(),  # ta:last:atr_ts_ms:BTCUSDT
        None,  # cfg:atr_sel_meta:BTCUSDT (from _read_sel_meta in _score)
    ]

    result = selector._read_fallback_candidate("BTCUSDT", 50000.0)

    assert result is not None
    assert result.src == "ta_last"
    assert result.tf == "na"
    assert result.atr == 35.0
    assert "fallback_penalty" in result.reason  # Should have fallback penalty


def test_selector_scoring_freshness(selector):
    """Test that freshness affects score."""
    now_ms = get_ny_time_millis()

    # Fresh candidate (10 seconds old)
    fresh = selector._score(
        "BTCUSDT", tf="1m", src="ATR_HASH", key="test",
        atr=50.0, ts_ms=now_ms - 10000, age_ms=10000, atr_bps=10.0
    )

    # Stale candidate (1 hour old)
    stale = selector._score(
        "BTCUSDT", tf="1m", src="ATR_HASH", key="test",
        atr=50.0, ts_ms=now_ms - 3600000, age_ms=3600000, atr_bps=10.0
    )

    assert fresh.score > stale.score


def test_selector_scoring_bps_sanity(selector):
    """Test that BPS sanity affects score."""
    now_ms = get_ny_time_millis()

    # Good BPS (50 bps)
    good = selector._score(
        "BTCUSDT", tf="1m", src="ATR_HASH", key="test",
        atr=50.0, ts_ms=now_ms - 10000, age_ms=10000, atr_bps=50.0
    )

    # Bad BPS (1000 bps, too high)
    bad = selector._score(
        "BTCUSDT", tf="1m", src="ATR_HASH", key="test",
        atr=500.0, ts_ms=now_ms - 10000, age_ms=10000, atr_bps=1000.0
    )

    assert good.score > bad.score
    assert "bps_bad" in bad.reason


def test_selector_hysteresis_hold_down(selector, mock_redis):
    """Test that hysteresis keeps previous selection during hold-down."""
    now_ms = get_ny_time_millis()

    # Set previous selection
    prev_meta = {
        "picked_tf": "1m",
        "picked_src": "ATR_HASH",
        "ts_ms": now_ms - 100000,  # Within hold-down period
        "atr_bps": 10.0,
    }
    # Calls: cfg:atr_sel_meta:BTCUSDT (for prev), then ATR:BTCUSDT:1m, ATR:BTCUSDT:5m, ATR:BTCUSDT:15m, etc.
    mock_redis.get.side_effect = [
        json.dumps(prev_meta).encode(),  # cfg:atr_sel_meta:BTCUSDT
    ] + [None] * 30  # For other candidate reads

    # Mock current candidates - hgetall for ATR hash
    mock_redis.hgetall.return_value = {
        b"atr": b"50.0",
        b"ts_ms": str(now_ms - 5000).encode(),
    }

    result = selector.select("BTCUSDT", px=50000.0)

    # Should prefer previous if it's still acceptable
    assert result is not None


def test_selector_persist_choice(selector, mock_redis):
    """Test that selection is persisted to Redis."""
    now_ms = get_ny_time_millis()
    candidate = ATRCandidate(
        tf="1m", src="ATR_HASH", key="test:key",
        atr=50.0, ts_ms=now_ms - 10000, age_ms=10000,
        atr_bps=10.0, score=0.8, reason="test"
    )

    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.get.return_value = None  # No previous meta

    selector._persist_choice("BTCUSDT", candidate)

    # Should have called pipeline methods
    assert mock_pipe.set.call_count == 3
    mock_pipe.execute.assert_called_once()


def test_selector_persist_choice_switch_tracking(selector, mock_redis):
    """Test that switch tracking is recorded when TF or source changes."""
    import json
    now_ms = get_ny_time_millis()

    # Previous selection: 1m, ATR_HASH
    prev_meta = {
        "picked_tf": "1m",
        "picked_src": "ATR_HASH",
        "ts_ms": now_ms - 10000,
    }
    mock_redis.get.return_value = json.dumps(prev_meta).encode()

    # New selection: 5m, atr_json (different TF and source)
    candidate = ATRCandidate(
        tf="5m", src="atr_json", key="test:key",
        atr=50.0, ts_ms=now_ms - 5000, age_ms=5000,
        atr_bps=10.0, score=0.9, reason="test"
    )

    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    with patch.dict(os.environ, {
        "ATR_SWITCH_WINDOW_SEC": "3600",
        "ATR_SWITCH_SYMBOLS_SET_TTL_SEC": "86400",
    }):
        selector._persist_choice("BTCUSDT", candidate)

    # Should have called pipeline methods: 3 sets + 1 incr + 2 expires + 1 sadd
    assert mock_pipe.set.call_count == 3
    mock_pipe.incr.assert_called_once()
    assert mock_pipe.expire.call_count >= 2
    mock_pipe.sadd.assert_called_once()
    mock_pipe.execute.assert_called_once()


def test_selector_persist_choice_no_switch(selector, mock_redis):
    """Test that switch tracking is not recorded when TF and source don't change."""
    import json
    now_ms = get_ny_time_millis()

    # Previous selection: 1m, ATR_HASH
    prev_meta = {
        "picked_tf": "1m",
        "picked_src": "ATR_HASH",
        "ts_ms": now_ms - 10000,
    }
    mock_redis.get.return_value = json.dumps(prev_meta).encode()

    # Same selection: 1m, ATR_HASH (no switch)
    candidate = ATRCandidate(
        tf="1m", src="ATR_HASH", key="test:key",
        atr=50.0, ts_ms=now_ms - 5000, age_ms=5000,
        atr_bps=10.0, score=0.9, reason="test"
    )

    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    selector._persist_choice("BTCUSDT", candidate)

    # Should have called pipeline methods: 3 sets only (no switch tracking)
    assert mock_pipe.set.call_count == 3
    mock_pipe.incr.assert_not_called()
    mock_pipe.sadd.assert_not_called()
    mock_pipe.execute.assert_called_once()


def test_selector_no_candidates(selector, mock_redis):
    """Test that selector returns None when no candidates available."""
    mock_redis.hgetall.return_value = {}
    mock_redis.get.return_value = None

    result = selector.select("BTCUSDT", px=50000.0)

    assert result is None

