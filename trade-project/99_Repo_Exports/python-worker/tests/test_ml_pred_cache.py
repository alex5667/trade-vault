"""Tests for ml_pred_cache module."""

import json
from unittest.mock import Mock, patch

from services.ml_pred_cache import _pred_key, cache_pred, get_pred


def test_pred_key():
    """Test prediction key generation."""
    assert _pred_key("test_sid_123") == "ml:pred:test_sid_123"


def test_cache_and_get_pred():
    """Test caching and retrieving predictions."""
    r = Mock()
    r.set = Mock()
    r.get = Mock(return_value=None)

    sid = "test_sid_123"
    payload = {
        "sid": sid,
        "ts_ms": 1234567890,
        "symbol": "BTCUSDT",
        "scenario_v4": "reversal",
        "p_edge": 0.75,
        "p_edge_chal": 0.72,
        "model_ver": "v1.0",
        "chal_ver": "v1.1",
        "enforce": 1,
        "mode": "ENFORCE",
    }

    # Test cache_pred
    cache_pred(r, sid=sid, payload=payload, ttl_sec=3600)
    r.set.assert_called_once()
    call_args = r.set.call_args
    assert call_args[0][0] == _pred_key(sid)
    cached_payload = json.loads(call_args[0][1])
    assert cached_payload == payload
    assert call_args[1]["ex"] == 3600

    # Test get_pred (found)
    r.get.return_value = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    result = get_pred(r, sid)
    assert result == payload
    r.get.assert_called_with(_pred_key(sid))

    # Test get_pred (not found)
    r.get.return_value = None
    result = get_pred(r, sid)
    assert result is None

    # Test get_pred (invalid JSON)
    r.get.return_value = "invalid json"
    result = get_pred(r, sid)
    assert result is None


def test_cache_pred_default_ttl():
    """Test cache_pred uses default TTL from env."""
    r = Mock()
    r.set = Mock()

    with patch.dict("os.environ", {"ML_PRED_TTL_SEC": "7200"}):
        cache_pred(r, sid="test", payload={"sid": "test"})
        call_args = r.set.call_args
        assert call_args[1]["ex"] == 7200


def test_cache_pred_custom_ttl():
    """Test cache_pred uses custom TTL when provided."""
    r = Mock()
    r.set = Mock()

    cache_pred(r, sid="test", payload={"sid": "test"}, ttl_sec=1800)
    call_args = r.set.call_args
    assert call_args[1]["ex"] == 1800

