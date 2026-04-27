"""Tests for observability alerts worker."""

import os
from unittest.mock import Mock, patch

import pytest

from services.observability.alerts_worker import _b2s, _cooldown_ok, _read_set


def test_b2s_bytes():
    """Test _b2s converts bytes to string."""
    assert _b2s(b"test") == "test"
    assert _b2s("test") == "test"


def test_read_set():
    """Test _read_set reads Redis set and converts to list of strings."""
    mock_r = Mock()
    mock_r.smembers.return_value = {b"ETHUSDT", b"BTCUSDT"}
    result = _read_set(mock_r, "test:set", max_n=10)
    assert result == ["BTCUSDT", "ETHUSDT"]  # sorted
    mock_r.smembers.assert_called_once_with("test:set")


def test_read_set_with_max_n():
    """Test _read_set respects max_n limit."""
    mock_r = Mock()
    mock_r.smembers.return_value = {b"ETHUSDT", b"BTCUSDT", b"SOLUSDT"}
    result = _read_set(mock_r, "test:set", max_n=2)
    assert len(result) == 2


def test_cooldown_ok_first_call():
    """Test _cooldown_ok returns True on first call."""
    mock_r = Mock()
    mock_r.get.return_value = None
    result = _cooldown_ok(mock_r, "test:key", 900)
    assert result is True
    mock_r.get.assert_called_once_with("test:key")
    mock_r.set.assert_called_once_with("test:key", "1", ex=900)


def test_cooldown_ok_second_call():
    """Test _cooldown_ok returns False on second call (within cooldown)."""
    mock_r = Mock()
    mock_r.get.return_value = b"1"
    result = _cooldown_ok(mock_r, "test:key", 900)
    assert result is False
    mock_r.get.assert_called_once_with("test:key")
    mock_r.set.assert_not_called()

