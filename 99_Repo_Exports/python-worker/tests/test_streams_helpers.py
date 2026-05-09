"""Tests for core.streams helper functions."""

import os
from unittest.mock import Mock, patch

from core.streams import (
    list_microbar_symbols,
    microbar_legacy_stream,
    microbar_majors_stream,
    microbar_per_symbol_prefix,
    microbar_stream_for_symbol,
    microbar_symbols_set,
)


def test_microbar_stream_for_symbol():
    """Test that microbar_stream_for_symbol generates correct stream key."""
    k = microbar_stream_for_symbol("ETHUSDT")
    assert k.endswith("ETHUSDT")
    assert k.startswith("events:microbar_closed:")


def test_microbar_legacy_stream():
    """Test legacy stream name."""
    assert microbar_legacy_stream() == "events:microbar_closed"
    with patch.dict(os.environ, {"MICROBAR_LEGACY_STREAM": "custom:legacy"}):
        assert microbar_legacy_stream() == "custom:legacy"


def test_microbar_per_symbol_prefix():
    """Test per-symbol prefix."""
    assert microbar_per_symbol_prefix() == "events:microbar_closed:"
    with patch.dict(os.environ, {"MICROBAR_PER_SYMBOL_PREFIX": "custom:prefix:"}):
        assert microbar_per_symbol_prefix() == "custom:prefix:"


def test_microbar_majors_stream():
    """Test majors stream name."""
    assert microbar_majors_stream() == "events:microbar_closed:majors"
    with patch.dict(os.environ, {"MICROBAR_MAJORS_STREAM": "custom:majors"}):
        assert microbar_majors_stream() == "custom:majors"


def test_microbar_symbols_set():
    """Test symbols set name."""
    assert microbar_symbols_set() == "events:microbar_closed:symbols"
    with patch.dict(os.environ, {"MICROBAR_SYMBOLS_SET": "custom:symbols"}):
        assert microbar_symbols_set() == "custom:symbols"


def test_list_microbar_symbols():
    """Test list_microbar_symbols returns sorted symbols."""
    mock_r = Mock()
    mock_r.smembers.return_value = {b"ETHUSDT", b"BTCUSDT", b"SOLUSDT"}
    result = list_microbar_symbols(mock_r, max_n=1000)
    assert result == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    mock_r.smembers.assert_called_once_with("events:microbar_closed:symbols")


def test_list_microbar_symbols_with_max_n():
    """Test list_microbar_symbols respects max_n limit."""
    mock_r = Mock()
    mock_r.smembers.return_value = {b"ETHUSDT", b"BTCUSDT", b"SOLUSDT", b"ADAUSDT"}
    result = list_microbar_symbols(mock_r, max_n=2)
    assert len(result) == 2
    assert result == ["BTCUSDT", "ETHUSDT"]


def test_list_microbar_symbols_empty():
    """Test list_microbar_symbols handles empty set."""
    mock_r = Mock()
    mock_r.smembers.return_value = set()
    result = list_microbar_symbols(mock_r)
    assert result == []


def test_list_microbar_symbols_handles_bytes():
    """Test list_microbar_symbols handles bytes and strings."""
    mock_r = Mock()
    mock_r.smembers.return_value = {b"ETHUSDT", "BTCUSDT"}
    result = list_microbar_symbols(mock_r)
    assert "ETHUSDT" in result
    assert "BTCUSDT" in result

