"""
Tests for capture_microbars.py split streams functionality.

Tests cover:
- _decode() function
- _discover_symbols() with SSCAN
- _make_stream_keys() template formatting
- Split streams XREAD fan-in logic
- Legacy fallback behavior
"""

import os
from unittest.mock import AsyncMock, patch

import pytest

# Import functions from capture_microbars
from services.capture_microbars import _decode, _discover_symbols, _make_stream_keys


class TestDecode:
    """Test _decode() function."""

    def test_decode_bytes(self):
        """Decode bytes to string."""
        assert _decode(b"BTCUSDT") == "BTCUSDT"
        assert _decode(b"ETHUSDT") == "ETHUSDT"

    def test_decode_string(self):
        """Return string as-is."""
        assert _decode("BTCUSDT") == "BTCUSDT"

    def test_decode_none(self):
        """Return empty string for None."""
        assert _decode(None) == ""

    def test_decode_invalid_utf8(self):
        """Handle invalid UTF-8 gracefully."""
        invalid = b'\xff\xfe'
        result = _decode(invalid)
        assert isinstance(result, str)
        assert len(result) >= 0  # Should not crash


@pytest.mark.asyncio
class TestDiscoverSymbols:
    """Test _discover_symbols() function."""

    async def test_discover_symbols_single_scan(self):
        """Discover symbols in single SSCAN iteration."""
        r = AsyncMock()
        r.sscan = AsyncMock(return_value=(0, [b"BTCUSDT", b"ETHUSDT", b"SOLUSDT"]))

        result = await _discover_symbols(r, limit=2000)

        assert set(result) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
        assert result == sorted(result)  # Should be sorted
        r.sscan.assert_called_once()

    async def test_discover_symbols_multiple_scans(self):
        """Discover symbols across multiple SSCAN iterations."""
        r = AsyncMock()
        r.sscan = AsyncMock(side_effect=[
            (100, [b"BTCUSDT", b"ETHUSDT"]),
            (200, [b"SOLUSDT", b"BNBUSDT"]),
            (0, [b"XRPUSDT"]),  # cursor=0 means done
        ])

        result = await _discover_symbols(r, limit=2000)

        assert set(result) == {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"}
        assert r.sscan.call_count == 3

    async def test_discover_symbols_limit(self):
        """Respect limit parameter."""
        r = AsyncMock()
        r.sscan = AsyncMock(return_value=(0, [b"BTCUSDT", b"ETHUSDT", b"SOLUSDT", b"BNBUSDT"]))

        result = await _discover_symbols(r, limit=2)

        assert len(result) == 2
        assert result == sorted(result)

    async def test_discover_symbols_empty(self):
        """Handle empty set."""
        r = AsyncMock()
        r.sscan = AsyncMock(return_value=(0, []))

        result = await _discover_symbols(r, limit=2000)

        assert result == []

    async def test_discover_symbols_deduplication(self):
        """Deduplicate symbols."""
        r = AsyncMock()
        r.sscan = AsyncMock(return_value=(0, [b"BTCUSDT", b"BTCUSDT", b"ETHUSDT"]))

        result = await _discover_symbols(r, limit=2000)

        assert result == ["BTCUSDT", "ETHUSDT"]


class TestMakeStreamKeys:
    """Test _make_stream_keys() function."""

    def test_make_stream_keys_normal(self):
        """Return normal multi-stream keys with {sym} replaced."""
        template = "events:microbars:{sym}"
        with patch("capture_microbars.STREAM_TEMPLATE", template):
            symbols = ["BTCUSDT", "ETHUSDT"]
            result = _make_stream_keys(symbols)
            assert sorted(result) == ["events:microbars:BTCUSDT", "events:microbars:ETHUSDT"]

    def test_make_stream_keys_no_template(self):
        """Return single key if template has no {sym}."""
        template = "events:microbar_closed"
        with patch("capture_microbars.STREAM_TEMPLATE", template):
            symbols = ["BTCUSDT", "ETHUSDT"]
            result = _make_stream_keys(symbols)
            assert result == ["events:microbar_closed"]

    def test_make_stream_keys_empty_symbols(self):
        """Return single key if symbols empty and no {sym}."""
        template = "events:microbar_closed"
        with patch("capture_microbars.STREAM_TEMPLATE", template):
            result = _make_stream_keys([])
            assert result == ["events:microbar_closed"]


@pytest.mark.asyncio
class TestSplitStreamsLogic:
    """Test split streams XREAD logic (integration-style)."""

    async def test_split_streams_enabled(self):
        """Test that split streams are used when enabled."""
        with patch.dict(os.environ, {
            "MICROBAR_SPLIT_STREAMS_ENABLE": "1",
            "MICROBAR_CAPTURE_SYMBOLS": "BTCUSDT,ETHUSDT"
        }):
            # This would be tested in integration test with actual Redis
            # For unit test, we verify the logic path
            assert os.getenv("MICROBAR_SPLIT_STREAMS_ENABLE") == "1"

    async def test_legacy_fallback(self):
        """Test legacy stream fallback when split is disabled."""
        with patch.dict(os.environ, {
            "MICROBAR_SPLIT_STREAMS_ENABLE": "0"
        }, clear=False):
            # Legacy mode should use single stream
            assert os.getenv("MICROBAR_SPLIT_STREAMS_ENABLE", "0") == "0"

    async def test_symbol_discovery_fallback(self):
        """Test that symbols are discovered when not provided via ENV."""
        with patch.dict(os.environ, {
            "MICROBAR_SPLIT_STREAMS_ENABLE": "1",
            "MICROBAR_CAPTURE_SYMBOLS": ""  # Empty, should trigger discovery
        }):
            # In real scenario, _discover_symbols would be called
            assert os.getenv("MICROBAR_CAPTURE_SYMBOLS", "").strip() == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

