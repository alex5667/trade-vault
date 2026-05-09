"""
Tests for microbar_streams module - split-streams migration helper.

Tests cover:
- pick_stream_key() fallback logic (per-symbol -> legacy)
- read_microbars() with per-symbol and legacy streams
- list_symbols() with symbols set
- _as_payload() JSON parsing
"""

from unittest.mock import AsyncMock

import pytest

from core.microbar_streams import (
    ALT_PER_SYMBOL_PREFIX,
    LEGACY_STREAM,
    PER_SYMBOL_PREFIX,
    SYMBOLS_SET,
    _as_payload,
    list_symbols,
    pick_stream_key,
    read_microbars,
)


class TestAsPayload:
    """Test payload extraction from fields."""

    def test_payload_json_string(self):
        """Extract payload from JSON string field."""
        fields = {"payload": '{"symbol": "BTCUSDT", "close": 50000}'}
        result = _as_payload(fields)
        assert result == {"symbol": "BTCUSDT", "close": 50000}

    def test_flat_fields(self):
        """Use flat fields directly if no payload key."""
        fields = {"symbol": "BTCUSDT", "close": 50000, "ts_ms": 1234567890}
        result = _as_payload(fields)
        assert result == fields

    def test_invalid_json(self):
        """Return empty dict on invalid JSON."""
        fields = {"payload": "not valid json"}
        result = _as_payload(fields)
        assert result == {}

    def test_empty_payload(self):
        """Return empty dict on empty payload."""
        fields = {"payload": ""}
        result = _as_payload(fields)
        assert result == {}

    def test_non_dict(self):
        """Return empty dict for non-dict input."""
        result = _as_payload("not a dict")
        assert result == {}


@pytest.mark.asyncio
class TestPickStreamKey:
    """Test stream key selection with fallback logic."""

    async def test_prefer_per_symbol_stream(self):
        """Prefer per-symbol stream if exists."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=True)

        result = await pick_stream_key(r, "BTCUSDT")

        assert result == f"{PER_SYMBOL_PREFIX}BTCUSDT"
        r.exists.assert_called_once_with(f"{PER_SYMBOL_PREFIX}BTCUSDT")

    async def test_fallback_to_alt_per_symbol(self):
        """Fallback to alt per-symbol prefix if primary doesn't exist."""
        r = AsyncMock()
        r.exists = AsyncMock(side_effect=[False, True])

        result = await pick_stream_key(r, "BTCUSDT")

        assert result == f"{ALT_PER_SYMBOL_PREFIX}BTCUSDT"
        assert r.exists.call_count == 2

    async def test_fallback_to_legacy(self):
        """Fallback to legacy stream if per-symbol streams don't exist."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=False)

        result = await pick_stream_key(r, "BTCUSDT")

        assert result == LEGACY_STREAM
        assert r.exists.call_count == 2

    async def test_exception_handling(self):
        """Handle exceptions gracefully, fallback to legacy."""
        r = AsyncMock()
        r.exists = AsyncMock(side_effect=Exception("Redis error"))

        result = await pick_stream_key(r, "BTCUSDT")

        assert result == LEGACY_STREAM


@pytest.mark.asyncio
class TestListSymbols:
    """Test symbol list retrieval from Redis sets."""

    async def test_prefer_primary_symbols_set(self):
        """Prefer primary symbols set."""
        r = AsyncMock()
        r.smembers = AsyncMock(return_value={"BTCUSDT", "ETHUSDT", "SOLUSDT"})

        result = await list_symbols(r)

        assert set(result) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
        assert result == sorted(result)  # Should be sorted
        r.smembers.assert_called_once_with(SYMBOLS_SET)

    async def test_fallback_to_alt_set(self):
        """Fallback to alt symbols set if primary is empty."""
        r = AsyncMock()
        r.smembers = AsyncMock(side_effect=[
            set(),  # Primary returns empty
            {"BTCUSDT", "ETHUSDT"}  # Alt returns symbols
        ])

        result = await list_symbols(r)

        assert set(result) == {"BTCUSDT", "ETHUSDT"}
        assert r.smembers.call_count == 2

    async def test_fallback_parameter(self):
        """Use fallback list if both sets are empty."""
        r = AsyncMock()
        r.smembers = AsyncMock(return_value=set())

        result = await list_symbols(r, fallback=["BTCUSDT", "ETHUSDT"])

        assert result == ["BTCUSDT", "ETHUSDT"]

    async def test_exception_handling(self):
        """Handle exceptions gracefully."""
        r = AsyncMock()
        r.smembers = AsyncMock(side_effect=Exception("Redis error"))

        result = await list_symbols(r, fallback=["BTCUSDT"])

        assert result == ["BTCUSDT"]


@pytest.mark.asyncio
class TestReadMicrobars:
    """Test reading microbars from streams."""

    async def test_read_per_symbol_stream(self):
        """Read from per-symbol stream (no filtering needed)."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=True)
        r.xrange = AsyncMock(return_value=[
            ("1234567890-0", {"payload": '{"symbol": "BTCUSDT", "close": 50000, "ts_ms": 1234567890}'}),
            ("1234567891-0", {"payload": '{"symbol": "BTCUSDT", "close": 50010, "ts_ms": 1234567891}'}),
        ])

        result = await read_microbars(r, sym="BTCUSDT", count=100)

        assert len(result) == 2
        assert result[0]["symbol"] == "BTCUSDT"
        assert result[0]["close"] == 50000
        r.xrange.assert_called_once()

    async def test_read_legacy_stream_with_filtering(self):
        """Read from legacy stream and filter by symbol."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=False)  # Per-symbol doesn't exist
        r.xrange = AsyncMock(return_value=[
            ("1234567890-0", {"payload": '{"symbol": "BTCUSDT", "close": 50000, "ts_ms": 1234567890}'}),
            ("1234567891-0", {"payload": '{"symbol": "ETHUSDT", "close": 3000, "ts_ms": 1234567891}'}),
            ("1234567892-0", {"payload": '{"symbol": "BTCUSDT", "close": 50010, "ts_ms": 1234567892}'}),
        ])

        result = await read_microbars(r, sym="BTCUSDT", count=100)

        assert len(result) == 2
        assert all(b["symbol"] == "BTCUSDT" for b in result)

    async def test_read_reverse(self):
        """Read in reverse order."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=True)
        r.xrevrange = AsyncMock(return_value=[
            ("1234567891-0", {"payload": '{"symbol": "BTCUSDT", "close": 50010, "ts_ms": 1234567891}'}),
            ("1234567890-0", {"payload": '{"symbol": "BTCUSDT", "close": 50000, "ts_ms": 1234567890}'}),
        ])

        result = await read_microbars(r, sym="BTCUSDT", count=100, reverse=True)

        assert len(result) == 2
        assert result[0]["ts_ms"] == 1234567891
        r.xrevrange.assert_called_once()

    async def test_flat_fields_no_payload(self):
        """Handle flat fields (no payload key)."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=True)
        r.xrange = AsyncMock(return_value=[
            ("1234567890-0", {"symbol": "BTCUSDT", "close": 50000, "ts_ms": 1234567890}),
        ])

        result = await read_microbars(r, sym="BTCUSDT", count=100)

        assert len(result) == 1
        assert result[0]["symbol"] == "BTCUSDT"
        assert result[0]["close"] == 50000

    async def test_exception_handling(self):
        """Handle exceptions gracefully, return empty list."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=True)
        r.xrange = AsyncMock(side_effect=Exception("Redis error"))

        result = await read_microbars(r, sym="BTCUSDT", count=100)

        assert result == []

    async def test_start_end_id_parameters(self):
        """Pass start_id and end_id correctly."""
        r = AsyncMock()
        r.exists = AsyncMock(return_value=True)
        r.xrange = AsyncMock(return_value=[])

        await read_microbars(
            r, sym="BTCUSDT", count=100,
            start_id="1234567890-0",
            end_id="1234567900-999999"
        )

        r.xrange.assert_called_once()
        call_args = r.xrange.call_args
        assert call_args[1]["min"] == "1234567890-0"
        assert call_args[1]["max"] == "1234567900-999999"
        assert call_args[1]["count"] == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
















