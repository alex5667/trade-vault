#!/usr/bin/env python3
"""Tests for tools/of_gate_missing_leg_report.py"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from tools.of_gate_missing_leg_report import _decode, _parse_missing_legs, main_async


def test_decode():
    """Test _decode function"""
    assert _decode("test") == "test"
    assert _decode(b"test") == "test"
    assert _decode(None) == ""
    assert _decode(b"\xff\xfe") == ""  # invalid UTF-8


def test_parse_missing_legs():
    """Test parsing missing legs from payload"""
    # Test with miss_leg field
    payload1 = {"miss_leg": "A"}
    assert _parse_missing_legs(payload1) == ["A"]

    # Test with missing_legs JSON
    payload2 = {"missing_legs": '["A", "B", "C"]'}
    legs = _parse_missing_legs(payload2)
    assert len(legs) == 3
    assert "A" in legs
    assert "B" in legs
    assert "C" in legs

    # Test with empty missing_legs
    payload3 = {"missing_legs": "[]"}
    assert _parse_missing_legs(payload3) == []

    # Test with invalid JSON
    payload4 = {"missing_legs": "invalid"}
    assert _parse_missing_legs(payload4) == []

    # Test with no fields
    payload5 = {}
    assert _parse_missing_legs(payload5) == []

    # Test with bytes
    payload6 = {"missing_legs": b'["A"]'}
    assert _parse_missing_legs(payload6) == ["A"]


@pytest.mark.asyncio
async def test_main_async_basic():
    """Test main_async with basic Redis mock"""
    # Create mock entries
    mock_entries = [
        (
            b"123-0",
            {
                b"ok": b"0",
                b"miss_leg": b"A",
                b"symbol": b"BTCUSDT",
            },
        ),
        (
            b"124-0",
            {
                b"ok": b"0",
                b"miss_leg": b"B",
                b"symbol": b"ETHUSDT",
            },
        ),
        (
            b"125-0",
            {
                b"ok": b"1",  # This should be skipped with --only-veto
                b"miss_leg": b"C",
                b"symbol": b"SOLUSDT",
            },
        ),
        (
            b"126-0",
            {
                b"ok": b"0",
                b"missing_legs": b'["A", "B"]',  # Test JSON parsing
                b"symbol": b"XRPUSDT",
            },
        ),
    ]

    with patch("tools.of_gate_missing_leg_report.aioredis") as mock_redis:
        mock_client = AsyncMock()
        mock_redis.from_url.return_value = mock_client
        mock_client.xrevrange = AsyncMock(return_value=mock_entries)

        import sys
        from io import StringIO

        old_argv = sys.argv
        old_stdout = sys.stdout
        try:
            sys.argv = [
                "of_gate_missing_leg_report.py",
                "--redis-url",
                "redis://localhost:6379/0",
                "--limit",
                "10",
                "--only-veto",
                "--top",
                "5",
            ]
            sys.stdout = StringIO()

            await main_async()

            output = sys.stdout.getvalue()
            result = json.loads(output)

            assert result["scanned"] == 4
            assert result["scanned_effective"] == 3  # Only veto entries (ok=0)
            assert result["only_veto"] is True
            assert len(result["top"]) > 0

            # Verify A appears (from both miss_leg and missing_legs)
            leg_counts = {item["leg"]: item["count"] for item in result["top"]}
            assert "A" in leg_counts
            assert leg_counts["A"] >= 2  # At least 2 occurrences

        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            await mock_client.close()


@pytest.mark.asyncio
async def test_main_async_no_veto_filter():
    """Test main_async without --only-veto filter"""
    mock_entries = [
        (
            b"123-0",
            {
                b"ok": b"0",
                b"miss_leg": b"A",
            },
        ),
        (
            b"124-0",
            {
                b"ok": b"1",
                b"miss_leg": b"B",
            },
        ),
    ]

    with patch("tools.of_gate_missing_leg_report.aioredis") as mock_redis:
        mock_client = AsyncMock()
        mock_redis.from_url.return_value = mock_client
        mock_client.xrevrange = AsyncMock(return_value=mock_entries)

        import sys
        from io import StringIO

        old_argv = sys.argv
        old_stdout = sys.stdout
        try:
            sys.argv = [
                "of_gate_missing_leg_report.py",
                "--limit",
                "10",
                "--top",
                "5",
            ]
            sys.stdout = StringIO()

            await main_async()

            output = sys.stdout.getvalue()
            result = json.loads(output)

            assert result["scanned"] == 2
            assert result["scanned_effective"] == 2  # All entries
            assert result["only_veto"] is False

        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            await mock_client.close()


@pytest.mark.asyncio
async def test_main_async_redis_error():
    """Test main_async with Redis error"""
    with patch("tools.of_gate_missing_leg_report.aioredis") as mock_redis:
        mock_client = AsyncMock()
        mock_redis.from_url.return_value = mock_client
        mock_client.xrevrange = AsyncMock(side_effect=Exception("Connection error"))

        import sys

        old_argv = sys.argv
        try:
            sys.argv = ["of_gate_missing_leg_report.py", "--limit", "10"]
            with pytest.raises(SystemExit) as exc_info:
                await main_async()
            assert "xrevrange_failed" in str(exc_info.value)
        finally:
            sys.argv = old_argv


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

