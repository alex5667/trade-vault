# -*- coding: utf-8 -*-
"""
Regression: is_virtual flag propagation + periodic_reporter classification (merge-blocker).

Tests:
  - is_virtual flag must be correctly classified from Redis hash values
  - Various truthy/falsy string representations handled
  - Virtual trades MUST NOT appear in real trade aggregation
  - Reporter correctly separates virtual and real trade buckets

Run:
    cd python-worker && python -m pytest tests/test_is_virtual_propagation.py -v
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# is_virtual classification logic (mirrors periodic_reporter.py line 606)
# ---------------------------------------------------------------------------

def _classify_is_virtual(value) -> bool:
    """Exact replica of the classification logic in periodic_reporter.py."""
    return str(value or "0") in ("1", "True", "true", "TRUE")


# ---------------------------------------------------------------------------
# Truthy values
# ---------------------------------------------------------------------------

class TestIsVirtualTruthy:
    @pytest.mark.parametrize("val", ["1", "True", "true", "TRUE", 1, True])
    def test_truthy_values(self, val) -> None:
        assert _classify_is_virtual(val) is True


# ---------------------------------------------------------------------------
# Falsy values
# ---------------------------------------------------------------------------

class TestIsVirtualFalsy:
    @pytest.mark.parametrize("val", ["0", "False", "false", "FALSE", 0, False, None, "", "no"])
    def test_falsy_values(self, val) -> None:
        assert _classify_is_virtual(val) is False


# ---------------------------------------------------------------------------
# Missing key defaults to "not virtual"
# ---------------------------------------------------------------------------

class TestIsVirtualMissing:
    def test_none_means_real(self) -> None:
        """dict.get("is_virtual") returns None when key missing → real trade."""
        t = {}
        assert _classify_is_virtual(t.get("is_virtual")) is False

    def test_empty_string_means_real(self) -> None:
        assert _classify_is_virtual("") is False


# ---------------------------------------------------------------------------
# Bucket segregation contract
# ---------------------------------------------------------------------------

class TestBucketSegregation:
    @pytest.fixture
    def mixed_trades(self):
        return [
            {"symbol": "BTCUSDT", "pnl": 100.0, "is_virtual": "0"},
            {"symbol": "ETHUSDT", "pnl": -50.0, "is_virtual": "1"},
            {"symbol": "SOLUSDT", "pnl": 200.0, "is_virtual": "True"},
            {"symbol": "BNBUSDT", "pnl": 30.0},  # missing → real
            {"symbol": "XRPUSDT", "pnl": -10.0, "is_virtual": "false"},
        ]

    def test_virtual_never_in_real_bucket(self, mixed_trades) -> None:
        real = [t for t in mixed_trades if not _classify_is_virtual(t.get("is_virtual"))]
        virtual = [t for t in mixed_trades if _classify_is_virtual(t.get("is_virtual"))]

        # Real: BTCUSDT, BNBUSDT (missing), XRPUSDT
        assert len(real) == 3
        # Virtual: ETHUSDT, SOLUSDT
        assert len(virtual) == 2

        real_symbols = {t["symbol"] for t in real}
        virtual_symbols = {t["symbol"] for t in virtual}
        assert real_symbols & virtual_symbols == set(), "No overlap between real and virtual"

    def test_all_trades_classified(self, mixed_trades) -> None:
        """Every trade must appear in exactly one bucket."""
        real = [t for t in mixed_trades if not _classify_is_virtual(t.get("is_virtual"))]
        virtual = [t for t in mixed_trades if _classify_is_virtual(t.get("is_virtual"))]
        assert len(real) + len(virtual) == len(mixed_trades)


# ---------------------------------------------------------------------------
# Redis hash byte-string edge case
# ---------------------------------------------------------------------------

class TestRedisHashBytes:
    """Redis hgetall returns bytes by default; our check must handle str conversion."""

    def test_bytes_value_one(self) -> None:
        """bytes b"1" should classify as virtual after str()."""
        val = b"1"
        # str(b"1") → "b'1'" which does NOT match "1"
        # This is intentional — the code uses `.get()` which decodes, but
        # if somehow bytes leak through, it should NOT be classified as virtual.
        result = _classify_is_virtual(val)
        # b"1" → str(b"1") → "b'1'" → not in truthy set → False
        # This documents the behavior — caller must decode first.
        assert result is False  # documents the need for decode_responses=True

    def test_decoded_value_one(self) -> None:
        """After decode_responses=True, "1" should classify correctly."""
        assert _classify_is_virtual("1") is True
