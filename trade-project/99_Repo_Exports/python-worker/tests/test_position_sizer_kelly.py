from __future__ import annotations

"""
Regression: Half-Kelly position sizer — boundary math invariants (merge-blocker).

Tests:
  - Positive expectation → fraction clamped to [min_size, max_size]
  - Negative expectation → min_size
  - Edge-case win rates: 0.0, 1.0, 0.5 with RR=1
  - Low n_trades → min_size (confidence scaling)
  - No DB row → min_size

Run:
    cd python-worker && python -m pytest tests/test_position_sizer_kelly.py -v
"""

import asyncio

import pytest

from services.position_sizer import KellyPositionSizer

# ---------------------------------------------------------------------------
# DB mock
# ---------------------------------------------------------------------------

class MockDbRow(dict):
    pass


class MockDbPool:
    def __init__(self, row: dict = None):
        self._row = row

    async def fetchrow(self, query, *args):
        if self._row is None:
            return None
        return MockDbRow(self._row)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Positive-expectation scenarios
# ---------------------------------------------------------------------------

class TestPositiveExpectation:
    def test_high_wr_high_rr_clamped_to_max(self) -> None:
        """win_rate=0.7, avg_rr=2.0, n=50 → half_kelly > max_size → clamped."""
        pool = MockDbPool({"win_rate": 0.7, "avg_rr": 2.0, "n_trades": 50})
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.10)
        result = _run(sizer.compute("BTCUSDT", "MOMENTUM", 1.0))
        assert result == pytest.approx(0.10)

    def test_moderate_wr_moderate_rr(self) -> None:
        """win_rate=0.55, avg_rr=1.2, n=50 → positive but small kelly."""
        pool = MockDbPool({"win_rate": 0.55, "avg_rr": 1.2, "n_trades": 50})
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.10)
        result = _run(sizer.compute("BTCUSDT", "MOMENTUM", 1.0))
        # Half-kelly fraction should be moderate → either clamped or in-range
        assert 0.01 <= result <= 0.10


# ---------------------------------------------------------------------------
# Negative-expectation scenarios
# ---------------------------------------------------------------------------

class TestNegativeExpectation:
    def test_low_wr_returns_min(self) -> None:
        """win_rate=0.3, avg_rr=1.0 → negative Kelly fraction → min_size."""
        pool = MockDbPool({"win_rate": 0.3, "avg_rr": 1.0, "n_trades": 40})
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.10)
        result = _run(sizer.compute("SOLUSDT", "RANGING", 1.0))
        assert result == pytest.approx(0.01)

    def test_breakeven_expectation(self) -> None:
        """win_rate=0.5, avg_rr=1.0 → kelly = 0 → min_size."""
        pool = MockDbPool({"win_rate": 0.5, "avg_rr": 1.0, "n_trades": 30})
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.10)
        result = _run(sizer.compute("ETHUSDT", "MOMENTUM", 1.0))
        assert result == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# Confidence scaling (low n_trades)
# ---------------------------------------------------------------------------

class TestConfidenceScaling:
    def test_low_n_trades_returns_min(self) -> None:
        """n_trades < 20 → insufficient confidence → min_size."""
        pool = MockDbPool({"win_rate": 0.8, "avg_rr": 3.0, "n_trades": 5})
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.10)
        result = _run(sizer.compute("ETHUSDT", "MOMENTUM", 1.0))
        assert result == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# No DB data
# ---------------------------------------------------------------------------

class TestNoData:
    def test_no_row_returns_min(self) -> None:
        """When DB returns None → min_size (fail-safe)."""
        pool = MockDbPool(row=None)
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.10)
        result = _run(sizer.compute("BTCUSDT", "MOMENTUM", 1.0))
        assert result == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# Output invariants
# ---------------------------------------------------------------------------

class TestOutputInvariants:
    def test_always_within_bounds(self) -> None:
        """Regardless of input, result ∈ [min_size, max_size]."""
        cases = [
            {"win_rate": 0.0, "avg_rr": 0.0, "n_trades": 0},
            {"win_rate": 1.0, "avg_rr": 10.0, "n_trades": 100},
            {"win_rate": 0.5, "avg_rr": 1.0, "n_trades": 30},
            {"win_rate": 0.99, "avg_rr": 5.0, "n_trades": 200},
        ]
        for row in cases:
            pool = MockDbPool(row)
            sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.10)
            result = _run(sizer.compute("BTCUSDT", "MOMENTUM", 1.0))
            assert 0.01 <= result <= 0.10, f"Out of bounds for {row}: {result}"
