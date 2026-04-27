"""Tests for signal_quality_gating/core/of_evidence.py

Tests cover: compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags.
"""

from __future__ import annotations

import sys
import os

import pytest

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from core.of_evidence import (
    compute_sweep_recent,
    compute_reclaim_recent,
    compute_absorption_flags,
)


# ---------------------------------------------------------------------------
# compute_sweep_recent
# ---------------------------------------------------------------------------

class TestComputeSweepRecent:
    def _ind(self) -> dict:
        return {}

    def test_none_sweep_returns_false(self) -> None:
        ind = self._ind()
        assert compute_sweep_recent(
            now_ts_ms=1000,
            last_sweep=None,
            cfg={},
            indicators=ind,
        ) is False
        assert ind.get("sweep_age_ms") == -1

    def test_fresh_sweep_returns_true(self) -> None:
        now = 100_000
        sweep = {"ts_ms": now - 5_000, "kind": "EQH", "direction_bias": "SHORT"}
        ind = self._ind()
        result = compute_sweep_recent(
            now_ts_ms=now,
            last_sweep=sweep,
            cfg={"sweep_valid_ms": 120_000},
            indicators=ind,
        )
        assert result is True
        assert ind["sweep_age_ms"] == 5_000
        assert ind["sweep_kind"] == "EQH"
        assert ind["sweep_dir_bias"] == "SHORT"

    def test_stale_sweep_returns_false(self) -> None:
        now = 200_000
        sweep = {"ts_ms": 0, "kind": "EQL"}  # ts=0 -> age = big
        ind = self._ind()
        result = compute_sweep_recent(
            now_ts_ms=now,
            last_sweep=sweep,
            cfg={"sweep_valid_ms": 120_000},
            indicators=ind,
        )
        assert result is False

    def test_sweep_at_boundary_valid_ms(self) -> None:
        # Sweep at exactly valid_ms age should pass (age == valid_ms)
        now = 120_000
        # ts_ms > 0 is required; ts_ms=0 is treated as "unknown" → stale
        sweep = {"ts_ms": 0}  # ts=0 → age=10**9 → stale
        ind = self._ind()
        result = compute_sweep_recent(
            now_ts_ms=now,
            last_sweep=sweep,
            cfg={"sweep_valid_ms": 120_000},
            indicators=ind,
        )
        # ts_ms=0 is treated as stale (age defaults to 10**9), so False
        assert result is False

    def test_sweep_exactly_at_valid_boundary(self) -> None:
        # Sweep with ts exactly at now-valid_ms (age = valid_ms) → should pass
        valid_ms = 120_000
        now = 200_000
        ts = now - valid_ms  # age exactly = valid_ms = boundary
        sweep = {"ts_ms": ts}
        ind = self._ind()
        result = compute_sweep_recent(
            now_ts_ms=now,
            last_sweep=sweep,
            cfg={"sweep_valid_ms": valid_ms},
            indicators=ind,
        )
        assert result is True
        assert ind["sweep_age_ms"] == valid_ms


# ---------------------------------------------------------------------------
# compute_reclaim_recent
# ---------------------------------------------------------------------------

class TestComputeReclaimRecent:
    def _ind(self) -> dict:
        return {}

    def test_none_reclaim_returns_false(self) -> None:
        ind = self._ind()
        ok, bars = compute_reclaim_recent(
            direction="LONG",
            now_ts_ms=1000,
            last_reclaim=None,
            cfg={},
            indicators=ind,
        )
        assert ok is False
        assert bars == 0
        assert ind["reclaim_age_ms"] == -1

    def test_fresh_matching_direction(self) -> None:
        now = 100_000
        reclaim = {"ts_ms": now - 1000, "direction_bias": "LONG", "hold_bars": 3}
        ind = self._ind()
        ok, bars = compute_reclaim_recent(
            direction="LONG",
            now_ts_ms=now,
            last_reclaim=reclaim,
            cfg={"reclaim_signal_valid_ms": 120_000},
            indicators=ind,
        )
        assert ok is True
        assert bars == 3

    def test_direction_mismatch_returns_false(self) -> None:
        now = 100_000
        reclaim = {"ts_ms": now - 1000, "direction_bias": "SHORT"}
        ind = self._ind()
        ok, bars = compute_reclaim_recent(
            direction="LONG",
            now_ts_ms=now,
            last_reclaim=reclaim,
            cfg={"reclaim_signal_valid_ms": 120_000},
            indicators=ind,
        )
        assert ok is False

    def test_empty_bias_does_not_block(self) -> None:
        now = 100_000
        reclaim = {"ts_ms": now - 1000, "direction_bias": ""}  # empty bias = no filter
        ind = self._ind()
        ok, bars = compute_reclaim_recent(
            direction="LONG",
            now_ts_ms=now,
            last_reclaim=reclaim,
            cfg={"reclaim_signal_valid_ms": 120_000, "reclaim_hold_bars": 2},
            indicators=ind,
        )
        assert ok is True

    def test_hold_bars_fallback_to_cfg(self) -> None:
        now = 100_000
        reclaim = {"ts_ms": now - 1000, "direction_bias": "SHORT", "hold_bars": 0}
        ind = self._ind()
        ok, bars = compute_reclaim_recent(
            direction="SHORT",
            now_ts_ms=now,
            last_reclaim=reclaim,
            cfg={"reclaim_signal_valid_ms": 120_000, "reclaim_hold_bars": 4},
            indicators=ind,
        )
        assert ok is True
        assert bars == 4


# ---------------------------------------------------------------------------
# compute_absorption_flags
# ---------------------------------------------------------------------------

class TestComputeAbsorptionFlags:
    def _ind(self) -> dict:
        return {}

    def test_none_absorption_returns_false(self) -> None:
        ind = self._ind()
        ok, vol = compute_absorption_flags(
            direction="LONG",
            absorption=None,
            cfg={},
            indicators=ind,
        )
        assert ok is False
        assert vol == 0.0

    def test_no_side_returns_false(self) -> None:
        ind = self._ind()
        ok, vol = compute_absorption_flags(
            direction="LONG",
            absorption={"volume": 100.0},
            cfg={},
            indicators=ind,
        )
        assert ok is False

    def test_matching_direction_long(self) -> None:
        ind = self._ind()
        ok, vol = compute_absorption_flags(
            direction="LONG",
            absorption={"side": "LONG", "volume": 50.0},
            cfg={"absorption_min_volume": 10.0},
            indicators=ind,
        )
        assert ok is True
        assert vol == pytest.approx(50.0)

    def test_short_side_blocked_for_long(self) -> None:
        ind = self._ind()
        ok, vol = compute_absorption_flags(
            direction="LONG",
            absorption={"side": "SHORT", "volume": 50.0},
            cfg={},
            indicators=ind,
        )
        assert ok is False

    def test_bid_side_normalizes_to_long(self) -> None:
        ind = self._ind()
        ok, vol = compute_absorption_flags(
            direction="LONG",
            absorption={"side": "BID", "volume": 100.0},
            cfg={},
            indicators=ind,
        )
        assert ok is True

    def test_below_min_volume_returns_false(self) -> None:
        ind = self._ind()
        ok, vol = compute_absorption_flags(
            direction="LONG",
            absorption={"side": "LONG", "volume": 5.0},
            cfg={"absorption_min_volume": 10.0},
            indicators=ind,
        )
        assert ok is False
        assert vol == pytest.approx(5.0)
