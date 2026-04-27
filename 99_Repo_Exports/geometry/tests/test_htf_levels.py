"""
Unit tests for geometry/htf_levels.py — HTFLevelsService
Ported and extended from python-worker/test_geometry_service.py
"""
import time
import pytest
from unittest.mock import MagicMock

from geometry.htf_levels import HTFLevelsService
from geometry.structures import Level, LevelType


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)


class _MockBar:
    """Minimal bar object for testing."""

    def __init__(
        self,
        ts_ms: int,
        high: float,
        low: float,
        close: float = 0.0,
        open_: float = 0.0,
        symbol: str = "BTCUSDT",
    ) -> None:
        self.ts_event_ms = ts_ms
        self.high = high
        self.low = low
        self.close = close
        self.open = open_
        self.symbol = symbol


def _make_service() -> HTFLevelsService:
    return HTFLevelsService()


# ──────────────────────────────────────────────
# Basic geometry
# ──────────────────────────────────────────────

class TestInitialGeometry:

    def test_empty_geometry(self) -> None:
        svc = _make_service()
        snap = svc.get_geometry("BTCUSDT", _now_ms(), 50_000.0)
        assert snap.symbol == "BTCUSDT"
        assert snap.levels == []
        assert snap.nearest_level_above is None
        assert snap.nearest_level_below is None
        assert snap.distance_to_nearest_level_bp is None

    def test_get_levels_empty(self) -> None:
        svc = _make_service()
        assert svc.get_levels("BTCUSDT") == []


# ──────────────────────────────────────────────
# Daily / weekly level creation
# ──────────────────────────────────────────────

class TestLevelCreation:

    def test_daily_levels_added_on_daily_close(self) -> None:
        svc = _make_service()
        bar = _MockBar(ts_ms=_now_ms(), high=51_000.0, low=49_000.0, close=50_000.0)
        svc._is_daily_close = lambda b: True
        svc._is_weekly_close = lambda b: False

        svc.on_bar(bar)

        snap = svc.get_geometry("BTCUSDT", bar.ts_event_ms, 50_000.0)
        types = {lv.level_type for lv in snap.levels}
        assert LevelType.DAILY_HIGH in types
        assert LevelType.DAILY_LOW in types

    def test_weekly_levels_added_on_weekly_close(self) -> None:
        svc = _make_service()
        bar = _MockBar(ts_ms=_now_ms(), high=55_000.0, low=45_000.0)
        svc._is_daily_close = lambda b: False
        svc._is_weekly_close = lambda b: True

        svc.on_bar(bar)

        types = {lv.level_type for lv in svc.get_levels("BTCUSDT")}
        assert LevelType.WEEKLY_HIGH in types
        assert LevelType.WEEKLY_LOW in types

    def test_on_bar_unknown_symbol(self) -> None:
        """Bar without symbol attribute defaults to 'unknown' and doesn't crash."""
        svc = _make_service()
        bar = MagicMock(spec=[])       # no symbol attr
        bar.ts_event_ms = _now_ms()
        bar.high = 1.0
        bar.low = 0.5
        bar.open = 0.75
        svc._is_daily_close = lambda b: True
        svc._is_weekly_close = lambda b: False
        svc.on_bar(bar)               # must not raise


# ──────────────────────────────────────────────
# Distance / nearest-level calculations
# ──────────────────────────────────────────────

class TestGeometryDistances:

    def _setup_levels(self, svc: HTFLevelsService) -> int:
        ts = _now_ms()
        svc._levels_by_symbol["BTCUSDT"].extend([
            Level("BTCUSDT", LevelType.DAILY_HIGH, 51_000.0, ts, ts + 86_400_000, 0.8),
            Level("BTCUSDT", LevelType.DAILY_LOW, 49_000.0, ts, ts + 86_400_000, 0.8),
        ])
        return ts

    def test_nearest_above_and_below(self) -> None:
        svc = _make_service()
        ts = self._setup_levels(svc)
        snap = svc.get_geometry("BTCUSDT", ts, 50_000.0)
        assert snap.nearest_level_above is not None
        assert snap.nearest_level_above.price == pytest.approx(51_000.0)
        assert snap.nearest_level_below is not None
        assert snap.nearest_level_below.price == pytest.approx(49_000.0)

    def test_distance_above_in_bps(self) -> None:
        svc = _make_service()
        ts = self._setup_levels(svc)
        snap = svc.get_geometry("BTCUSDT", ts, 50_000.0)
        expected = ((51_000.0 - 50_000.0) / 50_000.0) * 10_000.0  # 200 bps
        assert snap.nearest_resistance_bp == pytest.approx(expected, rel=1e-6)

    def test_distance_below_in_bps(self) -> None:
        svc = _make_service()
        ts = self._setup_levels(svc)
        snap = svc.get_geometry("BTCUSDT", ts, 50_000.0)
        expected = ((50_000.0 - 49_000.0) / 50_000.0) * 10_000.0  # 200 bps
        assert snap.nearest_support_bp == pytest.approx(expected, rel=1e-6)

    def test_distance_to_nearest_is_minimum(self) -> None:
        svc = _make_service()
        ts = _now_ms()
        # Asymmetric: above closer (100 bps), below farther (500 bps)
        svc._levels_by_symbol["BTCUSDT"].extend([
            Level("BTCUSDT", LevelType.RESISTANCE, 50_050.0, ts, ts + 86_400_000, 0.8),
            Level("BTCUSDT", LevelType.SUPPORT, 49_750.0, ts, ts + 86_400_000, 0.8),
        ])
        snap = svc.get_geometry("BTCUSDT", ts, 50_000.0)
        assert snap.distance_to_nearest_level_bp is not None
        assert snap.distance_to_nearest_level_bp == pytest.approx(
            snap.nearest_resistance_bp, rel=1e-6
        )

    def test_price_above_all_levels(self) -> None:
        svc = _make_service()
        ts = _now_ms()
        svc._levels_by_symbol["BTCUSDT"].append(
            Level("BTCUSDT", LevelType.SUPPORT, 49_000.0, ts, ts + 86_400_000, 0.8)
        )
        snap = svc.get_geometry("BTCUSDT", ts, 60_000.0)
        assert snap.nearest_level_above is None
        assert snap.nearest_level_below is not None

    def test_levels_count(self) -> None:
        svc = _make_service()
        ts = self._setup_levels(svc)
        snap = svc.get_geometry("BTCUSDT", ts, 50_000.0)
        assert snap.levels_above_count == 1
        assert snap.levels_below_count == 1


# ──────────────────────────────────────────────
# Session data tracking
# ──────────────────────────────────────────────

class TestSessionTracking:
    # Explicit UTC epoch timestamps for deterministic sessions:
    # 2022-01-01 00:00:00 UTC → Asia
    # 2022-01-01 12:00:00 UTC → Europe
    # 2022-01-01 18:00:00 UTC → US
    _ASIA_TS = 1_640_995_200_000
    _EUR_TS = 1_641_038_400_000
    _US_TS = 1_641_060_000_000

    def test_session_keys_created(self) -> None:
        svc = _make_service()
        for ts_ms in (self._ASIA_TS, self._EUR_TS, self._US_TS):
            svc.on_bar(_MockBar(ts_ms=ts_ms, high=100.0, low=99.0, open_=99.5))
        assert "BTCUSDT_asia" in svc._session_data
        assert "BTCUSDT_europe" in svc._session_data
        assert "BTCUSDT_us" in svc._session_data

    def test_session_high_tracks_max(self) -> None:
        svc = _make_service()
        svc.on_bar(_MockBar(ts_ms=self._ASIA_TS, high=100.0, low=99.0, open_=99.5))
        svc.on_bar(_MockBar(ts_ms=self._ASIA_TS + 60_000, high=105.0, low=99.5, open_=100.0))
        info = svc._session_data["BTCUSDT_asia"]
        assert info["high"] == pytest.approx(105.0)

    def test_session_low_tracks_min(self) -> None:
        svc = _make_service()
        svc.on_bar(_MockBar(ts_ms=self._US_TS, high=200.0, low=180.0, open_=190.0))
        svc.on_bar(_MockBar(ts_ms=self._US_TS + 60_000, high=195.0, low=175.0, open_=185.0))
        info = svc._session_data["BTCUSDT_us"]
        assert info["low"] == pytest.approx(175.0)

    def test_geometry_session_fields(self) -> None:
        svc = _make_service()
        bar = _MockBar(ts_ms=self._EUR_TS, high=3200.0, low=3100.0, open_=3150.0, symbol="ETHUSDT")
        svc.on_bar(bar)
        snap = svc.get_geometry("ETHUSDT", self._EUR_TS + 60_000, 3150.0)
        assert snap.current_session == "europe"
        assert snap.session_open_price == pytest.approx(3150.0)


# ──────────────────────────────────────────────
# Level eviction / limits
# ──────────────────────────────────────────────

class TestLevelLimits:

    def test_max_levels_enforced(self) -> None:
        svc = _make_service()
        svc._cfg["max_levels_per_symbol"] = 5
        ts = _now_ms()
        for i in range(10):
            svc._levels_by_symbol["BTCUSDT"].append(
                Level("BTCUSDT", LevelType.RESISTANCE, 50_000.0 + i, ts + i, ts + 86_400_000, 0.5)
            )
        svc._limit_levels_count("BTCUSDT")
        assert len(svc._levels_by_symbol["BTCUSDT"]) == 5

    def test_expired_levels_removed(self) -> None:
        svc = _make_service()
        ts_past = _now_ms() - 1_000  # already expired
        ts_future = _now_ms() + 86_400_000
        svc._levels_by_symbol["BTCUSDT"].extend([
            Level("BTCUSDT", LevelType.SUPPORT, 49_000.0, ts_past, ts_past, 0.5),  # expired
            Level("BTCUSDT", LevelType.RESISTANCE, 51_000.0, ts_past, ts_future, 0.5),  # valid
        ])
        svc._cleanup_expired_levels("BTCUSDT")
        remaining = svc._levels_by_symbol["BTCUSDT"]
        assert len(remaining) == 1
        assert remaining[0].level_type == LevelType.RESISTANCE


# ──────────────────────────────────────────────
# HTF provider integration
# ──────────────────────────────────────────────

class TestHTFProvider:

    def test_provider_levels_included(self) -> None:
        """Levels from provider appear in get_levels() output."""
        provider = MagicMock()
        htf_obj = MagicMock()
        htf_obj.pdh = 52_000.0
        htf_obj.pdl = 48_000.0
        htf_obj.week_hi = 55_000.0
        htf_obj.week_lo = 45_000.0
        htf_obj.asia_open = 50_500.0
        htf_obj.europe_open = 50_200.0
        provider.get_levels.return_value = htf_obj

        svc = HTFLevelsService(htf_provider=provider)
        levels = svc.get_levels("BTCUSDT", _now_ms())
        prices = {lv.price for lv in levels}
        assert 52_000.0 in prices
        assert 48_000.0 in prices
        assert 55_000.0 in prices

    def test_provider_none_returns_empty(self) -> None:
        provider = MagicMock()
        provider.get_levels.return_value = None
        svc = HTFLevelsService(htf_provider=provider)
        levels = svc.get_levels("BTCUSDT", _now_ms())
        assert levels == []
