"""
Unit tests for geometry/structures.py
"""
import pytest
from geometry.structures import Level, LevelType, GeometrySnapshot


def test_level_default_fields() -> None:
    lv = Level(symbol="BTCUSDT", level_type=LevelType.DAILY_HIGH, price=50000.0, ts_created_ms=1_000_000)
    assert lv.strength == 1.0
    assert lv.ts_valid_until_ms is None
    assert lv.metadata is None


def test_level_with_metadata() -> None:
    meta = {"session": "us", "score": 0.9}
    lv = Level("ETHUSDT", LevelType.SUPPORT, 3000.0, 2_000_000, metadata=meta)
    assert lv.metadata["session"] == "us"


def test_level_type_values() -> None:
    assert LevelType.DAILY_HIGH.value == "daily_high"
    assert LevelType.WEEKLY_LOW.value == "weekly_low"
    assert LevelType.FVG.value == "fvg"


def test_geometry_snapshot_defaults() -> None:
    snap = GeometrySnapshot(symbol="BTCUSDT", ts_event_ms=123_456_789, levels=[])
    assert snap.nearest_level_above is None
    assert snap.nearest_level_below is None
    assert snap.levels_above_count == 0
    assert snap.levels_below_count == 0
    assert snap.current_session is None
