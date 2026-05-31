"""P0 regression tests: enum normalization must not silently map SHORT→LONG."""
import pytest

from common.enums.trading import Direction, Side
from common.normalization import (
    NormalizedSide,
    get_side_int,
    get_side_int_safe,
    normalize_direction,
    normalize_direction_safe,
    normalize_side,
    normalize_side_3,
    normalize_side_safe,
)


class TestNormalizeDirection:
    # --- enum inputs (core bug regression) ---
    def test_enum_long(self):
        assert normalize_direction(Direction.LONG) == Direction.LONG

    def test_enum_short(self):
        # Was silently returning LONG before fix
        assert normalize_direction(Direction.SHORT) == Direction.SHORT

    # --- string inputs ---
    def test_string_long(self):
        assert normalize_direction("LONG") == Direction.LONG

    def test_string_short(self):
        assert normalize_direction("SHORT") == Direction.SHORT

    def test_string_aliases(self):
        for v in ("L", "1", "BUY"):
            assert normalize_direction(v) == Direction.LONG, v
        for v in ("S", "-1", "SELL"):
            assert normalize_direction(v) == Direction.SHORT, v

    def test_lowercase(self):
        assert normalize_direction("long") == Direction.LONG
        assert normalize_direction("short") == Direction.SHORT

    # --- strict: unknown must raise ---
    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            normalize_direction("UNKNOWN")

    def test_none_raises(self):
        with pytest.raises(ValueError):
            normalize_direction(None)

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            normalize_direction("")

    # --- default kwarg ---
    def test_default_long(self):
        assert normalize_direction("???", default=Direction.LONG) == Direction.LONG

    def test_default_short(self):
        assert normalize_direction(None, default=Direction.SHORT) == Direction.SHORT


class TestNormalizeDirectionSafe:
    def test_enum_short_safe(self):
        assert normalize_direction_safe(Direction.SHORT) == Direction.SHORT

    def test_unknown_returns_none(self):
        assert normalize_direction_safe("GARBAGE") is None

    def test_none_returns_none(self):
        assert normalize_direction_safe(None) is None


class TestNormalizeSide:
    def test_enum_buy(self):
        assert normalize_side(Side.BUY) == Side.BUY

    def test_enum_sell(self):
        assert normalize_side(Side.SELL) == Side.SELL

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            normalize_side("UNKNOWN")

    def test_aliases(self):
        for v in ("BUY", "B", "1", "LONG"):
            assert normalize_side(v) == Side.BUY, v
        for v in ("SELL", "S", "-1", "SHORT"):
            assert normalize_side(v) == Side.SELL, v


class TestNormalizeSideSafe:
    def test_sell_safe(self):
        assert normalize_side_safe(Side.SELL) == Side.SELL

    def test_unknown_none(self):
        assert normalize_side_safe("???") is None


class TestNormalizeSide3:
    def test_long(self):
        r = normalize_side_3(Direction.LONG)
        assert r == NormalizedSide(direction=Direction.LONG, side=Side.BUY, side_int=1)

    def test_short(self):
        r = normalize_side_3(Direction.SHORT)
        assert r == NormalizedSide(direction=Direction.SHORT, side=Side.SELL, side_int=-1)

    def test_string_sell(self):
        r = normalize_side_3("SELL")
        assert r.direction == Direction.SHORT
        assert r.side == Side.SELL
        assert r.side_int == -1

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            normalize_side_3("UNKNOWN")


class TestGetSideInt:
    def test_enum_long(self):
        assert get_side_int(Direction.LONG) == 1

    def test_enum_short(self):
        assert get_side_int(Direction.SHORT) == -1

    def test_enum_buy(self):
        assert get_side_int(Side.BUY) == 1

    def test_enum_sell(self):
        assert get_side_int(Side.SELL) == -1

    def test_int_positive(self):
        assert get_side_int(1) == 1

    def test_int_negative(self):
        assert get_side_int(-1) == -1

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            get_side_int(0)

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            get_side_int("UNKNOWN")


class TestGetSideIntSafe:
    def test_short_safe(self):
        assert get_side_int_safe(Direction.SHORT) == -1

    def test_unknown_none(self):
        assert get_side_int_safe("???") is None


class TestNormalizedSideSlots:
    def test_has_slots(self):
        assert hasattr(NormalizedSide, "__slots__")

    def test_no_dict(self):
        ns = NormalizedSide(direction=Direction.LONG, side=Side.BUY, side_int=1)
        assert not hasattr(ns, "__dict__")
