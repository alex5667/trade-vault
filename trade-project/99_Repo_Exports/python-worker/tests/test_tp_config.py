"""Tests for tp_config: compute_tp_qtys and compute_even_split_tp_qtys.

Run:
    cd python-worker && python -m pytest tests/test_tp_config.py -v
"""
from __future__ import annotations

import pytest

from services.tp_config import compute_even_split_tp_qtys, compute_tp_qtys, parse_tp_ratio


# ---------------------------------------------------------------------------
# parse_tp_ratio
# ---------------------------------------------------------------------------

class TestParseTpRatio:
    def test_default(self) -> None:
        result = parse_tp_ratio()
        assert result == [0.50, 0.30, 0.20]

    def test_comma_string(self) -> None:
        assert parse_tp_ratio("0.4,0.3,0.3") == pytest.approx([0.4, 0.3, 0.3])

    def test_percent_mode(self) -> None:
        assert parse_tp_ratio("50,30,20") == pytest.approx([0.5, 0.3, 0.2])

    def test_two_values(self) -> None:
        result = parse_tp_ratio("0.8,0.2")
        assert len(result) >= 2
        assert result[0] == pytest.approx(0.8)

    def test_invalid(self) -> None:
        result = parse_tp_ratio("abc,def")
        assert result == [0.50, 0.30, 0.20]


# ---------------------------------------------------------------------------
# compute_tp_qtys
# ---------------------------------------------------------------------------

class TestComputeTpQtys:
    def test_two_tp_equal_split(self) -> None:
        result = compute_tp_qtys(1.0, (0.50, 0.50))
        assert len(result) == 2
        assert sum(result) == pytest.approx(1.0)
        assert result[0] == pytest.approx(0.5)
        assert result[1] == pytest.approx(0.5)

    def test_three_tp_custom_ratios(self) -> None:
        result = compute_tp_qtys(10.0, (0.40, 0.30, 0.30))
        assert len(result) == 3
        assert sum(result) == pytest.approx(10.0)
        assert result[0] == pytest.approx(4.0)
        assert result[1] == pytest.approx(3.0)
        assert result[2] == pytest.approx(3.0)

    def test_range_80_20(self) -> None:
        result = compute_tp_qtys(100.0, (0.80, 0.20))
        assert result[0] == pytest.approx(80.0)
        assert result[1] == pytest.approx(20.0)

    def test_with_step_size(self) -> None:
        # qty=1.0, ratios=(0.5,0.5), step=0.01
        result = compute_tp_qtys(1.0, (0.50, 0.50), step_size=0.01)
        assert len(result) == 2
        assert sum(result) == pytest.approx(1.0)
        # Each should be quantised to 0.01 increments
        for q in result:
            assert round(q / 0.01) * 0.01 == pytest.approx(q, abs=1e-10)

    def test_step_size_remainder_goes_to_last(self) -> None:
        # qty=0.123, ratios=(0.5,0.5), step=0.01
        # First TP: floor(0.0615/0.01)*0.01 = 0.06
        # Last TP: 0.123 - 0.06 = 0.063
        result = compute_tp_qtys(0.123, (0.50, 0.50), step_size=0.01)
        assert sum(result) == pytest.approx(0.123)

    def test_normalises_non_unit_sum(self) -> None:
        # ratios don't sum to 1.0 — should be normalised
        result = compute_tp_qtys(10.0, (2.0, 1.0))
        assert sum(result) == pytest.approx(10.0)
        # 2/(2+1) = 0.667 → 6.67
        assert result[0] == pytest.approx(10.0 * 2.0 / 3.0)

    def test_empty_ratios(self) -> None:
        assert compute_tp_qtys(10.0, []) == []

    def test_zero_qty(self) -> None:
        assert compute_tp_qtys(0.0, (0.5, 0.5)) == []

    def test_single_ratio(self) -> None:
        result = compute_tp_qtys(5.0, (1.0,))
        assert result == [5.0]


# ---------------------------------------------------------------------------
# compute_even_split_tp_qtys
# ---------------------------------------------------------------------------

class TestComputeEvenSplitTpQtys:
    def test_two_tp(self) -> None:
        result = compute_even_split_tp_qtys(10.0, 2)
        assert len(result) == 2
        assert sum(result) == pytest.approx(10.0)
        assert result[0] == pytest.approx(5.0)

    def test_three_tp(self) -> None:
        result = compute_even_split_tp_qtys(9.0, 3)
        assert len(result) == 3
        assert sum(result) == pytest.approx(9.0)

    def test_single_tp(self) -> None:
        assert compute_even_split_tp_qtys(5.0, 1) == [5.0]

    def test_zero_tp(self) -> None:
        assert compute_even_split_tp_qtys(5.0, 0) == []

    def test_with_step_size(self) -> None:
        result = compute_even_split_tp_qtys(1.0, 3, step_size=0.01)
        assert len(result) == 3
        assert sum(result) == pytest.approx(1.0, abs=0.01)
        for q in result:
            assert q > 0
