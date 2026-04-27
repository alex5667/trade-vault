# -*- coding: utf-8 -*-
"""
Tests for risk.position_sizer.

Coverage:
  - SymbolSpecs.__post_init__ validation
  - PositionSizer._round_lot  (step rounding, clamping)
  - PositionSizer.size_by_atr (normal path, fallback path, edge cases)
"""
from __future__ import annotations

import logging
import sys
import os

import pytest

# ── путь до пакета risk (корень scanner_infra) ───────────────────────────────
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from risk.position_sizer import PositionSizer, SymbolSpecs  # noqa: E402


# ─────────────────────────────────────────────────────────
# Helpers / fixtures
# ─────────────────────────────────────────────────────────

def _specs(
    point: float = 0.01,
    tvpl: float = 1.0,
    min_lot: float = 0.01,
    max_lot: float = 10.0,
    lot_step: float = 0.01,
) -> SymbolSpecs:
    return SymbolSpecs(
        point=point,
        tick_value_per_lot=tvpl,
        min_lot=min_lot,
        max_lot=max_lot,
        lot_step=lot_step,
    )


# ─────────────────────────────────────────────────────────
# SymbolSpecs validation
# ─────────────────────────────────────────────────────────

class TestSymbolSpecsValidation:
    def test_valid_specs_ok(self) -> None:
        s = _specs()
        assert s.point == 0.01

    @pytest.mark.parametrize("point", [0.0, -1.0])
    def test_invalid_point_raises(self, point: float) -> None:
        with pytest.raises(ValueError, match="point"):
            _specs(point=point)

    @pytest.mark.parametrize("tvpl", [0.0, -0.5])
    def test_invalid_tvpl_raises(self, tvpl: float) -> None:
        with pytest.raises(ValueError, match="tick_value_per_lot"):
            _specs(tvpl=tvpl)

    @pytest.mark.parametrize("lot_step", [0.0, -0.01])
    def test_invalid_lot_step_raises(self, lot_step: float) -> None:
        with pytest.raises(ValueError, match="lot_step"):
            _specs(lot_step=lot_step)

    def test_min_lot_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="min_lot"):
            _specs(min_lot=0.0)

    def test_max_lot_lt_min_lot_raises(self) -> None:
        with pytest.raises(ValueError, match="max_lot"):
            _specs(min_lot=0.1, max_lot=0.05)

    def test_max_lot_equal_min_lot_ok(self) -> None:
        s = _specs(min_lot=0.01, max_lot=0.01)
        assert s.max_lot == s.min_lot


# ─────────────────────────────────────────────────────────
# PositionSizer._round_lot
# ─────────────────────────────────────────────────────────

class TestRoundLot:
    def _ps(self, **kw: float) -> PositionSizer:
        return PositionSizer(_specs(**kw))

    def test_round_down_to_step(self) -> None:
        ps = self._ps(lot_step=0.01)
        # 1.999 → floor to 1.99
        assert ps._round_lot(1.999) == pytest.approx(1.99, abs=1e-8)

    def test_clamp_to_min(self) -> None:
        ps = self._ps(min_lot=0.10, lot_step=0.01)
        assert ps._round_lot(0.001) == pytest.approx(0.10, abs=1e-8)

    def test_clamp_to_max(self) -> None:
        ps = self._ps(max_lot=5.0, lot_step=0.01)
        assert ps._round_lot(999.0) == pytest.approx(5.0, abs=1e-8)

    def test_exact_step_no_change(self) -> None:
        ps = self._ps(lot_step=0.1)
        # 2.5 / 0.1 = 25.0 exactly in IEEE754 → floor = 25 → 2.5
        assert ps._round_lot(2.5) == pytest.approx(2.5, abs=1e-8)

    def test_large_step(self) -> None:
        ps = self._ps(lot_step=1.0, min_lot=1.0, max_lot=100.0)
        assert ps._round_lot(3.7) == pytest.approx(3.0, abs=1e-8)

    def test_small_step_precision(self) -> None:
        ps = self._ps(lot_step=0.001, min_lot=0.001, max_lot=100.0)
        assert ps._round_lot(1.2345) == pytest.approx(1.234, abs=1e-8)


# ─────────────────────────────────────────────────────────
# PositionSizer.size_by_atr — normal path
# ─────────────────────────────────────────────────────────

class TestSizeByAtrNormal:
    """
    Formula:
        stop_dist = atr * atr_sl_mult
        ticks     = max(stop_dist / point, 1)
        lot       = balance * risk_pct/100 / (ticks * tvpl)
    """

    def _check(
        self,
        balance: float,
        risk_pct: float,
        atr: float,
        atr_sl_mult: float,
        specs: SymbolSpecs,
    ) -> tuple[float, float]:
        ps = PositionSizer(specs)
        lot, stop = ps.size_by_atr(balance, risk_pct, atr, atr_sl_mult)
        return lot, stop

    def test_basic_calculation(self) -> None:
        # balance=10_000, risk_pct=1 → money_risk=100
        # atr=1.0, mult=1.5 → stop_dist=1.5
        # ticks = 1.5/0.01 = 150
        # raw_lot = 100 / (150 * 1.0) = 0.666...
        # rounded → 0.66 (lot_step=0.01)
        s = _specs(point=0.01, tvpl=1.0)
        lot, stop = self._check(10_000, 1.0, 1.0, 1.5, s)
        assert stop == pytest.approx(1.5)
        assert lot == pytest.approx(0.66, abs=1e-6)

    def test_stop_distance_formula(self) -> None:
        s = _specs(point=0.01, tvpl=1.0)
        ps = PositionSizer(s)
        _, stop = ps.size_by_atr(10_000, 1.0, 2.0, 2.0)
        assert stop == pytest.approx(4.0)

    def test_lot_clamped_to_min(self) -> None:
        # Very small balance → tiny raw_lot → clamped to min_lot
        s = _specs(min_lot=0.01)
        ps = PositionSizer(s)
        lot, _ = ps.size_by_atr(1.0, 0.01, 1.0, 1.5)
        assert lot == pytest.approx(s.min_lot)

    def test_lot_clamped_to_max(self) -> None:
        # Huge balance, high risk → clamped to max_lot
        s = _specs(max_lot=5.0)
        ps = PositionSizer(s)
        lot, _ = ps.size_by_atr(10_000_000, 50.0, 0.001, 0.1)
        assert lot == pytest.approx(s.max_lot)

    @pytest.mark.parametrize("risk_pct", [0.5, 1.0, 2.0, 5.0])
    def test_proportional_to_risk_pct(self, risk_pct: float) -> None:
        """Higher risk_pct → proportionally larger lot (up to max_lot clip).

        Exact proportionality is broken by lot_step rounding, so we only
        assert directional monotonicity (higher risk_pct → larger or equal lot)
        and that the result stays within ±2 lot_steps of the ideal value.
        """
        s = _specs(max_lot=1000.0)
        ps = PositionSizer(s)
        lot_ref, _ = ps.size_by_atr(100_000, 1.0, 1.0, 1.5)
        lot, _ = ps.size_by_atr(100_000, risk_pct, 1.0, 1.5)
        ideal = lot_ref * risk_pct
        # Within 2 lot_steps of the ideal value
        assert abs(lot - ideal) <= 2 * s.lot_step + ideal * 1e-3, (
            f"lot={lot}, ideal={ideal}, risk_pct={risk_pct}"
        )
        if risk_pct >= 1.0:
            assert lot >= lot_ref

    def test_ticks_floor_at_one(self) -> None:
        """When stop_dist < point → ticks clamps to 1 (no div-by-zero)."""
        s = _specs(point=10.0, tvpl=1.0)
        ps = PositionSizer(s)
        # stop_dist = 0.001 * 1.5 = 0.0015 < point=10 → ticks = 1
        lot, _ = ps.size_by_atr(10_000, 1.0, 0.001, 1.5)
        expected_raw = 10_000 * 0.01 / (1.0 * 1.0)  # 100
        expected_lot = min(s.max_lot, expected_raw)
        assert lot == pytest.approx(expected_lot, rel=1e-3)


# ─────────────────────────────────────────────────────────
# PositionSizer.size_by_atr — fallback path
# ─────────────────────────────────────────────────────────

class TestSizeByAtrFallback:
    def test_atr_zero_returns_min_lot(self, caplog: pytest.LogCaptureFixture) -> None:
        s = _specs()
        ps = PositionSizer(s)
        with caplog.at_level(logging.WARNING, logger="risk.position_sizer"):
            lot, stop = ps.size_by_atr(10_000, 1.0, 0.0, 1.5)
        assert lot == s.min_lot
        # stop = mult * max(0, 1.0) = 1.5
        assert stop == pytest.approx(1.5)
        assert "fallback" in caplog.text.lower()

    def test_atr_negative_returns_min_lot(self) -> None:
        s = _specs()
        ps = PositionSizer(s)
        lot, stop = ps.size_by_atr(10_000, 1.0, -5.0, 2.0)
        assert lot == s.min_lot
        # stop = 2.0 * max(-5.0, 1.0) = 2.0
        assert stop == pytest.approx(2.0)

    def test_positive_atr_fallback_stop_uses_atr(self) -> None:
        """If atr > 0 but point=0 (invalid specs), stop uses actual atr."""
        # Can't create SymbolSpecs with point=0 (raises), so we manually
        # bypass — test the branch logic directly by monkey-patching.
        s = _specs(point=0.01)
        ps = PositionSizer(s)
        # Temporarily make specs invalid
        ps.specs = SymbolSpecs.__new__(SymbolSpecs)
        ps.specs.point = 0.0
        ps.specs.tick_value_per_lot = 1.0
        ps.specs.min_lot = 0.01
        ps.specs.max_lot = 10.0
        ps.specs.lot_step = 0.01
        lot, stop = ps.size_by_atr(10_000, 1.0, 3.0, 2.0)
        # atr=3.0 > 0, but point=0 → fallback
        # stop = 2.0 * max(3.0, 1.0) = 6.0
        assert lot == ps.specs.min_lot
        assert stop == pytest.approx(6.0)

    def test_warning_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        s = _specs()
        ps = PositionSizer(s)
        with caplog.at_level(logging.WARNING, logger="risk.position_sizer"):
            ps.size_by_atr(10_000, 1.0, 0.0, 1.5)
        assert any("fallback" in r.message.lower() for r in caplog.records)


# ─────────────────────────────────────────────────────────
# Return-type contract
# ─────────────────────────────────────────────────────────

class TestReturnTypes:
    def test_returns_tuple_of_two_floats(self) -> None:
        ps = PositionSizer(_specs())
        result = ps.size_by_atr(10_000, 1.0, 1.0, 1.5)
        assert isinstance(result, tuple)
        assert len(result) == 2
        lot, stop = result
        assert isinstance(lot, float)
        assert isinstance(stop, float)

    def test_lot_always_positive(self) -> None:
        ps = PositionSizer(_specs())
        for atr in [0.0, 0.001, 1.0, 100.0]:
            lot, _ = ps.size_by_atr(10_000, 1.0, atr, 1.5)
            assert lot > 0, f"lot={lot} for atr={atr}"
