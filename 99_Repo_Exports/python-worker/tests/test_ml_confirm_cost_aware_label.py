"""Tests for Task 2.1 cost-aware label augmentation."""
from __future__ import annotations

import pandas as pd
import pytest

from tools.ml_confirm_cost_aware_label_v1 import (
    apply_cost_aware_label,
    _resolve_slippage_bps,
)


def test_resolve_slippage_realized_first():
    row = {"slippage_realized_bps": 3.5, "expected_slippage_bps": 5.0}
    bps, src = _resolve_slippage_bps(row, col="slippage_realized_bps", fallback_bps=4.0)
    assert bps == 3.5
    assert src == "realized"


def test_resolve_slippage_falls_back_to_expected():
    row = {"expected_slippage_bps": 6.0}
    bps, src = _resolve_slippage_bps(row, col="slippage_realized_bps", fallback_bps=4.0)
    assert bps == 6.0
    assert src == "expected"


def test_resolve_slippage_fallback_when_missing():
    row = {}
    bps, src = _resolve_slippage_bps(row, col="slippage_realized_bps", fallback_bps=4.0)
    assert bps == 4.0
    assert src == "fallback"


def test_resolve_slippage_negative_treated_as_missing():
    row = {"slippage_realized_bps": -1.0, "expected_slippage_bps": 7.0}
    bps, src = _resolve_slippage_bps(row, col="slippage_realized_bps", fallback_bps=4.0)
    assert bps == 7.0
    assert src == "expected"


def test_cost_aware_label_marks_loss_when_costs_exceed_pnl():
    df = pd.DataFrame([
        {"pnl_net": 1.0, "fees": 1.5, "notional_usd": 10000.0, "slippage_realized_bps": 0.0},
    ])
    out = apply_cost_aware_label(
        df.copy(), fee_mul=2.0, slippage_bps_fallback=0.0, slippage_bps_col="slippage_realized_bps"
    )
    # cost = 2*1.5 = 3.0; pnl - cost = -2.0 → loss
    assert int(out.loc[0, "y_cost_aware"]) == 0
    assert out.loc[0, "cost_total_usd"] == pytest.approx(3.0)


def test_cost_aware_label_marks_win_when_pnl_exceeds_costs():
    df = pd.DataFrame([
        {"pnl_net": 20.0, "fees": 1.0, "notional_usd": 10000.0, "slippage_realized_bps": 1.0},
    ])
    out = apply_cost_aware_label(
        df.copy(), fee_mul=2.0, slippage_bps_fallback=0.0, slippage_bps_col="slippage_realized_bps"
    )
    # cost = 2*1.0 + (1.0/10000)*10000 = 2.0 + 1.0 = 3.0; pnl - cost = 17.0 → win
    assert int(out.loc[0, "y_cost_aware"]) == 1
    assert out.loc[0, "slippage_realized_usd"] == pytest.approx(1.0)


def test_cost_aware_label_slippage_source_tracking():
    df = pd.DataFrame([
        {"pnl_net": 10.0, "fees": 0.5, "notional_usd": 1000.0, "slippage_realized_bps": 2.0},
        {"pnl_net": 10.0, "fees": 0.5, "notional_usd": 1000.0, "expected_slippage_bps": 3.0},
        {"pnl_net": 10.0, "fees": 0.5, "notional_usd": 1000.0},
    ])
    out = apply_cost_aware_label(
        df.copy(), fee_mul=2.0, slippage_bps_fallback=4.0, slippage_bps_col="slippage_realized_bps"
    )
    assert list(out["slippage_bps_source"]) == ["realized", "expected", "fallback"]
    assert list(out["slippage_bps_used"]) == [2.0, 3.0, 4.0]


def test_cost_aware_label_missing_required_columns():
    df = pd.DataFrame([{"pnl_net": 1.0}])
    with pytest.raises(ValueError, match="fees"):
        apply_cost_aware_label(df, fee_mul=2.0, slippage_bps_fallback=4.0, slippage_bps_col="slippage_realized_bps")


def test_cost_aware_label_no_notional_zero_slip():
    df = pd.DataFrame([{"pnl_net": 5.0, "fees": 1.0}])
    out = apply_cost_aware_label(
        df.copy(), fee_mul=2.0, slippage_bps_fallback=4.0, slippage_bps_col="slippage_realized_bps"
    )
    # No notional → slip_usd=0 even though fallback bps=4; cost = 2*1=2; pnl-cost=3 → win
    assert int(out.loc[0, "y_cost_aware"]) == 1
    assert out.loc[0, "slippage_realized_usd"] == pytest.approx(0.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
