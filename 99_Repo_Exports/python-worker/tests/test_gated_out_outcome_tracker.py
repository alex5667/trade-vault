"""Unit tests for gated_out_outcome_tracker._evaluate_path.

Covers:
  Test 1 — v2 ML metadata propagated to outcome payload.
  Test 2 — cost-aware label (y_edge_cost_aware) with full cost model:
    TP+15 bps, fees=10 → positive (15-10=+5 > 0)
    TP+8 bps,  fees=10 → negative (8-10=-2 < 0)
    TIMEOUT+20 bps    → negative by policy (path uncertainty)
    TP with spread → positive only when net > 0 after fees+spread/2+slippage
"""
from __future__ import annotations

import pytest

from services.gated_out_outcome_tracker.tracker import (
    COST_FEES_BPS_RT,
    PendingSignal,
    _evaluate_path,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pending(
    entry: float = 60_000.0,
    tp_bps: float = 20.0,
    sl_bps: float = 10.0,
    direction: str = "LONG",
    spread_bps: float = 0.0,
    expected_slippage_bps: float = 0.0,
    sample_policy: str = "confidence_gated_out",
    selection_policy_version: str = "v1",
    selection_prob: float = 0.90,
    selection_weight: float = 0.90,
    virtual_min_conf: float = 0.35,
    meets_virtual_threshold: int = 1,
) -> PendingSignal:
    ts = 1_716_000_000_000
    return PendingSignal(
        msg_id="msg1", sid="sid1", symbol="BTCUSDT", direction=direction,
        entry=entry, sl=entry * 0.99, tp_bps=tp_bps, sl_bps=sl_bps,
        ts_ms=ts, confidence=0.45, min_conf=0.35, expire_ms=ts + 1_800_000,
        spread_bps=spread_bps,
        expected_slippage_bps=expected_slippage_bps,
        sample_policy=sample_policy,
        selection_policy_version=selection_policy_version,
        selection_prob=selection_prob,
        selection_weight=selection_weight,
        virtual_min_conf=virtual_min_conf,
        meets_virtual_threshold=meets_virtual_threshold,
    )


def _tp_path(p: PendingSignal) -> list[tuple[int, float]]:
    """Path that hits TP for both LONG and SHORT."""
    ts = p.ts_ms
    sign = 1.0 if p.direction == "LONG" else -1.0
    tp_px = p.entry * (1 + sign * p.tp_bps / 1e4)
    return [(ts, p.entry), (ts + 500, tp_px + sign * 1.0)]


def _timeout_path(p: PendingSignal, ret_bps: float = 20.0) -> list[tuple[int, float]]:
    """Path that neither hits TP nor SL — ends with a positive return."""
    ts = p.ts_ms
    sign = 1.0 if p.direction == "LONG" else -1.0
    close_px = p.entry * (1 + sign * ret_bps / 1e4)
    # keep price between SL and TP
    return [(ts, p.entry), (ts + 1_800_000, close_px)]


# ---------------------------------------------------------------------------
# Test 1: v2 ML metadata propagated to outcome payload
# ---------------------------------------------------------------------------

class TestOutcomeV2Metadata:
    def test_sample_policy_in_outcome(self) -> None:
        p = _make_pending(sample_policy="confidence_gated_out")
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["sample_policy"] == "confidence_gated_out"

    def test_selection_fields_in_outcome(self) -> None:
        p = _make_pending(selection_prob=0.88, selection_weight=0.88,
                          selection_policy_version="v2")
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["selection_weight"] == pytest.approx(0.88)
        assert result["selection_prob"] == pytest.approx(0.88)
        assert result["selection_policy_version"] == "v2"

    def test_virtual_min_conf_and_threshold_flag_in_outcome(self) -> None:
        p = _make_pending(virtual_min_conf=0.35, meets_virtual_threshold=1)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["virtual_min_conf"] == pytest.approx(0.35)
        assert result["meets_virtual_threshold"] == 1

    def test_schema_version_is_2(self) -> None:
        p = _make_pending()
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["v"] == 2


# ---------------------------------------------------------------------------
# Test 2: cost-aware label
# ---------------------------------------------------------------------------

class TestCostAwareLabel:
    """y_edge_cost_aware=1 only when TP_HIT and net edge after all costs > 0."""

    def test_tp_above_fees_is_positive(self) -> None:
        # tp_bps=15, fees=10 (default), spread=0, slip=0 → 15-10=+5 → 1
        p = _make_pending(tp_bps=15.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 1
        assert result["edge_after_cost_bps"] == pytest.approx(15.0 - COST_FEES_BPS_RT)

    def test_tp_below_fees_is_negative(self) -> None:
        # tp_bps=8, fees=10 → 8-10=-2 → 0
        p = _make_pending(tp_bps=8.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 0
        assert result["edge_after_cost_bps"] == pytest.approx(8.0 - COST_FEES_BPS_RT)

    def test_timeout_always_zero_regardless_of_return(self) -> None:
        # TIMEOUT with +20 bps → still 0 by policy
        p = _make_pending(tp_bps=50.0)  # high TP so path doesn't touch it
        result = _evaluate_path(p, _timeout_path(p, ret_bps=20.0))
        assert result is not None
        assert result["outcome"] == "TIMEOUT"
        assert result["y_edge_cost_aware"] == 0

    def test_sl_hit_always_zero(self) -> None:
        p = _make_pending()
        sl_px = p.entry * (1 - p.sl_bps / 1e4) - 1.0  # below SL
        path = [(p.ts_ms, p.entry), (p.ts_ms + 500, sl_px)]
        result = _evaluate_path(p, path)
        assert result is not None
        assert result["outcome"] == "SL_HIT"
        assert result["y_edge_cost_aware"] == 0

    def test_spread_reduces_net_edge(self) -> None:
        # tp_bps=15, fees=10, spread=12 → cost = 10 + 12/2 = 16 → 15-16=-1 → 0
        p = _make_pending(tp_bps=15.0, spread_bps=12.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 0
        assert result["cost_bps"] == pytest.approx(COST_FEES_BPS_RT + 6.0)

    def test_slippage_reduces_net_edge(self) -> None:
        # tp_bps=18, fees=10, slippage=5 → cost=15 → 18-15=+3 → 1
        p = _make_pending(tp_bps=18.0, expected_slippage_bps=5.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 1
        assert result["cost_bps"] == pytest.approx(COST_FEES_BPS_RT + 5.0)

    def test_cost_breakdown_fields_present(self) -> None:
        p = _make_pending(tp_bps=20.0, spread_bps=4.0, expected_slippage_bps=3.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert "cost_fees_bps" in result
        assert "cost_spread_bps" in result
        assert "cost_slippage_bps" in result
        assert result["cost_fees_bps"] == pytest.approx(COST_FEES_BPS_RT)
        assert result["cost_spread_bps"] == pytest.approx(2.0)   # spread/2
        assert result["cost_slippage_bps"] == pytest.approx(3.0)

    def test_combined_spread_slippage_negative(self) -> None:
        """tp=15, fees=10, spread=4, slippage=5 → cost=17, edge=-2 → y_edge_cost_aware=0."""
        p = _make_pending(tp_bps=15.0, spread_bps=4.0, expected_slippage_bps=5.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        # cost = 10 + 4/2 + 5 = 17
        assert result["cost_bps"] == pytest.approx(17.0)
        assert result["edge_after_cost_bps"] == pytest.approx(15.0 - 17.0)
        assert result["y_edge_cost_aware"] == 0

    def test_combined_spread_slippage_positive(self) -> None:
        """tp=25, fees=10, spread=4, slippage=5 → cost=17, edge=+8 → y_edge_cost_aware=1."""
        p = _make_pending(tp_bps=25.0, spread_bps=4.0, expected_slippage_bps=5.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["cost_bps"] == pytest.approx(17.0)
        assert result["edge_after_cost_bps"] == pytest.approx(25.0 - 17.0)
        assert result["y_edge_cost_aware"] == 1
