"""
Unit tests for cost-aware triple-barrier labeler (v14_of extension).

Covers:
  - backward-compat: cost_bps=0 (default) → identical outcomes vs legacy callers
  - cost_bps>0: edge_after_cost_bps + y_edge_cost_aware computed correctly
  - NO_TICKS path emits cost-aware fields (paid cost, realized nothing)
  - TIMEOUT closes at last observed signed_bps
  - SHORT direction symmetric to LONG
  - BarrierResult new fields default to 0.0 / 0 for backward-compat consumers
"""

from core.triple_barrier import BarrierOutcome, BarrierResult, BarrierSpec, label_path


# ---------------------------------------------------------------------------
# Backward-compat: cost_bps default = 0.0
# ---------------------------------------------------------------------------

def test_default_spec_has_cost_bps_zero():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    assert spec.cost_bps == 0.0


def test_default_result_has_cost_fields_zero():
    """Existing callers reading mae_bps/mfe_bps don't see new fields populated when cost=0."""
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)  # cost_bps default 0.0
    path = [(0, 100.0), (100, 100.11)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.TP_HIT
    assert r.cost_bps == 0.0
    # edge_after_cost == realized_close (cost is 0)
    assert r.edge_after_cost_bps == r.realized_close_bps
    assert r.y_edge_cost_aware == 1  # gross positive


def test_backward_compat_tp_hit_matches_legacy():
    """Same inputs as old test → same outcome (no regression)."""
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    path = [(0, 100.0), (100, 100.11)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.TP_HIT


def test_backward_compat_sl_hit_matches_legacy():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    path = [(0, 100.0), (100, 99.89)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.SL_HIT


def test_backward_compat_short_tp_hit():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    path = [(0, 100.0), (100, 99.88)]
    r = label_path(ts0_ms=0, direction="SHORT", entry_px=100.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.TP_HIT


# ---------------------------------------------------------------------------
# Cost-aware path: cost_bps > 0
# ---------------------------------------------------------------------------

def test_tp_hit_with_cost_keeps_outcome_but_reduces_edge():
    """TP barrier crossed (gross move) — outcome stays TP_HIT, but edge_after_cost is reduced."""
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0, cost_bps=4.0)
    # Move = +11 bps (100.0 → 100.11). Above gross TP of 10 bps.
    path = [(0, 100.0), (100, 100.11)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)

    assert r.outcome == BarrierOutcome.TP_HIT
    assert r.cost_bps == 4.0
    # realized_close ≈ 11 bps (slightly over barrier)
    assert abs(r.realized_close_bps - 11.0) < 0.01
    # edge_after_cost ≈ 11 - 4 = 7 bps → positive, y_edge_cost_aware = 1
    assert abs(r.edge_after_cost_bps - 7.0) < 0.01
    assert r.y_edge_cost_aware == 1


def test_tp_hit_with_high_cost_flips_y_edge_to_zero():
    """TP fires (gross), but cost > realized move → edge_after_cost negative → label 0."""
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0, cost_bps=20.0)
    path = [(0, 100.0), (100, 100.11)]  # gross +11 bps
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)

    assert r.outcome == BarrierOutcome.TP_HIT  # outcome unchanged
    assert abs(r.realized_close_bps - 11.0) < 0.01
    # edge_after_cost = 11 - 20 = -9 → label 0
    assert abs(r.edge_after_cost_bps - (-9.0)) < 0.01
    assert r.y_edge_cost_aware == 0


def test_sl_hit_with_cost_amplifies_loss():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0, cost_bps=3.0)
    path = [(0, 100.0), (100, 99.89)]  # SL hit at -11 bps
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)

    assert r.outcome == BarrierOutcome.SL_HIT
    assert abs(r.realized_close_bps - (-11.0)) < 0.01
    # edge_after_cost = -11 - 3 = -14 → label 0
    assert abs(r.edge_after_cost_bps - (-14.0)) < 0.01
    assert r.y_edge_cost_aware == 0


def test_short_tp_hit_with_cost_consistent():
    """SHORT direction: cost-aware computation is sign-symmetric."""
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0, cost_bps=2.0)
    path = [(0, 100.0), (100, 99.88)]  # SHORT TP: price drops → signed +12 bps from short's POV
    r = label_path(ts0_ms=0, direction="SHORT", entry_px=100.0, path=path, spec=spec)

    assert r.outcome == BarrierOutcome.TP_HIT
    assert abs(r.realized_close_bps - 12.0) < 0.01
    assert abs(r.edge_after_cost_bps - 10.0) < 0.01
    assert r.y_edge_cost_aware == 1


# ---------------------------------------------------------------------------
# TIMEOUT: realized close = last observed signed_bps
# ---------------------------------------------------------------------------

def test_timeout_closes_at_last_tick():
    """No TP/SL hit → TIMEOUT, realized_close = last signed_bps.

    STRICT semantic: TIMEOUT is NEVER y_edge_cost_aware=1 even if edge_after_cost > 0,
    because the label gates on TP_HIT (aligned with legacy y_edge). TIMEOUT-positive
    cases carry close-assumption risk and are excluded.
    """
    spec = BarrierSpec(h_ms=1000, tp_bps=50.0, sl_bps=50.0, cost_bps=1.0)
    # Path stays in [-5, +5] bps — never hits ±50 barriers
    path = [(0, 100.0), (100, 100.03), (500, 100.05), (900, 100.02)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)

    assert r.outcome == BarrierOutcome.TIMEOUT
    # last sb ≈ 2 bps
    assert abs(r.realized_close_bps - 2.0) < 0.01
    # edge_after_cost = 2 - 1 = 1 bps (positive), BUT strict gates on TP_HIT → y=0
    assert abs(r.edge_after_cost_bps - 1.0) < 0.01
    assert r.y_edge_cost_aware == 0


def test_timeout_negative_close_zero_label():
    spec = BarrierSpec(h_ms=1000, tp_bps=50.0, sl_bps=50.0, cost_bps=0.0)
    path = [(0, 100.0), (100, 100.05), (900, 99.98)]  # ends below entry
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)

    assert r.outcome == BarrierOutcome.TIMEOUT
    assert r.realized_close_bps < 0.0
    assert r.y_edge_cost_aware == 0


# ---------------------------------------------------------------------------
# NO_TICKS: cost still tracked (paid entry, no realization)
# ---------------------------------------------------------------------------

def test_no_ticks_with_cost_records_negative_edge():
    """Zero entry_px → NO_TICKS. cost_bps still surfaced; edge = -cost."""
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0, cost_bps=5.0)
    r = label_path(ts0_ms=0, direction="LONG", entry_px=0.0, path=[(0, 100.0)], spec=spec)

    assert r.outcome == BarrierOutcome.NO_TICKS
    assert r.cost_bps == 5.0
    assert r.realized_close_bps == 0.0
    assert r.edge_after_cost_bps == -5.0
    assert r.y_edge_cost_aware == 0


def test_no_ticks_with_zero_cost_backward_compat():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    r = label_path(ts0_ms=0, direction="LONG", entry_px=0.0, path=[(0, 100.0)], spec=spec)

    assert r.outcome == BarrierOutcome.NO_TICKS
    assert r.cost_bps == 0.0
    assert r.edge_after_cost_bps == 0.0
    assert r.y_edge_cost_aware == 0


def test_empty_path_returns_no_ticks():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0, cost_bps=2.0)
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=[], spec=spec)

    assert r.outcome == BarrierOutcome.NO_TICKS
    assert r.edge_after_cost_bps == -2.0


# ---------------------------------------------------------------------------
# Frozen dataclass: callers may still construct legacy form (no cost fields)
# ---------------------------------------------------------------------------

def test_barrier_result_can_be_constructed_legacy_style():
    """Existing code that constructs BarrierResult without new fields must still work
    (new fields have defaults)."""
    r = BarrierResult(
        outcome=BarrierOutcome.TP_HIT,
        hit_ms=100,
        mae_bps=2.0,
        mfe_bps=12.0,
        adverse_proxy=0.17,
    )
    # New fields take their dataclass defaults
    assert r.cost_bps == 0.0
    assert r.realized_close_bps == 0.0
    assert r.edge_after_cost_bps == 0.0
    assert r.y_edge_cost_aware == 0


def test_barrier_spec_can_be_constructed_legacy_style():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    assert spec.cost_bps == 0.0
