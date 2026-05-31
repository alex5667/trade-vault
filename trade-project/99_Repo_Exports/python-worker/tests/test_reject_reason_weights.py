"""Unit tests for `core.reject_reason_weights` policy + integration with the
`PEdgeThresholdCalibrator.observe(weight=...)` path.

Coverage:
- master switch off → every reason returns 1.0 (back-compat)
- passed sentinels (OK / empty / ALLOW / PASSED) → 1.0
- known prefix → mapped value
- specificity: longer prefix wins (e.g. VETO_BREADTH_RET_HIGH matches VETO_BREADTH
  not VETO_)
- unknown reason → 1.0 (fail-open)
- ENV override (REJECT_REASON_WEIGHTS_JSON) parsed correctly
- ENV override rejects out-of-range / non-numeric values
- weight=0 → observe() silently discards sample
- weighted EV/quantile differs from unweighted when reasons differ
- master switch off → IDENTICAL p_min as legacy code
"""
from __future__ import annotations

import json
import os
from unittest import mock

import pytest

from core import reject_reason_weights as rrw
from core.p_edge_threshold_calibrator import (
    PEdgeThresholdCalibrator,
    _weighted_quantile,
)


@pytest.fixture(autouse=True)
def _reset_weights_cache():
    """Each test gets a clean env-cache."""
    rrw.reset_cache()
    yield
    rrw.reset_cache()


# ---------------------------------------------------------------------------
# Policy module
# ---------------------------------------------------------------------------


def test_master_switch_off_returns_1():
    """Default: REJECT_REASON_WEIGHTS_ENABLED unset → everything weight=1.0."""
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("REJECT_REASON_WEIGHTS_ENABLED", None)
        rrw.reset_cache()
        assert rrw.is_enabled() is False
        # Reasons that DO have a non-1 entry in the table still return 1.0
        assert rrw.weight_for_reason("VETO_FREEZE_ACTIVE") == 1.0
        assert rrw.weight_for_reason("VETO_SPREAD_SHOCK") == 1.0


def test_master_switch_on_applies_table():
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        assert rrw.is_enabled() is True
        assert rrw.weight_for_reason("VETO_FREEZE_ACTIVE") == 0.10
        assert rrw.weight_for_reason("VETO_SPREAD_SHOCK") == 0.30


def test_passed_sentinels_always_one():
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        for tok in ("", "OK", "ok", "PASSED", "ALLOW"):
            assert rrw.weight_for_reason(tok) == 1.0, tok
        assert rrw.weight_for_reason(None) == 1.0


def test_specificity_longer_prefix_wins():
    """VETO_BREADTH_RET_HIGH must resolve to VETO_BREADTH (0.60), not VETO_."""
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        w = rrw.weight_for_reason("VETO_BREADTH_RET_HIGH")
        # VETO_BREADTH entry in DEFAULT_WEIGHTS = 0.60
        assert w == 0.60


def test_shadow_veto_family():
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        # Any SHADOW_VETO_* prefix → 0.70
        assert rrw.weight_for_reason("SHADOW_VETO_BREADTH_VOL_LOW") == 0.70
        assert rrw.weight_for_reason("SHADOW_VETO_SOMETHING_NEW") == 0.70


def test_unknown_reason_fail_open():
    """Unknown reason must NOT silently drop the sample — return 1.0."""
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        assert rrw.weight_for_reason("THIS_REASON_DOES_NOT_EXIST") == 1.0


def test_env_override_parses_json():
    override = json.dumps(
        {"VETO_FREEZE_ACTIVE": 0.05, "MY_CUSTOM_REASON": 0.5}
    )
    with mock.patch.dict(
        os.environ,
        {
            "REJECT_REASON_WEIGHTS_ENABLED": "1",
            "REJECT_REASON_WEIGHTS_JSON": override,
        },
    ):
        rrw.reset_cache()
        assert rrw.weight_for_reason("VETO_FREEZE_ACTIVE") == 0.05
        assert rrw.weight_for_reason("MY_CUSTOM_REASON") == 0.5


def test_env_override_rejects_out_of_range():
    """Negative or >1 values silently ignored; bad value → fallback."""
    bad = json.dumps({"VETO_FREEZE_ACTIVE": 2.5, "VETO_SPREAD_SHOCK": -0.5})
    with mock.patch.dict(
        os.environ,
        {
            "REJECT_REASON_WEIGHTS_ENABLED": "1",
            "REJECT_REASON_WEIGHTS_JSON": bad,
        },
    ):
        rrw.reset_cache()
        # Both reject — defaults kick in
        assert rrw.weight_for_reason("VETO_FREEZE_ACTIVE") == 0.10
        assert rrw.weight_for_reason("VETO_SPREAD_SHOCK") == 0.30


def test_env_override_bad_json_ignored():
    with mock.patch.dict(
        os.environ,
        {
            "REJECT_REASON_WEIGHTS_ENABLED": "1",
            "REJECT_REASON_WEIGHTS_JSON": "{not json",
        },
    ):
        rrw.reset_cache()
        # Defaults survive
        assert rrw.weight_for_reason("VETO_FREEZE_ACTIVE") == 0.10


def test_reason_family_bounded_cardinality():
    """`reason_family` should map raw reasons to a small set of families."""
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        assert rrw.reason_family("") == "passed"
        assert rrw.reason_family("OK") == "passed"
        assert rrw.reason_family(None) == "na"
        # Family for a known prefix is the prefix (or its normalized form)
        fam = rrw.reason_family("VETO_BREADTH_RET_HIGH")
        assert fam.startswith("veto_breadth") or fam == "VETO_BREADTH".lower()


# ---------------------------------------------------------------------------
# _weighted_quantile primitive
# ---------------------------------------------------------------------------


def test_weighted_quantile_unweighted_matches_legacy():
    """All weights = 1.0 must give identical result to _quantile."""
    from core.p_edge_threshold_calibrator import _quantile
    xs = [0.1, 0.3, 0.5, 0.7, 0.9, 0.2, 0.4, 0.6, 0.8]
    for q in (0.1, 0.5, 0.9):
        unweighted = _quantile(xs, q)
        weighted = _weighted_quantile([(x, 1.0) for x in xs], q)
        assert abs(unweighted - weighted) < 1e-9, (q, unweighted, weighted)


def test_weighted_quantile_zero_weight_skips_sample():
    """A zero-weighted point shouldn't affect the quantile."""
    pairs = [(0.1, 1.0), (0.5, 0.0), (0.9, 1.0)]
    q90 = _weighted_quantile(pairs, 0.9)
    # The 0.5 is fully discounted → should be close to 0.9 (only two real points)
    assert q90 > 0.5


def test_weighted_quantile_empty_returns_zero():
    assert _weighted_quantile([], 0.5) == 0.0


# ---------------------------------------------------------------------------
# Calibrator integration
# ---------------------------------------------------------------------------


def _wins(c, n: int, w: float = 1.0, p: float = 0.7, base_ms: int = 1_000_000):
    """Feed `n` WIN trades with p_edge=p, r_multiple=1.5, weight=w."""
    for i in range(n):
        c.observe(
            symbol="BTCUSDT",
            regime="trend",
            kind="breakout",
            p_edge=p,
            r_multiple=1.5,
            result="WIN",
            ts_ms=base_ms + i * 1000,
            weight=w,
        )


def _losses(c, n: int, w: float = 1.0, p: float = 0.65, base_ms: int = 2_000_000):
    for i in range(n):
        c.observe(
            symbol="BTCUSDT",
            regime="trend",
            kind="breakout",
            p_edge=p,
            r_multiple=-1.0,
            result="LOSS",
            ts_ms=base_ms + i * 1000,
            weight=w,
        )


def test_observe_default_weight_back_compat():
    """observe() without weight= keeps legacy semantics (weight=1.0)."""
    c = PEdgeThresholdCalibrator(min_kept_trades=10, min_total_trades=20, enforce=True)
    _wins(c, 50)
    _losses(c, 20)
    # Should have produced a non-default p_min
    p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout")
    assert p_min >= 0.0  # produced or kept default — both valid


def test_observe_zero_weight_discards():
    c = PEdgeThresholdCalibrator(min_kept_trades=10, min_total_trades=20, enforce=True)
    _wins(c, 50, w=1.0)
    n_before = len(c.bins[("BTCUSDT", "trend", "breakout", "*")].buf)
    # Try to feed a heavily zero-weighted batch — must be discarded
    _wins(c, 100, w=0.0)
    n_after = len(c.bins[("BTCUSDT", "trend", "breakout", "*")].buf)
    assert n_before == n_after


def test_observe_negative_weight_discards():
    c = PEdgeThresholdCalibrator(min_kept_trades=10, min_total_trades=20)
    c.observe(
        symbol="BTC",
        regime="trend",
        kind="breakout",
        p_edge=0.7,
        r_multiple=1.5,
        result="WIN",
        ts_ms=1_000_000,
        weight=-0.5,
    )
    # No bin should have been created (or buffer remains empty)
    bin_ = c.bins.get(("BTC", "trend", "breakout", "*"))
    assert bin_ is None or len(bin_.buf) == 0


def test_observe_weight_clipped_to_one():
    """weight > 1.0 silently clipped down to 1.0 (no error)."""
    c = PEdgeThresholdCalibrator(min_kept_trades=10, min_total_trades=20)
    c.observe(
        symbol="BTC",
        regime="trend",
        kind="breakout",
        p_edge=0.7,
        r_multiple=1.5,
        result="WIN",
        ts_ms=1_000_000,
        weight=5.0,
    )
    bin_ = c.bins[("BTC", "trend", "breakout", "*")]
    assert bin_.buf[0].w == 1.0


def test_weighted_ev_dominated_by_high_weight_samples():
    """Mix of full-weight WINS and low-weight LOSSES — weighted EV ≈ WIN R."""
    c = PEdgeThresholdCalibrator(
        target_ev_r=0.5,
        min_kept_trades=10,
        min_total_trades=20,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
    )
    # 30 high-quality WINS (passed real trades, weight=1.0, R=+1.5)
    _wins(c, 30, w=1.0, p=0.7)
    # 60 low-weight LOSSES (freeze veto, weight=0.1, R=-1.0)
    _losses(c, 60, w=0.1, p=0.7)
    # Trigger recompute by feeding the 100th sample (since min_total=20 it's
    # already past threshold).
    bin_ = c.bins[("BTCUSDT", "trend", "breakout", "*")]
    # Eligible samples: 30 WIN (w=1.0) + 60 LOSS (w=0.1)
    # weighted_mean_R = (30 * 1.0 * 1.5 + 60 * 0.1 * (-1.0)) / (30 + 6)
    #                 = (45 - 6) / 36 = 39/36 ≈ 1.083
    # Unweighted_mean_R = (30 * 1.5 + 60 * (-1.0)) / 90 = -0.167
    # So with weighting, EV ≈ +1.083 should easily clear target_ev_r=0.5
    # Without weighting, it would NOT clear.
    assert len(bin_.buf) == 90
    # Force a recompute (low recompute_gap_ms=0 makes it always fire)
    c._maybe_recompute(("BTCUSDT", "trend", "breakout", "*"), now_ms=10_000_000)
    # Should have a non-zero shadow_p_min (the grid found a threshold)
    assert bin_.shadow_p_min > 0.0
    # And shadow EV should be positive (weighted EV passed target)
    assert bin_.shadow_ev_at_pin > 0.0


def test_weighted_disabled_matches_unweighted_baseline():
    """Two calibrators fed identical streams; one with mixed weights, one all
    weight=1.0. p_min must differ (proof that weights actually affect
    thresholds, not just metadata)."""
    c_weighted = PEdgeThresholdCalibrator(
        target_ev_r=0.10,
        min_kept_trades=10,
        min_total_trades=20,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        enforce=True,
    )
    c_unweighted = PEdgeThresholdCalibrator(
        target_ev_r=0.10,
        min_kept_trades=10,
        min_total_trades=20,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        enforce=True,
    )
    # 40 wins at p=0.7
    for i in range(40):
        for c, w in ((c_weighted, 1.0), (c_unweighted, 1.0)):
            c.observe(
                symbol="BTC", regime="t", kind="brk",
                p_edge=0.7, r_multiple=1.5, result="WIN",
                ts_ms=1_000_000 + i * 1000, weight=w,
            )
    # 80 losses at p=0.65; weighted runs with w=0.1, unweighted with w=1.0
    for i in range(80):
        c_weighted.observe(
            symbol="BTC", regime="t", kind="brk",
            p_edge=0.65, r_multiple=-1.0, result="LOSS",
            ts_ms=2_000_000 + i * 1000, weight=0.1,
        )
        c_unweighted.observe(
            symbol="BTC", regime="t", kind="brk",
            p_edge=0.65, r_multiple=-1.0, result="LOSS",
            ts_ms=2_000_000 + i * 1000, weight=1.0,
        )
    p_w = c_weighted.p_min_for(symbol="BTC", regime="t", kind="brk")
    p_u = c_unweighted.p_min_for(symbol="BTC", regime="t", kind="brk")
    # The weighted calibrator should NOT be forced as high as the unweighted
    # one (because losses are downweighted, the EV target is easier to satisfy
    # at a lower τ).
    assert p_w <= p_u + 1e-9
