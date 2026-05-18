from __future__ import annotations

import json
import random
from types import SimpleNamespace

from core.z_mapping_calibrator import (
    DEFAULT_BOUNDS,
    ZMappingCalibrator,
    _mad,
    _median,
    _quantile,
)
from services.signal_confidence import ConfidenceScorer


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _feed(
    c: ZMappingCalibrator,
    *,
    metric: str,
    symbol: str,
    regime: str,
    session: str,
    samples: list[float],
    base_ms: int = 0,
    step_ms: int = 100,
) -> int:
    """Feed samples through observe(); returns the last ts."""
    t = base_ms
    for v in samples:
        c.observe(
            metric,
            symbol=symbol,
            regime=regime,
            session=session,
            z_abs=v,
            now_ms=t,
        )
        t += step_ms
    return t


# ---------------------------------------------------------------------------
# numerical primitives
# ---------------------------------------------------------------------------


def test_quantile_basic() -> None:
    xs = [float(i) for i in range(1, 101)]  # 1..100
    assert _quantile(xs, 0.0) == 1.0
    # q is clamped to 0.999 to avoid edge fragility; q=1.0 ≈ 99.901
    assert abs(_quantile(xs, 1.0) - 99.901) < 1e-3
    # Monotonic [1..100] at q=0.5 -> 50.5 via linear interpolation
    assert abs(_quantile(xs, 0.5) - 50.5) < 1e-6
    # q=0.95 → index ≈ 94.05 → ≈ 95.05
    assert abs(_quantile(xs, 0.95) - 95.05) < 1e-6


def test_quantile_empty_and_single() -> None:
    assert _quantile([], 0.5) == 0.0
    assert _quantile([7.0], 0.9) == 7.0


def test_median_and_mad() -> None:
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _median(xs) == 3.0
    # deviations: 2,1,0,1,2 → MAD = 1
    assert _mad(xs) == 1.0
    assert _mad([5.0, 5.0, 5.0]) == 0.0


# ---------------------------------------------------------------------------
# cold start & fallback
# ---------------------------------------------------------------------------


def test_bounds_returns_defaults_before_warmup() -> None:
    c = ZMappingCalibrator(enforce=True)
    lo, hi = c.bounds("main_z", symbol="BTCUSDT", regime="trend", session="us")
    assert (lo, hi) == DEFAULT_BOUNDS["main_z"]

    lo, hi = c.bounds("obi_z", symbol="BTCUSDT", regime="trend", session="us")
    assert (lo, hi) == DEFAULT_BOUNDS["obi_z"]


def test_unknown_metric_returns_safe_defaults() -> None:
    c = ZMappingCalibrator(enforce=True)
    # custom defaults respected
    lo, hi = c.bounds(
        "unknown_metric",
        symbol="X",
        regime="trend",
        session="us",
        default_lo=2.0,
        default_hi=7.0,
    )
    assert (lo, hi) == (2.0, 7.0)


def test_shadow_mode_does_not_apply_bounds() -> None:
    c = ZMappingCalibrator(enforce=False, min_samples=50, hold_ms=0, recompute_gap_ms=0)
    _feed(
        c,
        metric="main_z",
        symbol="BTCUSDT",
        regime="trend",
        session="us",
        samples=[1.0 + 0.01 * i for i in range(400)],
    )
    # enforce=False → defaults
    lo, hi = c.bounds("main_z", symbol="BTCUSDT", regime="trend", session="us")
    assert (lo, hi) == DEFAULT_BOUNDS["main_z"]
    # but shadow has been updated
    s_lo, s_hi = c.shadow_bounds("main_z", symbol="BTCUSDT", regime="trend", session="us")
    assert s_lo > 0.0 and s_hi > s_lo


# ---------------------------------------------------------------------------
# quantile-driven adaptation
# ---------------------------------------------------------------------------


def test_low_vol_symbol_compresses_bounds() -> None:
    """Symbol with |z| centered around 0.5–1.5 should get tighter bounds than default."""
    c = ZMappingCalibrator(
        enforce=True, min_samples=50, hold_ms=0, recompute_gap_ms=0,
        rel_thresh=0.0,  # no hysteresis for this test
        max_jump_mult=10.0,  # no jump cap
    )
    rng = random.Random(42)
    samples = [abs(rng.gauss(0.0, 0.6)) for _ in range(500)]
    _feed(
        c,
        metric="main_z",
        symbol="LOWVOL",
        regime="range",
        session="eu",
        samples=samples,
    )
    lo, hi = c.bounds("main_z", symbol="LOWVOL", regime="range", session="eu")
    d_lo, d_hi = DEFAULT_BOUNDS["main_z"]
    # On a low-vol symbol q60 should be well below default 1.0
    assert lo < d_lo
    # And q95 of |N(0,0.6)| ≈ 1.18 << default 4.0
    assert hi < d_hi


def test_high_vol_symbol_widens_bounds() -> None:
    c = ZMappingCalibrator(
        enforce=True, min_samples=50, hold_ms=0, recompute_gap_ms=0,
        rel_thresh=0.0,
        max_jump_mult=10.0,
    )
    rng = random.Random(7)
    samples = [abs(rng.gauss(0.0, 3.0)) for _ in range(500)]
    _feed(
        c,
        metric="main_z",
        symbol="HIGHVOL",
        regime="trend",
        session="us",
        samples=samples,
    )
    lo, hi = c.bounds("main_z", symbol="HIGHVOL", regime="trend", session="us")
    d_lo, _ = DEFAULT_BOUNDS["main_z"]
    # q60 of |N(0,3)| ≈ 2.5 > default 1.0
    assert lo > d_lo
    assert hi > lo  # ordering preserved


def test_min_spacing_enforced_on_degenerate_input() -> None:
    """Constant non-zero input is rejected by MAD floor; degenerate jitter
    just above the floor produces collapsed q60/q95 but the calibrator
    refuses to commit when MAD is below the floor."""
    c = ZMappingCalibrator(
        enforce=True, min_samples=10, hold_ms=0, recompute_gap_ms=0,
        mad_floor=1e-6,
    )
    # Constant samples → MAD = 0 → no commit
    _feed(
        c,
        metric="main_z",
        symbol="FLAT",
        regime="range",
        session="eu",
        samples=[2.0] * 50,
    )
    lo, hi = c.bounds("main_z", symbol="FLAT", regime="range", session="eu")
    assert (lo, hi) == DEFAULT_BOUNDS["main_z"]


# ---------------------------------------------------------------------------
# hysteresis & jump-limit
# ---------------------------------------------------------------------------


def test_hysteresis_skips_small_changes() -> None:
    c = ZMappingCalibrator(
        enforce=True,
        min_samples=50,
        hold_ms=0,
        recompute_gap_ms=0,
        rel_thresh=0.20,  # require ≥20% change to commit
        max_jump_mult=10.0,
    )
    rng = random.Random(99)
    s1 = [abs(rng.gauss(0.0, 1.0)) for _ in range(300)]
    last = _feed(c, metric="main_z", symbol="S", regime="trend", session="us", samples=s1)
    lo1, hi1 = c.bounds("main_z", symbol="S", regime="trend", session="us")
    assert lo1 > 0.0 and hi1 > lo1

    # Feed very similar data — should not move bounds (within hysteresis)
    s2 = [abs(rng.gauss(0.0, 1.0)) for _ in range(300)]
    _feed(
        c, metric="main_z", symbol="S", regime="trend", session="us",
        samples=s2, base_ms=last + 1, step_ms=100,
    )
    lo2, hi2 = c.bounds("main_z", symbol="S", regime="trend", session="us")
    assert (lo2, hi2) == (lo1, hi1)


def test_jump_limit_caps_extreme_swings() -> None:
    """Jump-limit caps EACH committed update at prev × max_jump_mult.

    Throttle recompute to fire exactly twice: once after the calm batch,
    once after the wild batch — second commit must be capped.
    """
    GAP = 1_000_000  # 1000s: only one recompute per batch
    c = ZMappingCalibrator(
        enforce=True,
        min_samples=50,
        hold_ms=0,
        recompute_gap_ms=GAP,
        rel_thresh=0.0,
        max_jump_mult=2.0,
    )
    rng = random.Random(1)
    # Batch 1: |z| ~ |N(0, 0.5)| — all packed at the same ts so only the
    # first observation triggers a recompute (gap is huge).
    for v in [abs(rng.gauss(0.0, 0.5)) for _ in range(500)]:
        c.observe("main_z", symbol="X", regime="trend", session="us",
                  z_abs=v, now_ms=1_000)
    lo1, hi1 = c.bounds("main_z", symbol="X", regime="trend", session="us")
    assert lo1 > 0.0 and hi1 > lo1

    # Advance time past the gap, then feed one extreme batch.
    rng2 = random.Random(2)
    for v in [abs(rng2.gauss(0.0, 10.0)) for _ in range(2500)]:
        c.observe("main_z", symbol="X", regime="trend", session="us",
                  z_abs=v, now_ms=1_000 + GAP + 1)
    lo2, hi2 = c.bounds("main_z", symbol="X", regime="trend", session="us")
    # Single committed step is capped at × max_jump_mult.
    assert lo2 <= lo1 * 2.0 + 1e-9
    assert hi2 <= hi1 * 2.0 + 1e-9


# ---------------------------------------------------------------------------
# throttling
# ---------------------------------------------------------------------------


def test_recompute_gap_throttles_updates() -> None:
    c = ZMappingCalibrator(
        enforce=True,
        min_samples=50,
        hold_ms=0,
        recompute_gap_ms=5_000,  # 5s gap
        rel_thresh=0.0,
        max_jump_mult=10.0,
    )
    rng = random.Random(123)
    # All observations packed into 100ms window: only first recompute fires
    samples = [abs(rng.gauss(0.0, 1.0)) for _ in range(300)]
    _feed(
        c, metric="main_z", symbol="T", regime="trend", session="us",
        samples=samples, base_ms=1_000, step_ms=0,  # no time advance
    )
    bin_key = ("main_z", "T", "trend", "us")
    b = c.bins[bin_key]
    # Recompute throttle prevents more than one apply within recompute_gap_ms.
    # last_apply_ms must equal last_recompute_ms.
    assert b.last_apply_ms == b.last_recompute_ms
    # And no later than the first sample's ts (no time advanced).
    assert b.last_recompute_ms == 1_000


# ---------------------------------------------------------------------------
# fallback hierarchy
# ---------------------------------------------------------------------------


def test_fallback_hierarchy_uses_parent_bin() -> None:
    """When (symbol × regime × session) bin is cold, fall back to parents."""
    c = ZMappingCalibrator(
        enforce=True, min_samples=50, hold_ms=0, recompute_gap_ms=0,
        rel_thresh=0.0, max_jump_mult=10.0,
    )
    rng = random.Random(11)
    # Feed enough samples → fills aggregated parent bins automatically.
    _feed(
        c, metric="main_z", symbol="BTCUSDT", regime="trend", session="us",
        samples=[abs(rng.gauss(0.0, 1.5)) for _ in range(400)],
    )
    # Now query a cold finer key (different session) — should fall back to
    # (symbol, regime, "*") which observe() populated.
    lo, hi = c.bounds("main_z", symbol="BTCUSDT", regime="trend", session="asia")
    assert lo > 0.0 and hi > lo


# ---------------------------------------------------------------------------
# input validation
# ---------------------------------------------------------------------------


def test_observe_rejects_invalid_inputs() -> None:
    c = ZMappingCalibrator(enforce=True, min_samples=10, hold_ms=0, recompute_gap_ms=0)
    c.observe("main_z", symbol="X", regime="trend", session="us",
              z_abs=float("nan"), now_ms=1)
    c.observe("main_z", symbol="X", regime="trend", session="us",
              z_abs=float("inf"), now_ms=2)
    c.observe("main_z", symbol="X", regime="trend", session="us",
              z_abs=-1.0, now_ms=3)
    # Unknown metric — silently ignored
    c.observe("unknown", symbol="X", regime="trend", session="us",
              z_abs=1.0, now_ms=4)
    assert c.bins == {}


# ---------------------------------------------------------------------------
# snapshot / load_state round-trip
# ---------------------------------------------------------------------------


def test_snapshot_load_state_roundtrip() -> None:
    c1 = ZMappingCalibrator(
        enforce=True, min_samples=50, hold_ms=0, recompute_gap_ms=0,
        rel_thresh=0.0, max_jump_mult=10.0,
    )
    rng = random.Random(42)
    _feed(
        c1, metric="main_z", symbol="BTCUSDT", regime="trend", session="us",
        samples=[abs(rng.gauss(0.0, 1.0)) for _ in range(300)],
    )
    snap = c1.snapshot()
    # Must JSON-serializable.
    raw = json.dumps(snap)
    parsed = json.loads(raw)

    c2 = ZMappingCalibrator(enforce=False)  # different default
    c2.load_state(parsed)
    assert c2.enforce is True
    lo1, hi1 = c1.bounds("main_z", symbol="BTCUSDT", regime="trend", session="us")
    lo2, hi2 = c2.bounds("main_z", symbol="BTCUSDT", regime="trend", session="us")
    assert abs(lo2 - lo1) < 1e-9
    assert abs(hi2 - hi1) < 1e-9


def test_load_state_skips_malformed_rows() -> None:
    c = ZMappingCalibrator(enforce=False)
    bad = {
        "enforce": True,
        "bins": [
            {"metric": "main_z", "symbol": "X", "regime": "trend",
             "session": "us", "lo": 1.5, "hi": 4.5},
            {"metric": "bogus", "symbol": "Y"},   # unknown metric → skip
            {"no_metric_field": True},            # malformed → skip
        ],
    }
    c.load_state(bad)
    assert c.enforce is True
    lo, hi = c.bounds("main_z", symbol="X", regime="trend", session="us")
    assert (lo, hi) == (1.5, 4.5)


# ---------------------------------------------------------------------------
# integration with confidence_scorer
# ---------------------------------------------------------------------------


def test_confidence_scorer_reads_calibrated_bounds() -> None:
    """Verify that confidence_scorer.py picks up calibrated z_core_lo/hi
    and obi_z_lo/hi from ctx attributes (the wiring contract)."""
    from handlers.crypto_orderflow.scoring.confidence_scorer import _crypto_conf_factor

    class Ctx:
        def __init__(self) -> None:
            # Make most signals neutral
            self.main_z = 2.0
            self.obi_z = 1.0
            self.atr_q_main = 0.5
            self.spread_bps = 3.0
            self.range_vs_atr = 1.0
            self.market_regime = "trend"
            self.market_mode = "momentum"
            self.evidence = {}
            self.confirmations = []

    # Baseline with default bounds → z_core = (2.0-1.0)/(4.0-1.0) ≈ 0.333
    ctx = Ctx()
    _, parts_default = _crypto_conf_factor(ctx, "breakout", side="LONG")
    assert parts_default is not None
    assert abs(parts_default["z_core"] - (1.0 / 3.0)) < 1e-6

    # Inject tighter calibrated bounds via ctx attrs → z_core should rise
    ctx2 = Ctx()
    ctx2.z_core_lo = 0.5  # type: ignore[attr-defined]
    ctx2.z_core_hi = 2.5  # type: ignore[attr-defined]
    _, parts_calib = _crypto_conf_factor(ctx2, "breakout", side="LONG")
    assert parts_calib is not None
    assert abs(parts_calib["z_core"] - ((2.0 - 0.5) / (2.5 - 0.5))) < 1e-6
    assert parts_calib["z_core_lo"] == 0.5
    assert parts_calib["z_core_hi"] == 2.5

    # Inverted/bad bounds → safe fallback to defaults
    ctx3 = Ctx()
    ctx3.z_core_lo = 5.0  # type: ignore[attr-defined]
    ctx3.z_core_hi = 1.0  # type: ignore[attr-defined]
    _, parts_bad = _crypto_conf_factor(ctx3, "breakout", side="LONG")
    assert parts_bad is not None
    assert abs(parts_bad["z_core"] - (1.0 / 3.0)) < 1e-6


def test_confidence_scorer_obi_z_bounds_calibrated() -> None:
    from handlers.crypto_orderflow.scoring.confidence_scorer import _crypto_conf_factor

    class Ctx:
        def __init__(self) -> None:
            self.main_z = 1.5
            self.obi_z = 1.0
            self.atr_q_main = 0.5
            self.spread_bps = 3.0
            self.range_vs_atr = 1.0
            self.market_regime = "trend"
            self.market_mode = "momentum"
            self.evidence = {}
            self.confirmations = []
            # No obi_windowLevels → falls through to obi_z branch

    ctx = Ctx()
    _, parts_default = _crypto_conf_factor(ctx, "breakout", side="LONG")
    assert parts_default is not None
    # Default: (1.0-0.5)/(2.5-0.5) = 0.25
    assert abs(parts_default["obi_persist"] - 0.25) < 1e-6

    ctx2 = Ctx()
    ctx2.obi_z_lo = 0.2  # type: ignore[attr-defined]
    ctx2.obi_z_hi = 1.2  # type: ignore[attr-defined]
    _, parts_calib = _crypto_conf_factor(ctx2, "breakout", side="LONG")
    assert parts_calib is not None
    # Calibrated: (1.0-0.2)/(1.2-0.2) = 0.8
    assert abs(parts_calib["obi_persist"] - 0.8) < 1e-6
    assert parts_calib["obi_z_lo"] == 0.2
    assert parts_calib["obi_z_hi"] == 1.2


# ---------------------------------------------------------------------------
# OLD scorer (services/signal_confidence.py) adaptive bounds integration
# ---------------------------------------------------------------------------


def _make_ctx(**kwargs):
    """SimpleNamespace mimic used by signal_confidence._score_rule_based."""
    defaults = {
        "delta_z": 3.5, "z_delta": 3.5,
        "obi_avg": 0.3, "obi_sustained": True,
        "market_mode": "momentum",
        "confirmations": [],
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_signal_confidence_uses_calibrated_z_bounds_custom() -> None:
    """Calibrated (lo, hi) via ctx replaces fixed main_z_thr in 'custom' kind."""
    scorer = ConfidenceScorer(main_z_thr=3.0)

    # Default: _ramp(3.5, 3.0, 4.8) = (3.5-3.0)/(4.8-3.0) ≈ 0.278
    ctx_default = _make_ctx()
    conf_default, parts_default = scorer._score_rule_based(kind="custom", side="LONG", ctx=ctx_default)
    assert 0 < conf_default < 1
    assert "z_calib_lo" not in parts_default

    # Inject narrow calibrated range so s_z jumps to 1.0
    ctx_calib = _make_ctx(z_core_lo=1.0, z_core_hi=2.0)  # z_abs=3.5 > hi → s_z=1.0
    _, parts_calib = scorer._score_rule_based(kind="custom", side="LONG", ctx=ctx_calib)
    assert parts_calib["z_calib_lo"] == 1.0
    assert parts_calib["z_calib_hi"] == 2.0
    assert parts_calib["s_z"] == 1.0

    # Default s_z with z_abs=3.5 < 4.8 saturation → s_z < 1.0
    assert parts_default["s_z"] < 1.0


def test_signal_confidence_calibrated_bounds_bad_values_fallback() -> None:
    """Inverted or zero calibrated bounds fall back to scorer defaults."""
    scorer = ConfidenceScorer(main_z_thr=3.0)
    z_abs_val = 3.5

    # Reference: default bounds
    ctx_ref = _make_ctx(delta_z=z_abs_val, z_delta=z_abs_val)
    _, ref_parts = scorer._score_rule_based(kind="custom", side="LONG", ctx=ctx_ref)

    # Inverted bounds → fallback
    ctx_inv = _make_ctx(delta_z=z_abs_val, z_delta=z_abs_val, z_core_lo=5.0, z_core_hi=2.0)
    _, inv_parts = scorer._score_rule_based(kind="custom", side="LONG", ctx=ctx_inv)
    assert "z_calib_lo" not in inv_parts
    assert abs(inv_parts["s_z"] - ref_parts["s_z"]) < 1e-9

    # Zero lo → fallback
    ctx_zero = _make_ctx(delta_z=z_abs_val, z_delta=z_abs_val, z_core_lo=0.0, z_core_hi=3.0)
    _, zero_parts = scorer._score_rule_based(kind="custom", side="LONG", ctx=ctx_zero)
    assert "z_calib_lo" not in zero_parts
    assert abs(zero_parts["s_z"] - ref_parts["s_z"]) < 1e-9


def test_signal_confidence_calibrated_bounds_breakout_kind() -> None:
    """Calibrated bounds are applied in 'breakout' kind too."""
    scorer = ConfidenceScorer(breakout_z_thr=3.5)
    ctx = _make_ctx(
        delta_z=4.0, z_delta=4.0,
        obi_avg_20=0.3, obi_sustained_20=True,
        microprice_shift_bps_20=0.5,
        depletion_score=0.3, refill_score=0.1,
        z_core_lo=2.0, z_core_hi=3.5,  # z_abs=4.0 > hi → s_z=1.0
    )
    _, parts = scorer._score_rule_based(kind="breakout", side="LONG", ctx=ctx)
    assert parts["s_z"] == 1.0
    assert parts["z_calib_lo"] == 2.0


def test_signal_confidence_calibrated_bounds_extreme_kind() -> None:
    """Calibrated bounds are applied in 'extreme' kind."""
    scorer = ConfidenceScorer(extreme_z_thr=5.0)
    ctx = _make_ctx(
        delta_z=3.0, z_delta=3.0,
        obi_avg=0.4, obi_sustained=True,
        z_core_lo=1.0, z_core_hi=2.5,  # z_abs=3.0 > hi → s_z=1.0
    )
    _, parts = scorer._score_rule_based(kind="extreme", side="LONG", ctx=ctx)
    assert parts["s_z"] == 1.0
    assert parts["z_calib_lo"] == 1.0


def test_z_mapping_calibrator_end_to_end_old_scorer() -> None:
    """Full E2E: ZMappingCalibrator.bounds() → ctx injection → old scorer uses them."""
    calib = ZMappingCalibrator(
        q_lo=0.10,   # force very low lo
        q_hi=0.50,   # force moderate hi
        min_samples=10,
        enforce=True,
        hold_ms=0,
        recompute_gap_ms=0,
    )
    # Feed 30 samples uniformly in [1.0, 5.0]
    samples = [1.0 + 4.0 * i / 29 for i in range(30)]
    for i, s in enumerate(samples):
        calib.observe("main_z", symbol="BTCUSDT", regime="trend", session="us",
                      z_abs=s, now_ms=i * 100)

    lo, hi = calib.bounds("main_z", symbol="BTCUSDT", regime="trend", session="us")
    assert lo > 0.0 and hi > lo, f"calib not warm: lo={lo}, hi={hi}"

    scorer = ConfidenceScorer(main_z_thr=3.0)
    ctx = _make_ctx(delta_z=4.5, z_delta=4.5, z_core_lo=lo, z_core_hi=hi)
    _, parts = scorer._score_rule_based(kind="custom", side="LONG", ctx=ctx)

    assert parts["z_calib_lo"] == lo
    assert parts["z_calib_hi"] == hi
    expected_s_z = min(1.0, max(0.0, (4.5 - lo) / (hi - lo))) if hi > lo else 0.0
    assert abs(parts["s_z"] - expected_s_z) < 1e-9
