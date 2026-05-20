from __future__ import annotations

import json
import random

from core.p_edge_threshold_calibrator import (
    DEFAULT_P_MIN,
    TAU_CEIL,
    TAU_FLOOR,
    PEdgeThresholdCalibrator,
    _default_tau_grid,
    _quantile,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _feed(
    c: PEdgeThresholdCalibrator,
    *,
    symbol: str,
    regime: str,
    kind: str,
    samples: list[tuple[float, float, str]],
    base_ms: int = 1_000_000,
    step_ms: int = 1_000,
) -> int:
    """Feed (p_edge, r_multiple, result) tuples; returns last ts."""
    t = base_ms
    for p, r, res in samples:
        c.observe(
            symbol=symbol,
            regime=regime,
            kind=kind,
            p_edge=p,
            r_multiple=r,
            result=res,
            ts_ms=t,
        )
        t += step_ms
    return t


def _mk_outcomes(
    *,
    n: int,
    win_rate_above: float,
    win_rate_below: float,
    cut: float,
    rng: random.Random,
    r_win: float = 1.5,
    r_loss: float = -1.0,
) -> list[tuple[float, float, str]]:
    """Synthesize closed-trade outcomes where p_edge linearly drives win rate.

    Trades with p_edge ≥ cut win at `win_rate_above`; below at `win_rate_below`.
    """
    out: list[tuple[float, float, str]] = []
    for _ in range(n):
        p = rng.uniform(0.30, 0.85)
        wr = win_rate_above if p >= cut else win_rate_below
        if rng.random() < wr:
            out.append((p, r_win, "WIN"))
        else:
            out.append((p, r_loss, "LOSS"))
    return out


# ---------------------------------------------------------------------------
# primitives
# ---------------------------------------------------------------------------


def test_default_tau_grid_shape() -> None:
    g = _default_tau_grid()
    assert g[0] == TAU_FLOOR
    assert abs(g[-1] - TAU_CEIL) < 1e-9
    assert len(g) == 21
    # Strict monotonicity, ~constant step.
    for a, b in zip(g[:-1], g[1:]):
        assert b > a
        assert abs((b - a) - 0.02) < 1e-9


def test_quantile_basic() -> None:
    xs = [float(i) for i in range(1, 101)]
    assert _quantile(xs, 0.0) == 1.0
    assert abs(_quantile(xs, 0.5) - 50.5) < 1e-6
    assert abs(_quantile(xs, 0.9) - 90.1) < 1e-6
    assert _quantile([], 0.5) == 0.0
    assert _quantile([7.0], 0.99) == 7.0


# ---------------------------------------------------------------------------
# cold start & shadow mode
# ---------------------------------------------------------------------------


def test_returns_default_before_warmup() -> None:
    c = PEdgeThresholdCalibrator(enforce=True)
    assert c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout") == DEFAULT_P_MIN


def test_shadow_mode_does_not_apply() -> None:
    c = PEdgeThresholdCalibrator(
        enforce=False,
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
    )
    rng = random.Random(1)
    s = _mk_outcomes(n=400, win_rate_above=0.85, win_rate_below=0.30, cut=0.55, rng=rng)
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout", samples=s)

    # enforce=False → committed cutoff is still the default
    assert c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout") == DEFAULT_P_MIN
    # but a shadow proposal exists and is sensible
    sp = c.shadow_p_min(symbol="BTCUSDT", regime="trend", kind="breakout")
    assert TAU_FLOOR <= sp <= TAU_CEIL
    assert sp > 0.0


# ---------------------------------------------------------------------------
# observe input validation
# ---------------------------------------------------------------------------


def test_observe_rejects_invalid_inputs() -> None:
    c = PEdgeThresholdCalibrator(enforce=True)
    # NaN/Inf
    c.observe(symbol="X", regime="trend", kind="breakout",
              p_edge=float("nan"), r_multiple=1.0, result="WIN", ts_ms=1)
    c.observe(symbol="X", regime="trend", kind="breakout",
              p_edge=0.6, r_multiple=float("inf"), result="WIN", ts_ms=2)
    # out-of-range p
    c.observe(symbol="X", regime="trend", kind="breakout",
              p_edge=-0.1, r_multiple=1.0, result="WIN", ts_ms=3)
    c.observe(symbol="X", regime="trend", kind="breakout",
              p_edge=1.1, r_multiple=1.0, result="WIN", ts_ms=4)
    # unknown result
    c.observe(symbol="X", regime="trend", kind="breakout",
              p_edge=0.6, r_multiple=1.0, result="MAYBE", ts_ms=5)
    assert c.bins == {}


def test_be_excluded_from_ev_math_but_counted() -> None:
    c = PEdgeThresholdCalibrator(
        enforce=True,
        min_total_trades=10,
        min_kept_trades=5,
        recompute_gap_ms=0,
        hold_ms=0,
    )
    # All BE trades → eligible (post-BE-filter) sample count = 0 → no commit.
    samples = [(0.7, 0.0, "BE")] * 100
    _feed(c, symbol="X", regime="trend", kind="breakout", samples=samples)
    assert c.p_min_for(symbol="X", regime="trend", kind="breakout") == DEFAULT_P_MIN
    # Bin still recorded the observations.
    assert c.bins[("X", "trend", "breakout", "*")].n_observed == 100


# ---------------------------------------------------------------------------
# EV-driven threshold selection
# ---------------------------------------------------------------------------


def test_picks_smallest_tau_meeting_target_ev() -> None:
    """When wins concentrate above p=0.55, the chosen τ should land in 0.44-0.56.

    With high target_ev_r=0.50 and strong below-cut loss rate, the grid will
    have to climb until enough sub-cut losers are excluded.
    """
    c = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=0.50,
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,            # disable jump-limit for this test
        conformal_min_losses=10_000, # disable conformal floor
    )
    rng = random.Random(42)
    s = _mk_outcomes(
        n=2000, win_rate_above=0.80, win_rate_below=0.05, cut=0.55, rng=rng,
        r_win=1.5, r_loss=-1.0,
    )
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout", samples=s)
    tau = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout")
    # EV above 0.55: 0.80*1.5 + 0.20*(-1.0) = +1.00R
    # EV below 0.55: 0.05*1.5 + 0.95*(-1.0) = -0.875R
    # Smallest τ where weighted mean clears 0.50R lands near 0.44-0.50.
    assert 0.42 <= tau <= 0.56


def test_low_quality_population_falls_back_to_default_or_cf() -> None:
    """If no τ in grid meets target_ev, calibrator must NOT commit a random value."""
    c = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=0.50,            # unrealistic — nothing will satisfy
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,
        conformal_min_losses=10_000, # disable cf-floor
    )
    rng = random.Random(7)
    # Garbage signal: win rate 50/50 everywhere → EV = 0.25R, < 0.50 target.
    s = _mk_outcomes(
        n=600, win_rate_above=0.50, win_rate_below=0.50, cut=0.55, rng=rng,
    )
    _feed(c, symbol="X", regime="trend", kind="breakout", samples=s)
    tau = c.p_min_for(symbol="X", regime="trend", kind="breakout")
    # Grid found nothing & cf disabled → fall back to default_p_min.
    assert abs(tau - DEFAULT_P_MIN) < 1e-9


def test_conformal_floor_raises_threshold_under_loss_clustering() -> None:
    """If many losses cluster at high p_edge, cf-floor should pull τ UP."""
    c = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=-10.0,           # trivially satisfied → grid picks τ_floor
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,
        conformal_alpha=0.10,        # τ_cf = q90 of LOSS-only p
        conformal_min_losses=30,
    )
    # Losses concentrated at p ∈ [0.55, 0.70]
    samples: list[tuple[float, float, str]] = []
    rng = random.Random(99)
    for _ in range(150):
        p = rng.uniform(0.55, 0.70)
        samples.append((p, -1.0, "LOSS"))
    for _ in range(50):
        p = rng.uniform(0.40, 0.55)
        samples.append((p, 1.5, "WIN"))
    rng.shuffle(samples)
    _feed(c, symbol="X", regime="trend", kind="breakout", samples=samples)
    tau = c.p_min_for(symbol="X", regime="trend", kind="breakout")
    # q90 of losses ∈ [0.55,0.70] ≈ 0.685 — calibrator must clip to ≥ 0.60-ish.
    assert tau > 0.55
    assert tau <= TAU_CEIL


def test_committed_tau_clipped_to_rails() -> None:
    c = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=-10.0,
        min_total_trades=10,
        min_kept_trades=5,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,
        conformal_alpha=0.001,       # q99.9
        conformal_min_losses=10,
    )
    # All losses at p≈0.95 → cf wants τ≈0.95, must be clipped to TAU_CEIL=0.80.
    samples = [(0.95, -1.0, "LOSS")] * 50
    _feed(c, symbol="X", regime="trend", kind="breakout", samples=samples)
    tau = c.p_min_for(symbol="X", regime="trend", kind="breakout")
    assert abs(tau - TAU_CEIL) < 1e-9


# ---------------------------------------------------------------------------
# hysteresis & jump-limit
# ---------------------------------------------------------------------------


def test_hysteresis_skips_small_changes() -> None:
    c = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=0.10,
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.10,             # require ≥0.10 abs to commit
        max_jump_abs=1.0,
        conformal_min_losses=10_000,
    )
    rng = random.Random(11)
    s1 = _mk_outcomes(
        n=600, win_rate_above=0.80, win_rate_below=0.25, cut=0.55, rng=rng,
    )
    last = _feed(c, symbol="S", regime="trend", kind="breakout", samples=s1)
    tau1 = c.p_min_for(symbol="S", regime="trend", kind="breakout")

    # Feed near-identical batch — should not move committed tau (within hysteresis).
    s2 = _mk_outcomes(
        n=600, win_rate_above=0.80, win_rate_below=0.25, cut=0.55, rng=rng,
    )
    _feed(c, symbol="S", regime="trend", kind="breakout",
          samples=s2, base_ms=last + 1, step_ms=1_000)
    tau2 = c.p_min_for(symbol="S", regime="trend", kind="breakout")
    assert abs(tau2 - tau1) < 1e-9


def test_jump_limit_caps_single_step() -> None:
    c = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=0.10,
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=0.03,           # cap each step at 0.03 abs
        conformal_min_losses=10_000,
    )
    # Batch 1: high-quality trades, cut at 0.45 → tau1 ≈ 0.42-0.46
    rng = random.Random(1)
    s1 = _mk_outcomes(
        n=600, win_rate_above=0.85, win_rate_below=0.20, cut=0.45, rng=rng,
    )
    last = _feed(c, symbol="X", regime="trend", kind="breakout", samples=s1)
    tau1 = c.p_min_for(symbol="X", regime="trend", kind="breakout")
    assert tau1 > 0.0

    # Batch 2: quality collapses above cut=0.75 → unconstrained τ would jump up.
    rng2 = random.Random(2)
    s2 = _mk_outcomes(
        n=600, win_rate_above=0.85, win_rate_below=0.20, cut=0.75, rng=rng2,
    )
    _feed(c, symbol="X", regime="trend", kind="breakout",
          samples=s2, base_ms=last + 1, step_ms=1_000)
    tau2 = c.p_min_for(symbol="X", regime="trend", kind="breakout")
    # Single committed step capped at +0.03.
    assert tau2 <= tau1 + 0.03 + 1e-9


# ---------------------------------------------------------------------------
# rolling window pruning
# ---------------------------------------------------------------------------


def test_window_prune_drops_stale_samples() -> None:
    c = PEdgeThresholdCalibrator(
        enforce=True,
        window_ms=10_000,            # 10s rolling window
        min_total_trades=10,
        min_kept_trades=5,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,
        conformal_min_losses=10_000,
    )
    # Old samples — bad signal at p ≥ 0.55.
    old = [(0.65, -1.0, "LOSS") for _ in range(80)] + [(0.45, 1.5, "WIN") for _ in range(20)]
    last_old = _feed(c, symbol="X", regime="trend", kind="breakout",
                     samples=old, base_ms=1_000, step_ms=10)
    # New samples after a long gap (well past window_ms) — flip signal.
    new = [(0.65, 1.5, "WIN") for _ in range(80)] + [(0.45, -1.0, "LOSS") for _ in range(20)]
    _feed(c, symbol="X", regime="trend", kind="breakout",
          samples=new, base_ms=last_old + 100_000, step_ms=10)
    # Buffer must contain only the new batch (old beyond window).
    b = c.bins[("X", "trend", "breakout", "*")]
    assert len(b.buf) == len(new)
    # And the committed τ must be ≤ 0.55 (good signal at low τ).
    tau = c.p_min_for(symbol="X", regime="trend", kind="breakout")
    assert tau <= 0.55 + 1e-9


# ---------------------------------------------------------------------------
# fallback hierarchy
# ---------------------------------------------------------------------------


def test_fallback_to_parent_when_finer_key_cold() -> None:
    c = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=0.10,
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,
        conformal_min_losses=10_000,
    )
    rng = random.Random(11)
    s = _mk_outcomes(
        n=600, win_rate_above=0.80, win_rate_below=0.25, cut=0.55, rng=rng,
    )
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout", samples=s)
    # observe() also populated (BTCUSDT, trend, "*") and ("*", trend, "*"), etc.
    # Query a cold finer key — must fall back to a parent and return ≠ default.
    tau = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="absorption_cold")
    assert tau != DEFAULT_P_MIN
    assert TAU_FLOOR <= tau <= TAU_CEIL


def test_unrelated_symbol_falls_through_to_default() -> None:
    c = PEdgeThresholdCalibrator(enforce=True)
    # No observations → no parent bins for ALTUSDT → default.
    assert c.p_min_for(symbol="ALTUSDT", regime="range", kind="breakout") == DEFAULT_P_MIN


# ---------------------------------------------------------------------------
# snapshot / load_state round-trip
# ---------------------------------------------------------------------------


def test_snapshot_load_state_roundtrip() -> None:
    c1 = PEdgeThresholdCalibrator(
        enforce=True,
        target_ev_r=0.10,
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,
        conformal_min_losses=10_000,
    )
    rng = random.Random(42)
    s = _mk_outcomes(
        n=600, win_rate_above=0.80, win_rate_below=0.25, cut=0.55, rng=rng,
    )
    _feed(c1, symbol="BTCUSDT", regime="trend", kind="breakout", samples=s)
    snap = c1.snapshot()
    raw = json.dumps(snap)
    parsed = json.loads(raw)

    c2 = PEdgeThresholdCalibrator(enforce=False)
    c2.load_state(parsed)
    assert c2.enforce is True
    tau1 = c1.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout")
    tau2 = c2.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout")
    assert abs(tau2 - tau1) < 1e-9


def test_load_state_skips_malformed_rows() -> None:
    c = PEdgeThresholdCalibrator(enforce=False)
    bad = {
        "enforce": True,
        "target_ev_r": 0.15,
        "bins": [
            {"symbol": "X", "regime": "trend", "kind": "breakout", "p_min": 0.62},
            {"no_symbol": True},                 # malformed → skip
            {"symbol": "Y", "regime": None, "p_min": "not-a-number"},  # → skip
        ],
    }
    c.load_state(bad)
    assert c.enforce is True
    assert abs(c.target_ev_r - 0.15) < 1e-9
    assert abs(c.p_min_for(symbol="X", regime="trend", kind="breakout") - 0.62) < 1e-9
