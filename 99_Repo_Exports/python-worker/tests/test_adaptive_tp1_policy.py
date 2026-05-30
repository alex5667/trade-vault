"""
Unit tests for core/adaptive_tp1_policy.py (Plan 3, 2026-05-29).

Coverage:
  - disabled → tp1_adaptive_skip_disabled
  - missing prob curve → tp1_adaptive_skip_no_prob_curve
  - low samples → tp1_adaptive_skip_low_samples
  - uncalibrated → tp1_adaptive_skip_uncalibrated
  - low EV delta → tp1_adaptive_skip_low_ev_delta
  - tiny TP1 floor enforced
  - clamp min/max RR
  - shadow mode never sets apply=True
  - enforce mode sets apply=True when EV delta passes
  - ev formula correctness
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.adaptive_tp1_policy import (
    AdaptiveTP1Decision,
    choose_adaptive_tp1,
    ev_full_exit_r,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mk_ctx(
    *,
    curve: dict[str, float] | None = None,
    samples: int = 500,
    calibration_ok: int = 1,
    spread_bps: float = 0.0,
    slippage_ema_bps: float = 0.0,
) -> SimpleNamespace:
    return SimpleNamespace(
        tp1_hit_prob_by_rr=curve,
        tp1_prob_samples=samples,
        tp1_calibration_ok=calibration_ok,
        spread_bps=spread_bps,
        slippage_ema_bps=slippage_ema_bps,
    )


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Clear all TP1_ADAPTIVE_* env vars between tests so each test is hermetic.
    for k in list(__import__("os").environ.keys()):
        if k.startswith("TP1_ADAPTIVE_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("TAKER_FEE_BPS", raising=False)


# ---------------------------------------------------------------------------
# Disabled / smoke
# ---------------------------------------------------------------------------


def test_disabled_by_default() -> None:
    ctx = _mk_ctx(curve={"1.00": 0.5})
    d = choose_adaptive_tp1(
        ctx=ctx, entry=100.0, stop_dist=1.0, baseline_tp1_dist=1.0,
    )
    assert isinstance(d, AdaptiveTP1Decision)
    assert d.enabled is False
    assert d.apply is False
    assert d.reason == "tp1_adaptive_skip_disabled"


def test_skip_disabled_when_mode_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "off")
    d = choose_adaptive_tp1(
        ctx=_mk_ctx(curve={"1.00": 0.5}),
        entry=100.0, stop_dist=1.0, baseline_tp1_dist=1.0,
    )
    assert d.enabled is False
    assert d.reason == "tp1_adaptive_skip_disabled"


# ---------------------------------------------------------------------------
# Bad inputs
# ---------------------------------------------------------------------------


def test_skip_bad_levels(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    d = choose_adaptive_tp1(
        ctx=_mk_ctx(curve={"1.00": 0.5}),
        entry=0.0, stop_dist=1.0, baseline_tp1_dist=1.0,
    )
    assert d.reason == "tp1_adaptive_skip_bad_levels"
    assert d.apply is False


def test_skip_no_prob_curve(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    d = choose_adaptive_tp1(
        ctx=_mk_ctx(curve=None),
        entry=100.0, stop_dist=1.0, baseline_tp1_dist=1.0,
    )
    assert d.reason == "tp1_adaptive_skip_no_prob_curve"
    assert d.apply is False


def test_skip_low_samples(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_SAMPLES", "200")
    d = choose_adaptive_tp1(
        ctx=_mk_ctx(curve={"1.00": 0.5}, samples=50),
        entry=100.0, stop_dist=1.0, baseline_tp1_dist=1.0,
    )
    assert d.reason == "tp1_adaptive_skip_low_samples"
    assert d.samples == 50


def test_skip_uncalibrated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    monkeypatch.setenv("TP1_ADAPTIVE_REQUIRE_CALIBRATION_OK", "1")
    d = choose_adaptive_tp1(
        ctx=_mk_ctx(curve={"1.00": 0.5}, calibration_ok=0),
        entry=100.0, stop_dist=1.0, baseline_tp1_dist=1.0,
    )
    assert d.reason == "tp1_adaptive_skip_uncalibrated"


# ---------------------------------------------------------------------------
# EV math correctness
# ---------------------------------------------------------------------------


def test_ev_formula_winner_minus_loser_minus_cost() -> None:
    # p=0.5, tp_rr=1, cost=0 → 0.5*1 - 0.5*1 - 0 = 0
    assert ev_full_exit_r(p_hit=0.5, tp_rr=1.0, cost_r=0.0) == pytest.approx(0.0)
    # p=0.8, tp_rr=1.15, cost=0.08 → 0.92-0.20-0.08 = 0.64
    assert ev_full_exit_r(p_hit=0.8, tp_rr=1.15, cost_r=0.08) == pytest.approx(0.64, abs=1e-9)
    # extreme clamps
    assert ev_full_exit_r(p_hit=-1.0, tp_rr=1.0, cost_r=0.0) == pytest.approx(-1.0)
    assert ev_full_exit_r(p_hit=2.0, tp_rr=1.0, cost_r=0.0) == pytest.approx(1.0)


def test_skip_low_ev_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    """Baseline TP1_R=1.15 with p=0.80 vs candidate 0.70 with p=0.90:
    EV_base = 0.80*1.15 - 0.20*1 - 0.08 = 0.64
    EV_adapt = 0.90*0.70 - 0.10*1 - 0.08 = 0.45
    → adaptive is worse, skip_low_ev_delta.
    """
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.70,1.15")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_EV_DELTA_R", "0.05")
    # set costs so cost_r = 0.08 in R: stop_bps=100, cost_bps=8
    # entry=10000, stop_dist=100 → stop_bps = 100
    # fee=4, spread=0, slip=0, buffer=4 → cost_bps=8, cost_r=0.08
    monkeypatch.setenv("TAKER_FEE_BPS", "4.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "4.0")
    ctx = _mk_ctx(curve={"0.70": 0.90, "1.15": 0.80})
    d = choose_adaptive_tp1(
        ctx=ctx, entry=10000.0, stop_dist=100.0, baseline_tp1_dist=115.0,
    )
    assert d.reason == "tp1_adaptive_skip_low_ev_delta"
    assert d.apply is False
    assert d.ev_baseline_r == pytest.approx(0.64, abs=1e-9)


def test_adaptive_apply_when_ev_better(monkeypatch: pytest.MonkeyPatch) -> None:
    """Baseline TP1_R=1.15 with p=0.50 vs candidate 0.80 with p=0.90:
    EV_base = 0.50*1.15 - 0.50*1 - 0.08 = -0.005
    EV_adapt = 0.90*0.80 - 0.10*1 - 0.08 = 0.54
    → adaptive wins, apply=True (in enforce mode).
    """
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "enforce")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.80,1.15")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_RR", "0.80")
    monkeypatch.setenv("TAKER_FEE_BPS", "4.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "4.0")
    ctx = _mk_ctx(curve={"0.80": 0.90, "1.15": 0.50})
    d = choose_adaptive_tp1(
        ctx=ctx, entry=10000.0, stop_dist=100.0, baseline_tp1_dist=115.0,
    )
    assert d.apply is True
    assert d.reason == "tp1_adaptive_apply"
    assert d.tp1_rr == pytest.approx(0.80)
    assert d.tp1_dist == pytest.approx(80.0)
    assert d.ev_delta_r > 0.5


def test_shadow_mode_does_not_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    """Even when EV is strictly better, shadow mode never sets apply=True."""
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.80,1.15")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_RR", "0.80")
    monkeypatch.setenv("TAKER_FEE_BPS", "4.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "4.0")
    ctx = _mk_ctx(curve={"0.80": 0.90, "1.15": 0.50})
    d = choose_adaptive_tp1(
        ctx=ctx, entry=10000.0, stop_dist=100.0, baseline_tp1_dist=115.0,
    )
    assert d.apply is False
    assert d.reason == "tp1_adaptive_shadow"
    assert d.tp1_dist == pytest.approx(80.0)
    assert d.tp1_rr == pytest.approx(0.80)
    # telemetry attrs still populated:
    assert d.ev_delta_r > 0.0


# ---------------------------------------------------------------------------
# Floors and clamps
# ---------------------------------------------------------------------------


def test_clamp_min_rr(monkeypatch: pytest.MonkeyPatch) -> None:
    """RR=0.50 in grid clamped up to 0.80, evaluated at that key, reason=clamped."""
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "enforce")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.50,1.15")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_RR", "0.80")
    monkeypatch.setenv("TAKER_FEE_BPS", "4.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "4.0")
    ctx = _mk_ctx(curve={"0.80": 0.95, "1.15": 0.50})
    d = choose_adaptive_tp1(
        ctx=ctx, entry=10000.0, stop_dist=100.0, baseline_tp1_dist=115.0,
    )
    assert d.apply is True
    assert d.reason == "tp1_adaptive_clamped_min_rr"
    assert d.tp1_rr == pytest.approx(0.80)


def test_tiny_tp1_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    """If stop_bps * best_rr < min_tp1_bps → skip_tiny_tp1.
    stop_bps = 1.0 (entry=10000, stop_dist=1) → 0.80 RR gives 0.80 bps < 8 → skip.
    """
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "enforce")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.80,1.15")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_RR", "0.80")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_TP1_BPS", "8.0")
    monkeypatch.setenv("TAKER_FEE_BPS", "0.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "0.0")
    ctx = _mk_ctx(curve={"0.80": 0.95, "1.15": 0.50})
    d = choose_adaptive_tp1(
        ctx=ctx, entry=10000.0, stop_dist=1.0, baseline_tp1_dist=1.15,
    )
    # baseline tiny too → grid filtered → adaptive picks nothing; reason: low_ev_delta
    assert d.apply is False
    assert d.reason in {"tp1_adaptive_skip_tiny_tp1", "tp1_adaptive_skip_low_ev_delta"}


def test_baseline_outside_grid_uses_fallback_prob(monkeypatch: pytest.MonkeyPatch) -> None:
    """When baseline RR has no matching grid entry, fallback ctx.tp1_hit_prob used."""
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.80,1.00")
    monkeypatch.setenv("TP1_ADAPTIVE_MAX_RR", "1.50")
    monkeypatch.setenv("TAKER_FEE_BPS", "0.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "0.0")
    ctx = _mk_ctx(curve={"0.80": 0.90, "1.00": 0.85})
    # baseline_rr = 5.0 → out-of-curve → use fallback
    ctx.tp1_hit_prob = 0.40
    d = choose_adaptive_tp1(
        ctx=ctx, entry=10000.0, stop_dist=10.0, baseline_tp1_dist=50.0,
    )
    # baseline EV = 0.40 * 5.0 - 0.60 * 1.0 - 0 = 1.40
    # adaptive 0.80 EV = 0.90*0.80 - 0.10 = 0.62 → worse
    # adaptive 1.00 EV = 0.85*1.00 - 0.15 = 0.70 → worse
    assert d.reason == "tp1_adaptive_skip_low_ev_delta"
    assert d.ev_baseline_r == pytest.approx(1.40, abs=1e-9)


# ---------------------------------------------------------------------------
# Telemetry shape
# ---------------------------------------------------------------------------


def test_telemetry_fields_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.80,1.15")
    monkeypatch.setenv("TAKER_FEE_BPS", "4.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "4.0")
    ctx = _mk_ctx(curve={"0.80": 0.90, "1.15": 0.50}, spread_bps=2.0, slippage_ema_bps=1.0)
    d = choose_adaptive_tp1(
        ctx=ctx, entry=10000.0, stop_dist=100.0, baseline_tp1_dist=115.0,
        symbol="BTCUSDT", kind="of", regime="trending_calm",
    )
    assert d.enabled is True
    assert d.mode == "shadow"
    assert d.baseline_rr == pytest.approx(1.15)
    assert d.cost_r > 0.0
    assert d.samples == 500
    assert d.grid_evaluated  # non-empty
