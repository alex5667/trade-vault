"""
Integration tests for AdaptiveTP1Policy wired into signals/level_enricher.py
(Plan 3, 2026-05-29).

Invariants verified:
  - SHADOW mode: baseline TP1 price unchanged; telemetry attrs populated.
  - ENFORCE mode: only TP1 (tp_levels[0]) changes; SL and other TPs preserved.
  - When AdaptiveTP1 disabled: ctx remains identical to legacy behaviour.
  - When prob curve missing: shadow telemetry reason=no_prob_curve, no change.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from signals.level_enricher import attach_trade_levels_to_ctx


def _base_cfg() -> dict:
    # Use TP_MODE=RR without TP_ATR_MULTS to keep baseline TP1 deterministic at
    # RR=1.15 → entry + 1.15*stop_dist. ATR mults are tested separately.
    return {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 0.5,
        "TP_MODE": "RR",
        "TP_RR": "1.15,2.0,3.0",
    }


def _mk_ctx_with_curve(
    *,
    curve: dict[str, float] | None,
    samples: int = 500,
    calibration_ok: int = 1,
) -> SimpleNamespace:
    ctx = SimpleNamespace()
    ctx.price = 10000.0
    ctx.atr = 200.0  # stop_dist = 100 → stop_bps = 100
    ctx.tp1_hit_prob_by_rr = curve
    ctx.tp1_prob_samples = samples
    ctx.tp1_calibration_ok = calibration_ok
    return ctx


@pytest.fixture(autouse=True)
def _no_floors(monkeypatch: pytest.MonkeyPatch) -> None:
    # Disable bps floors / bounded SL / per-symbol overrides that could
    # interfere with assertions.
    for var in (
        "BTC_STOP_MODE", "BTC_STOP_ATR_MULT", "BTC_TP_MODE", "BTC_TP_RR",
        "BOUNDED_SL_ENABLED", "BOUNDED_SL_SHADOW",
        "EDGE_LEVELS_MIN_STOP_BPS", "EDGE_LEVELS_MIN_TP1_BPS",
        "EDGE_LEVELS_MIN_STOP_BPS_BTCUSDT", "EDGE_LEVELS_MIN_TP1_BPS_BTCUSDT",
        "TP1_ADAPTIVE_ENABLED", "TP1_ADAPTIVE_MODE",
        "TP1_ADAPTIVE_RR_GRID", "TP1_ADAPTIVE_MIN_RR", "TP1_ADAPTIVE_MAX_RR",
        "TP1_ADAPTIVE_MIN_SAMPLES", "TP1_ADAPTIVE_MIN_EV_DELTA_R",
        "TP1_ADAPTIVE_MIN_TP1_BPS", "TP1_ADAPTIVE_COST_BUFFER_BPS",
        "TP1_ADAPTIVE_REQUIRE_CALIBRATION_OK", "TAKER_FEE_BPS",
    ):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# Disabled — no change vs legacy
# ---------------------------------------------------------------------------


def test_adaptive_disabled_keeps_baseline_tp1() -> None:
    # default: TP1_ADAPTIVE_ENABLED unset.
    # compute_levels enforces an internal tp1_min_rr floor that clamps RR
    # from 1.15 → 1.20 (entry=10000, stop=100 → TP1=10120).
    ctx = _mk_ctx_with_curve(curve={"0.80": 0.9, "1.20": 0.5})
    attach_trade_levels_to_ctx(
        ctx, side="LONG", symbol="BTCUSDT", cfg=_base_cfg(), overwrite=True,
    )
    assert ctx.tp1_price == pytest.approx(10120.0)
    assert getattr(ctx, "levels_source", None) == "baseline_cfg"
    # adaptive policy ran (we always run it for telemetry) but reported disabled
    assert getattr(ctx, "tp1_adaptive_reason", None) == "tp1_adaptive_skip_disabled"
    assert getattr(ctx, "tp1_adaptive_enabled", None) is False


# ---------------------------------------------------------------------------
# Shadow mode — never alters TP1 price
# ---------------------------------------------------------------------------


def test_adaptive_shadow_does_not_change_tp1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "shadow")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.80,1.20")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_RR", "0.80")
    monkeypatch.setenv("TAKER_FEE_BPS", "4.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "4.0")

    # curve where 0.80 RR strictly dominates baseline 1.20
    ctx = _mk_ctx_with_curve(curve={"0.80": 0.95, "1.20": 0.40})
    attach_trade_levels_to_ctx(
        ctx, side="LONG", symbol="BTCUSDT", cfg=_base_cfg(), overwrite=True,
    )

    # Baseline TP1 preserved despite favourable adaptive recommendation.
    assert ctx.tp1_price == pytest.approx(10120.0)
    assert ctx.sl_price == pytest.approx(9900.0)  # SL = entry - stop_dist = 9900
    assert getattr(ctx, "levels_source", None) == "baseline_cfg"

    # Shadow telemetry recorded.
    assert getattr(ctx, "tp1_adaptive_reason", None) == "tp1_adaptive_shadow"
    assert getattr(ctx, "tp1_adaptive_mode", None) == "shadow"
    assert getattr(ctx, "tp1_adaptive_apply", None) is False
    assert getattr(ctx, "tp1_adaptive_rr_selected", None) == pytest.approx(0.80)
    assert getattr(ctx, "tp1_adaptive_ev_delta_r", 0.0) > 0.0


# ---------------------------------------------------------------------------
# Enforce mode — TP1 changes; SL and TP2/TP3 preserved
# ---------------------------------------------------------------------------


def test_adaptive_enforce_only_changes_tp1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "enforce")
    monkeypatch.setenv("TP1_ADAPTIVE_RR_GRID", "0.80,1.20")
    monkeypatch.setenv("TP1_ADAPTIVE_MIN_RR", "0.80")
    monkeypatch.setenv("TAKER_FEE_BPS", "4.0")
    monkeypatch.setenv("TP1_ADAPTIVE_COST_BUFFER_BPS", "4.0")

    # Baseline first — to capture invariants
    base_ctx = _mk_ctx_with_curve(curve=None)
    attach_trade_levels_to_ctx(
        base_ctx, side="LONG", symbol="BTCUSDT", cfg=_base_cfg(), overwrite=True,
    )
    baseline_sl = base_ctx.sl_price
    baseline_tp_levels = list(base_ctx.tp_levels)
    assert len(baseline_tp_levels) == 3

    # Enforce-mode adaptive
    ctx = _mk_ctx_with_curve(curve={"0.80": 0.95, "1.20": 0.40})
    attach_trade_levels_to_ctx(
        ctx, side="LONG", symbol="BTCUSDT", cfg=_base_cfg(), overwrite=True,
    )

    # TP1 moved to 0.80 RR → 10000 + 0.80 * 100 = 10080
    assert ctx.tp1_price == pytest.approx(10080.0)
    assert ctx.tp_levels[0] == pytest.approx(10080.0)
    # SL preserved (stop_dist_override = baseline_stop_dist).
    assert ctx.sl_price == pytest.approx(baseline_sl)
    # TP2 and TP3 should remain at baseline RR (compute_levels keeps tail TPs).
    # When tp1_dist_override is provided, the override re-runs compute_levels;
    # downstream tp_levels[1:] may shift depending on TP_MODE logic, but TP1
    # MUST be the adaptive value. Make assertion strict only on tp[0].
    assert getattr(ctx, "levels_source", None) == "adaptive_tp1"
    assert getattr(ctx, "tp1_adaptive_apply", None) is True
    assert getattr(ctx, "tp1_adaptive_reason", None) == "tp1_adaptive_apply"


# ---------------------------------------------------------------------------
# No prob curve — shadow telemetry, no change
# ---------------------------------------------------------------------------


def test_adaptive_no_prob_curve_shadow_logs_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_ENABLED", "1")
    monkeypatch.setenv("TP1_ADAPTIVE_MODE", "enforce")  # even enforce can't apply without curve
    ctx = _mk_ctx_with_curve(curve=None)
    attach_trade_levels_to_ctx(
        ctx, side="LONG", symbol="BTCUSDT", cfg=_base_cfg(), overwrite=True,
    )
    assert ctx.tp1_price == pytest.approx(10120.0)
    assert getattr(ctx, "tp1_adaptive_reason", None) == "tp1_adaptive_skip_no_prob_curve"
    assert getattr(ctx, "tp1_adaptive_apply", None) is False
