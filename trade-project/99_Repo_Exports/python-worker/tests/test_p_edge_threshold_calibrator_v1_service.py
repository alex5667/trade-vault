from __future__ import annotations

"""Unit-level tests for orderflow_services.p_edge_threshold_calibrator_v1.

The XREADGROUP main loop is not exercised here — those are integration tests
covered by the live service. We test the pure helpers:
  * `_realized_ev_at_threshold` — empirical EV computation.
  * `_build_counterfactual_report` — committed/shadow/default comparison
    schema matches the dashboards consumed by ops.
"""

import random

from core.p_edge_threshold_calibrator import PEdgeThresholdCalibrator
from orderflow_services.p_edge_threshold_calibrator_v1 import (
    _build_counterfactual_report,
    _realized_ev_at_threshold,
)


def test_realized_ev_excludes_below_threshold() -> None:
    samples = [
        (0.40, -1.0, 0),
        (0.50,  1.5, 1),
        (0.60,  1.5, 1),
        (0.70, -1.0, 0),
    ]
    mean, n = _realized_ev_at_threshold(samples, 0.50)
    # Keep (0.50, 0.60, 0.70) → r = (1.5 + 1.5 - 1.0)/3 = 0.666…
    assert n == 3
    assert abs(mean - (1.5 + 1.5 - 1.0) / 3.0) < 1e-9


def test_realized_ev_empty_above_threshold() -> None:
    mean, n = _realized_ev_at_threshold([(0.40, 1.0, 1)], 0.80)
    assert n == 0
    assert mean != mean  # NaN


def test_counterfactual_report_schema() -> None:
    """Report rows must include committed/shadow/default τ + their realized EV
    for each populated bin, sorted deterministically."""
    cal = PEdgeThresholdCalibrator(
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
    rng = random.Random(7)
    base_ms = 100_000
    for i in range(400):
        p = rng.uniform(0.40, 0.80)
        wr = 0.80 if p >= 0.55 else 0.20
        win = "WIN" if rng.random() < wr else "LOSS"
        cal.observe(
            symbol="BTCUSDT",
            regime="trend",
            kind="breakout",
            p_edge=p,
            r_multiple=1.5 if win == "WIN" else -1.0,
            result=win,
            ts_ms=base_ms + i * 1000,
        )

    report = _build_counterfactual_report(cal, default_tau=0.55, generated_ms=999_999)

    assert report["enforce"] is True
    assert report["target_ev_r"] == 0.10
    assert report["default_p_min"] == cal.default_p_min
    assert report["n_bins"] >= 1
    # Each bin must carry every comparison angle.
    sample = report["bins"][0]
    for key in (
        "symbol", "regime", "kind", "n_total", "n_eligible",
        "committed_tau", "committed_ev_r", "committed_n_kept",
        "shadow_tau", "shadow_ev_r", "shadow_n_kept",
        "default_tau", "default_ev_r", "default_n_kept",
        "last_apply_ms", "last_recompute_ms",
    ):
        assert key in sample, f"missing field: {key}"

    # default_tau is mirrored from caller (gate's ENV) — pre-promotion baseline.
    assert all(b["default_tau"] == 0.55 for b in report["bins"])

    # Ordering deterministic by (symbol, regime, kind).
    keys = [(b["symbol"], b["regime"], b["kind"]) for b in report["bins"]]
    assert keys == sorted(keys)


def test_counterfactual_report_empty_calibrator() -> None:
    cal = PEdgeThresholdCalibrator()
    report = _build_counterfactual_report(cal, default_tau=0.55, generated_ms=1)
    assert report["n_bins"] == 0
    assert report["bins"] == []


def test_counterfactual_uses_default_when_no_committed_or_shadow() -> None:
    """A cold bin (no committed/shadow τ) reports EV evaluated at default_tau —
    important so dashboards can show 'EV at static cutoff' even pre-warmup."""
    cal = PEdgeThresholdCalibrator(
        enforce=True,
        min_total_trades=10_000,  # never warms up
        min_kept_trades=10_000,
    )
    cal.observe(symbol="X", regime="trend", kind="breakout",
                p_edge=0.65, r_multiple=1.5, result="WIN", ts_ms=1)
    cal.observe(symbol="X", regime="trend", kind="breakout",
                p_edge=0.50, r_multiple=-1.0, result="LOSS", ts_ms=2)
    report = _build_counterfactual_report(cal, default_tau=0.55, generated_ms=3)
    # Find the BTC-like bin we created.
    rows = [b for b in report["bins"] if b["symbol"] == "X" and b["kind"] == "breakout"]
    assert rows
    row = rows[0]
    assert row["committed_tau"] == 0.0
    assert row["shadow_tau"] == 0.0
    # At default τ=0.55 only the WIN (p=0.65) is kept → EV = 1.5.
    assert row["default_n_kept"] == 1
    assert abs(row["default_ev_r"] - 1.5) < 1e-9
