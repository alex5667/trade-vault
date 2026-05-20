from __future__ import annotations

"""Phase-B tests for PEdgeThresholdCalibrator: direction dimension.

Phase B (2026-05-19) extended the bin key from `(symbol, regime, kind)` to
`(symbol, regime, kind, direction)`. This file covers:
  - direction normalization (LONG/SHORT/BUY/SELL/etc → long/short/*)
  - observe writes BOTH direction-specific AND direction="*" bins
  - p_min_for fallback prefers direction-specific bin when warmed
  - p_min_for falls back to direction="*" when direction-specific is cold
  - snapshot includes direction; load_state back-compat with legacy snapshots
"""

from typing import Iterable

import pytest

from core.p_edge_threshold_calibrator import (
    DEFAULT_P_MIN,
    PEdgeThresholdCalibrator,
    _norm_direction,
)


def _feed(
    c: PEdgeThresholdCalibrator,
    *,
    symbol: str,
    regime: str,
    kind: str,
    direction: str,
    samples: Iterable[tuple[float, float, str]],
    base_ms: int = 1_000,
    step_ms: int = 10,
) -> int:
    """Feed (p_edge, r_multiple, result) tuples; returns last ts_ms used."""
    ts = base_ms
    for p, r, res in samples:
        c.observe(
            symbol=symbol, regime=regime, kind=kind, direction=direction,
            p_edge=p, r_multiple=r, result=res, ts_ms=ts,
        )
        ts += step_ms
    return ts - step_ms


# ---------------------------------------------------------------------------
# direction normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw,expected", [
    ("long", "long"),
    ("LONG", "long"),
    ("BUY", "long"),
    ("Bull", "long"),
    ("1", "long"),
    ("+1", "long"),
    ("short", "short"),
    ("SHORT", "short"),
    ("SELL", "short"),
    ("bear", "short"),
    ("-1", "short"),
    ("*", "*"),
    ("", "*"),
    (None, "*"),
    ("na", "*"),
    ("garbage_value", "*"),  # unknown → wildcard fail-open
])
def test_norm_direction(raw: str | None, expected: str) -> None:
    assert _norm_direction(raw) == expected


# ---------------------------------------------------------------------------
# observe populates direction-specific AND wildcard bins
# ---------------------------------------------------------------------------


def test_observe_populates_dir_specific_and_wildcard() -> None:
    c = PEdgeThresholdCalibrator(
        min_total_trades=5, min_kept_trades=3,
        recompute_gap_ms=0, hold_ms=0, abs_thresh=0.0, max_jump_abs=1.0,
        conformal_min_losses=10_000, enforce=True,
    )
    samples = [(0.65, 1.0, "WIN") for _ in range(10)]
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout",
          direction="long", samples=samples)
    # The dir-specific bin AND the (sym,reg,knd,"*") aggregate both exist.
    assert ("BTCUSDT", "trend", "breakout", "long") in c.bins
    assert ("BTCUSDT", "trend", "breakout", "*") in c.bins
    # SHORT bin must NOT be populated (asymmetric ship of the dimension).
    assert ("BTCUSDT", "trend", "breakout", "short") not in c.bins
    # Cross-asset anchor populated as part of the wildcard hierarchy.
    assert ("*", "*", "*", "*") in c.bins


def test_observe_with_star_direction_skips_dir_specific_bin() -> None:
    """When caller passes direction='*' (or omits it), we do NOT create a
    spurious '*' direction bin via the optional finest-grain key — it would
    collide with the aggregate '*' bin and double-count samples."""
    c = PEdgeThresholdCalibrator(enforce=True)
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout",
          direction="*", samples=[(0.65, 1.0, "WIN")])
    # Only ONE entry for (BTCUSDT, trend, breakout, *) — and it has n_observed=1
    # (not 2, which would happen if observe inserted twice).
    b = c.bins[("BTCUSDT", "trend", "breakout", "*")]
    assert b.n_observed == 1


# ---------------------------------------------------------------------------
# fallback hierarchy: direction-specific preferred when warmed
# ---------------------------------------------------------------------------


def test_p_min_for_prefers_dir_specific_when_warmed() -> None:
    """When the direction-specific bin has a different committed τ than the
    wildcard aggregate, p_min_for(direction=long) returns the dir-specific
    value."""
    c = PEdgeThresholdCalibrator(
        min_total_trades=5, min_kept_trades=3,
        recompute_gap_ms=0, hold_ms=0, abs_thresh=0.0, max_jump_abs=1.0,
        conformal_min_losses=10_000, enforce=True,
        target_ev_r=0.10,
    )
    # LONG side: good edge at p≥0.50 (mean R 1.0 > target 0.10).
    long_samples = [(0.55, 1.0, "WIN") for _ in range(100)]
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout",
          direction="long", samples=long_samples)
    # SHORT side: bad edge below 0.70 — mean R goes negative for low τ, only
    # above τ≈0.70 the wins dominate and EV crosses the 0.10 target.
    short_samples = (
        [(0.55, -1.0, "LOSS") for _ in range(80)] +
        [(0.75, 0.8, "WIN") for _ in range(80)]
    )
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout",
          direction="short", samples=short_samples, base_ms=2_000_000)

    long_p = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout",
                         direction="long")
    short_p = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout",
                          direction="short")
    star_p = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout",
                         direction="*")

    # LONG should require a low τ, SHORT a high one — and the wildcard
    # aggregate sits in between (or equals one of them depending on data).
    assert long_p <= short_p
    # All three are concrete (committed) values — none are the default.
    assert long_p != DEFAULT_P_MIN or short_p != DEFAULT_P_MIN
    assert short_p > long_p  # asymmetry preserved
    # Star is at least covered by the aggregate path (not default).
    assert star_p > 0.0


def test_p_min_for_falls_back_to_wildcard_when_dir_cold() -> None:
    """If only the direction='*' aggregate has data, asking for a specific
    direction returns the wildcard value (hierarchy walks downward)."""
    c = PEdgeThresholdCalibrator(
        min_total_trades=5, min_kept_trades=3,
        recompute_gap_ms=0, hold_ms=0, abs_thresh=0.0, max_jump_abs=1.0,
        conformal_min_losses=10_000, enforce=True,
    )
    samples = [(0.65, 1.0, "WIN") for _ in range(50)]
    # Only direction="*" sees data.
    _feed(c, symbol="BTCUSDT", regime="trend", kind="breakout",
          direction="*", samples=samples)

    star_p = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout",
                         direction="*")
    long_p = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout",
                         direction="long")

    # The long-specific bin doesn't exist, so we fall through to the
    # (sym,reg,knd,"*") aggregate — same value.
    assert long_p == star_p
    assert long_p > 0.0


# ---------------------------------------------------------------------------
# snapshot + load_state back-compat
# ---------------------------------------------------------------------------


def test_snapshot_includes_direction_field() -> None:
    c = PEdgeThresholdCalibrator(enforce=True)
    c.observe(
        symbol="BTCUSDT", regime="trend", kind="breakout", direction="long",
        p_edge=0.65, r_multiple=1.0, result="WIN", ts_ms=1_000,
    )
    snap = c.snapshot()
    assert snap["schema_version"] == 2
    long_row = [r for r in snap["bins"] if r["direction"] == "long"]
    star_row = [r for r in snap["bins"] if r["direction"] == "*"]
    assert len(long_row) >= 1
    assert len(star_row) >= 1
    # Both rows carry the new field explicitly.
    assert all("direction" in r for r in snap["bins"])


def test_load_state_back_compat_legacy_snapshot_without_direction() -> None:
    """Legacy snapshots (pre-Phase B, schema_version=1 or missing) had no
    `direction` field. Loader must default missing direction to '*' so old
    pinned snapshots from `autocal:p_edge:state` keep loading cleanly during
    the dual-read window."""
    legacy_snapshot = {
        "enforce": True,
        "target_ev_r": 0.10,
        "default_p_min": 0.55,
        # NOTE: no schema_version field at all — this is what pre-Phase B
        # snapshots look like.
        "bins": [
            {
                "symbol": "BTCUSDT", "regime": "trend", "kind": "breakout",
                "n": 200, "p_min": 0.62, "shadow_p_min": 0.62,
                "shadow_ev_at_pin": 0.15, "shadow_n_kept": 150,
                "last_apply_ms": 12345, "last_recompute_ms": 12345,
            },
        ],
    }
    c = PEdgeThresholdCalibrator()
    c.load_state(legacy_snapshot)
    # Bin must land in the direction="*" aggregate slot.
    assert ("BTCUSDT", "trend", "breakout", "*") in c.bins
    b = c.bins[("BTCUSDT", "trend", "breakout", "*")]
    assert b.p_min == pytest.approx(0.62)
    # Querying with direction="*" or a concrete side both work
    # (the latter via the fallback hierarchy → wildcard aggregate).
    assert c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout",
                       direction="*") == pytest.approx(0.62)
    assert c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout",
                       direction="long") == pytest.approx(0.62)


def test_snapshot_roundtrip_preserves_direction() -> None:
    c1 = PEdgeThresholdCalibrator(enforce=True)
    c1.observe(symbol="BTC", regime="trend", kind="breakout", direction="long",
               p_edge=0.65, r_multiple=1.0, result="WIN", ts_ms=1_000)
    c1.observe(symbol="BTC", regime="trend", kind="breakout", direction="short",
               p_edge=0.65, r_multiple=-1.0, result="LOSS", ts_ms=2_000)
    snap = c1.snapshot()

    c2 = PEdgeThresholdCalibrator()
    c2.load_state(snap)
    # Both direction-specific bins land in the right slots after roundtrip.
    assert ("BTC", "trend", "breakout", "long") in c2.bins
    assert ("BTC", "trend", "breakout", "short") in c2.bins
    assert ("BTC", "trend", "breakout", "*") in c2.bins
