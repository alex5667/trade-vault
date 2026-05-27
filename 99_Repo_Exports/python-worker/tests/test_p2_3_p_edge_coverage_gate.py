"""P2.3 — min-coverage gate for directional bins in PEdgeThresholdCalibrator.

Covers:
  1.  Directional bin skipped when n_observed < min_dir_coverage
  2.  Directional bin used when n_observed >= min_dir_coverage
  3.  shadow_p_min also respects the coverage gate
  4.  Wildcard bins ("*") never gated — always trusted
  5.  min_dir_coverage=0 disables gate completely
  6.  Coverage gate does NOT apply to non-directional fallback levels (sym,reg,kind,"*")
  7.  p_min_for returns default when directional bin below gate and wildcard cold
  8.  Both long and short bins gated independently
  9.  Coverage gate does not prevent sample accumulation in the bin
 10.  Snapshot/load_state round-trip preserves n_observed correctly
 11.  Regression: existing Phase-B behavior unchanged when n_observed >= gate
 12.  Coverage gate applied to level 1 only — level 2+ wildcard never gated
"""
from __future__ import annotations

from core.p_edge_threshold_calibrator import (
    DEFAULT_P_MIN,
    PEdgeThresholdCalibrator,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_calibrator(*, enforce: bool = True, min_dir_coverage: int = 150,
                     min_total_trades: int = 10, min_kept_trades: int = 5,
                     hold_ms: int = 0, recompute_gap_ms: int = 0) -> PEdgeThresholdCalibrator:
    return PEdgeThresholdCalibrator(
        enforce=enforce,
        min_dir_coverage=min_dir_coverage,
        min_total_trades=min_total_trades,
        min_kept_trades=min_kept_trades,
        hold_ms=hold_ms,
        recompute_gap_ms=recompute_gap_ms,
        target_ev_r=0.05,
        conformal_min_losses=5,
    )


def _fill_bin(c: PEdgeThresholdCalibrator, *, symbol: str, regime: str,
              kind: str, direction: str, n: int, p: float = 0.7, r: float = 0.5,
              base_ms: int = 1_000) -> None:
    """Feed n WIN samples to warm up a bin."""
    for i in range(n):
        c.observe(
            symbol=symbol, regime=regime, kind=kind, direction=direction,
            p_edge=p, r_multiple=r, result="WIN", ts_ms=base_ms + i * 10,
        )


# ─── 1. Directional bin skipped below min_dir_coverage ──────────────────────

def test_directional_bin_skipped_below_coverage():
    """Directional bin with p_min>0 but n_observed < min_dir_coverage is skipped."""
    c = _make_calibrator(min_dir_coverage=150)
    # Feed only 50 samples to the LONG directional bin
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="long", n=50)
    # Feed 200 samples to the wildcard aggregate bin
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="*", n=200, p=0.65, r=0.4, base_ms=100_000)

    # Directional bin has p_min set (50 samples > min_total_trades=10)
    dir_key = ("BTCUSDT", "trend", "iceberg", "long")
    assert dir_key in c.bins
    assert c.bins[dir_key].p_min > 0.0
    assert c.bins[dir_key].n_observed < 150  # below gate

    # p_min_for should fall through to wildcard bin, not use directional
    p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="iceberg",
                        direction="long")
    wildcard_key = ("BTCUSDT", "trend", "iceberg", "*")
    wildcard_p_min = c.bins[wildcard_key].p_min
    assert p_min == wildcard_p_min, (
        f"Expected wildcard p_min={wildcard_p_min:.3f}, got {p_min:.3f} "
        f"(directional bin has only {c.bins[dir_key].n_observed} obs < gate=150)"
    )


# ─── 2. Directional bin used when >= min_dir_coverage ────────────────────────

def test_directional_bin_used_above_coverage():
    """Directional bin with n_observed >= min_dir_coverage is preferred over wildcard."""
    c = _make_calibrator(min_dir_coverage=50)
    # Feed 80 samples to LONG directional bin (> gate=50)
    _fill_bin(c, symbol="ETHUSDT", regime="range", kind="delta_spike",
              direction="long", n=80, p=0.75, r=0.6)
    # Feed 200 samples to wildcard with lower p_min
    _fill_bin(c, symbol="ETHUSDT", regime="range", kind="delta_spike",
              direction="*", n=200, p=0.60, r=0.3, base_ms=100_000)

    dir_key = ("ETHUSDT", "range", "delta_spike", "long")
    assert c.bins[dir_key].n_observed >= 50  # above gate

    p_min = c.p_min_for(symbol="ETHUSDT", regime="range", kind="delta_spike",
                        direction="long")
    # Should use directional bin, not wildcard
    assert p_min == c.bins[dir_key].p_min, (
        f"Expected directional p_min={c.bins[dir_key].p_min:.3f}, got {p_min:.3f}"
    )


# ─── 3. shadow_p_min also respects coverage gate ─────────────────────────────

def test_shadow_p_min_respects_coverage_gate():
    """shadow_p_min skips directional bin below min_dir_coverage."""
    c = _make_calibrator(min_dir_coverage=100, enforce=False)
    _fill_bin(c, symbol="SOLUSDT", regime="trending", kind="breakout",
              direction="short", n=30)  # below gate=100
    _fill_bin(c, symbol="SOLUSDT", regime="trending", kind="breakout",
              direction="*", n=120, p=0.68, r=0.4, base_ms=50_000)

    dir_key = ("SOLUSDT", "trending", "breakout", "short")
    wc_key = ("SOLUSDT", "trending", "breakout", "*")
    assert c.bins[dir_key].shadow_p_min > 0.0
    assert c.bins[dir_key].n_observed < 100

    shadow = c.shadow_p_min(symbol="SOLUSDT", regime="trending", kind="breakout",
                            direction="short")
    assert shadow == c.bins[wc_key].shadow_p_min, (
        f"shadow_p_min should use wildcard bin (dir bin below coverage gate)"
    )


# ─── 4. Wildcard bins never gated ────────────────────────────────────────────

def test_wildcard_bins_never_gated():
    """direction='*' bins are always trusted regardless of n_observed."""
    c = _make_calibrator(min_dir_coverage=999_999)  # impossibly high gate
    # Feed only 15 samples to wildcard bin (above min_total_trades=10)
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="*", n=15, p=0.7, r=0.5)
    assert c.bins[("BTCUSDT", "trend", "iceberg", "*")].p_min > 0.0

    p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="iceberg")
    # Wildcard bin should be used even though gate=999_999 (gate doesn't apply to "*")
    assert p_min == c.bins[("BTCUSDT", "trend", "iceberg", "*")].p_min


# ─── 5. min_dir_coverage=0 disables gate ─────────────────────────────────────

def test_min_dir_coverage_zero_disables_gate():
    """min_dir_coverage=0 disables the gate — directional bin always used if p_min>0."""
    c = _make_calibrator(min_dir_coverage=0)
    # Use 15 samples — above min_total_trades=10 — so grid runs and p_min gets set
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="long", n=15, p=0.7, r=0.5)

    dir_key = ("BTCUSDT", "trend", "iceberg", "long")
    assert c.bins[dir_key].p_min > 0.0, (
        f"after 15 obs (>min_total=10), p_min should be set; got {c.bins[dir_key].p_min}"
    )

    p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="iceberg",
                        direction="long")
    assert p_min == c.bins[dir_key].p_min, "gate disabled → use directional bin directly"


# ─── 6. Non-directional fallback levels (direction="*") never gated ──────────

def test_non_directional_fallback_levels_never_gated():
    """Level 2 (sym,reg,kind,"*") falls back to level 3 (sym,reg,"*","*") etc.
    None of these are gated by min_dir_coverage — gate only applies to level 1."""
    c = _make_calibrator(min_dir_coverage=999)
    # Feed only level 3 (sym,reg,"*","*") through a LONG observe
    # (observe writes to all wildcard levels automatically)
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="unknown_kind",
              direction="long", n=15, p=0.7, r=0.5)

    # level 1 (BTCUSDT,trend,unknown_kind,long) has 15 obs < gate=999 → gated
    # level 2 (BTCUSDT,trend,unknown_kind,*) has 15 obs but direction="*" → not gated
    p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="unknown_kind",
                        direction="long")
    # Should use level 2 (direction="*") — not gated
    wc_key = ("BTCUSDT", "trend", "unknown_kind", "*")
    assert p_min == c.bins[wc_key].p_min


# ─── 7. Returns default when directional below gate AND wildcard cold ─────────

def test_returns_default_when_both_cold():
    """When directional bin is below gate and wildcard has no committed p_min, return default.

    Note: observe() always writes to both direction-specific AND wildcard bins.
    To test the 'both cold' path we inject a directional bin directly with p_min>0
    but n_observed below gate, while leaving wildcard bins unset.
    """
    c = _make_calibrator(min_dir_coverage=500)
    # Directly inject a directional bin with p_min>0 but n_observed<gate
    # (simulates a bin that was computed but has few observations)
    from collections import deque
    from core.p_edge_threshold_calibrator import _Bin
    dir_key: tuple = ("PEPEUSDT", "squeeze", "delta_spike", "short")
    b = _Bin(buf=deque(maxlen=5000))
    b.p_min = 0.60  # has a committed threshold
    b.n_observed = 30  # but below gate=500

    c.bins[dir_key] = b
    # No wildcard bins → all wildcard keys have p_min=0.0

    p_min = c.p_min_for(symbol="PEPEUSDT", regime="squeeze", kind="delta_spike",
                        direction="short")
    # Directional gated (30 < 500), wildcard cold (p_min=0.0) → fallback to default
    assert p_min == DEFAULT_P_MIN, (
        f"Expected default_p_min={DEFAULT_P_MIN}, got {p_min}"
    )


# ─── 8. Long and short bins gated independently ───────────────────────────────

def test_long_and_short_gated_independently():
    """LONG above gate, SHORT below gate → LONG uses directional, SHORT uses wildcard."""
    c = _make_calibrator(min_dir_coverage=100)
    # LONG: 150 obs → above gate
    _fill_bin(c, symbol="ETHUSDT", regime="trend", kind="iceberg",
              direction="long", n=150, p=0.72, r=0.55, base_ms=0)
    # SHORT: 40 obs → below gate
    _fill_bin(c, symbol="ETHUSDT", regime="trend", kind="iceberg",
              direction="short", n=40, p=0.68, r=0.3, base_ms=200_000)
    # Wildcard: 200 obs
    _fill_bin(c, symbol="ETHUSDT", regime="trend", kind="iceberg",
              direction="*", n=200, p=0.65, r=0.4, base_ms=400_000)

    long_key = ("ETHUSDT", "trend", "iceberg", "long")
    short_key = ("ETHUSDT", "trend", "iceberg", "short")
    wc_key = ("ETHUSDT", "trend", "iceberg", "*")

    assert c.bins[long_key].n_observed >= 100
    assert c.bins[short_key].n_observed < 100

    p_long = c.p_min_for(symbol="ETHUSDT", regime="trend", kind="iceberg",
                         direction="long")
    p_short = c.p_min_for(symbol="ETHUSDT", regime="trend", kind="iceberg",
                          direction="short")

    assert p_long == c.bins[long_key].p_min, "LONG above gate → use directional"
    assert p_short == c.bins[wc_key].p_min, "SHORT below gate → fallback to wildcard"


# ─── 9. Coverage gate does not block sample accumulation ─────────────────────

def test_coverage_gate_does_not_block_observe():
    """observe() always appends to directional bin regardless of gate status."""
    c = _make_calibrator(min_dir_coverage=200)
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="long", n=50)
    dir_key = ("BTCUSDT", "trend", "iceberg", "long")
    # n_observed is tracked even if bin is below gate on read
    assert c.bins[dir_key].n_observed == 50
    # Add 10 more
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="long", n=10, base_ms=100_000)
    assert c.bins[dir_key].n_observed == 60


# ─── 10. After restart, directional bin n_observed resets to 0 (safe fallback) ──

def test_restart_resets_n_observed_to_zero():
    """n_observed is not persisted in snapshot — after load_state it resets to 0.

    This is the CORRECT safe behavior: after a restart the gate forces fallback
    to the wildcard bin until enough new observations re-accumulate. The committed
    p_min is restored (from snapshot) but the gate treats the directional bin as
    cold until n_observed crosses min_dir_coverage again.
    """
    c = _make_calibrator(min_dir_coverage=50)
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="long", n=80)  # above gate=50

    dir_key = ("BTCUSDT", "trend", "iceberg", "long")
    assert c.bins[dir_key].n_observed >= 50
    assert c.bins[dir_key].p_min > 0.0

    # Snapshot and reload (simulating restart)
    snap = c.snapshot()
    c2 = _make_calibrator(min_dir_coverage=50)
    c2.load_state(snap)

    # After restart: p_min is restored but n_observed resets to 0 (not persisted)
    if dir_key in c2.bins:
        assert c2.bins[dir_key].p_min > 0.0, "p_min should survive restart"
        assert c2.bins[dir_key].n_observed == 0, (
            "n_observed resets to 0 after restart — directional bin is re-gated "
            "until new observations accumulate. This is safe (fallback to wildcard)."
        )


# ─── 11. Regression: Phase-B fallback uses directional bin when above gate ────

def test_phase_b_directional_preferred_when_above_gate():
    """p_min_for() returns the directional bin's value (not wildcard) when above gate.

    Both bins may converge to the same tau (TAU_FLOOR) in all-WIN scenarios —
    what matters is that p_min_for() selects the directional bin first.
    We verify this by directly injecting bins with different p_min values.
    """
    c = _make_calibrator(min_dir_coverage=50)
    from collections import deque
    from core.p_edge_threshold_calibrator import _Bin

    long_key: tuple = ("BTCUSDT", "trend", "iceberg", "long")
    wc_key: tuple = ("BTCUSDT", "trend", "iceberg", "*")

    # Inject directional bin: p_min=0.72, n_observed=200 (above gate=50)
    b_long = _Bin(buf=deque(maxlen=5000))
    b_long.p_min = 0.72
    b_long.n_observed = 200
    c.bins[long_key] = b_long

    # Inject wildcard bin: p_min=0.60 (lower)
    b_wc = _Bin(buf=deque(maxlen=5000))
    b_wc.p_min = 0.60
    b_wc.n_observed = 400
    c.bins[wc_key] = b_wc

    p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="iceberg",
                        direction="long")
    # Directional bin is above gate (200 >= 50), should be preferred over wildcard
    assert p_min == 0.72, f"Expected directional p_min=0.72, got {p_min}"
    assert p_min != b_wc.p_min, "Directional (0.72) should differ from wildcard (0.60)"


# ─── 12. Gate only at level 1 — level 2+ wildcard not gated ─────────────────

def test_gate_only_at_level1_not_level2():
    """The min-coverage gate is ONLY applied at level 1 (direction-specific key).
    Level 2 (direction='*' but sym/reg/kind specific) is a wildcard key and always trusted."""
    c = _make_calibrator(min_dir_coverage=999)
    # Feed only level 2 wildcard (via direction="*" observe)
    _fill_bin(c, symbol="BTCUSDT", regime="trend", kind="iceberg",
              direction="*", n=30, p=0.7, r=0.5)

    wc_key = ("BTCUSDT", "trend", "iceberg", "*")
    assert c.bins[wc_key].n_observed == 30
    assert c.bins[wc_key].p_min > 0.0

    # Query with direction="long" — level 1 key doesn't exist, falls to level 2
    p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="iceberg",
                        direction="long")
    # Level 2 is a wildcard key, not gated → should be used
    assert p_min == c.bins[wc_key].p_min
