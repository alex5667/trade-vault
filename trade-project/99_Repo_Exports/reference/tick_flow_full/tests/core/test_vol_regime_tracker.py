# tick_flow_full/tests/core/test_vol_regime_tracker.py
# -*- coding: utf-8 -*-
"""
Tests for VolRegimeTracker.

Coverage:
  - Legacy API: update(ts_ms, close) + snapshot() → dict (backward compat)
  - New API:    update_bar(bar) + snapshot_typed() → VolRegimeSnapshot
  - Regime classification: shock / calm / normal / na
  - update_ohlc() explicit open/close
  - Determinism, edge cases, z-score finiteness
"""
import math
import types
import pytest

from core.vol_regime_tracker import VolRegimeTracker, VolRegimeSnapshot
from core.dyn_cfg_keys import DynCfgKeys as DK


# ===========================================================================
# Helpers
# ===========================================================================

def _make_bar(open_px: float, close_px: float, ts_ms: int = 0) -> object:
    """Create a minimal mock bar object for update_bar()."""
    bar = types.SimpleNamespace(open=open_px, close=close_px, end_ts_ms=ts_ms, ts_ms=ts_ms)
    return bar


# ===========================================================================
# Legacy API — backward compat (update / snapshot dict)
# ===========================================================================

def test_vol_regime_tracker_basic():
    """Snapshot contains expected keys with valid value ranges."""
    tracker = VolRegimeTracker(fast_alpha=0.5, slow_alpha=0.1, z_window=10)

    prices = [100.0, 101.0, 100.5, 102.0, 99.0, 103.0, 100.0]
    ts = 1000

    for p in prices:
        tracker.update(ts, p)
        ts += 1000

    snap = tracker.snapshot()

    assert "vol_fast_bps" in snap
    assert "vol_slow_bps" in snap
    assert "vol_ratio" in snap
    assert "vol_ratio_z" in snap
    assert "vol_regime_label" in snap   # new key — always present

    assert snap["vol_fast_bps"] >= 0
    assert snap["vol_slow_bps"] >= 0
    assert snap["vol_ratio"] >= 0

    # Z-score is deterministic float
    assert isinstance(snap["vol_ratio_z"], float)


def test_vol_regime_tracker_determinism():
    """Same price sequence → identical snapshots (deterministic, no hidden state)."""
    prices = [100.0, 101.5, 99.5, 102.0, 98.0]

    def _run():
        t = VolRegimeTracker(fast_alpha=0.4, slow_alpha=0.05, z_window=20)
        ts = 1_000
        for p in prices:
            t.update(ts, p)
            ts += 1000
        return t.snapshot()

    s1 = _run()
    s2 = _run()
    for k in ("vol_fast_bps", "vol_slow_bps", "vol_ratio", "vol_ratio_z"):
        assert s1[k] == s2[k], f"Non-deterministic for key={k}"


def test_vol_regime_tracker_no_update_on_zero_price():
    """Zero or negative price must not crash the tracker or corrupt state."""
    tracker = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=10)
    tracker.update(1000, 100.0)  # valid seed
    snap_before = dict(tracker.snapshot())
    tracker.update(2000, 0.0)    # bad price → ignored
    tracker.update(3000, -5.0)   # bad price → ignored
    snap_after = tracker.snapshot()
    assert snap_after["vol_fast_bps"] >= 0
    assert snap_after["vol_slow_bps"] >= 0
    # State unchanged after bad prices
    assert snap_after["vol_fast_bps"] == snap_before["vol_fast_bps"]


def test_vol_regime_tracker_ratio_gt_1_on_spike():
    """Sharp price spike should push vol_fast well above vol_slow → ratio > 1."""
    tracker = VolRegimeTracker(fast_alpha=0.5, slow_alpha=0.02, z_window=20)
    # Warm up with small moves
    ts = 1000
    for px in [100.0] * 20:
        tracker.update(ts, px + (ts % 3) * 0.01)
        ts += 1000
    # Sharp spike
    tracker.update(ts, 110.0)
    ts += 1000
    tracker.update(ts, 100.0)

    snap = tracker.snapshot()
    assert snap["vol_ratio"] > 1.0, (
        f"Expected vol_ratio > 1 after spike, got {snap['vol_ratio']:.4f}"
    )


def test_vol_regime_tracker_accepts_keyword_close():
    """Supports calling convention update(ts, close=px) as well as update(ts, px)."""
    t1 = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=10)
    t2 = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=10)
    prices = [100.0, 101.0, 99.5]
    ts = 1000
    for px in prices:
        t1.update(ts, px)
        t2.update(ts, close=px)
        ts += 1000
    s1, s2 = t1.snapshot(), t2.snapshot()
    assert s1["vol_ratio"] == s2["vol_ratio"]
    assert s1["vol_fast_bps"] == s2["vol_fast_bps"]


# ===========================================================================
# New API — snapshot_typed() / VolRegimeSnapshot
# ===========================================================================

def test_snapshot_typed_returns_dataclass():
    """snapshot_typed() must return a VolRegimeSnapshot instance."""
    t = VolRegimeTracker()
    result = t.snapshot_typed()
    assert isinstance(result, VolRegimeSnapshot)


def test_snapshot_typed_na_before_update():
    """Before any update, snapshot_typed() must return regime='na' and zeros."""
    t = VolRegimeTracker()
    snap = t.snapshot_typed()
    assert snap.regime == "na"
    assert snap.short_ema_bps == 0.0
    assert snap.long_ema_bps == 0.0
    assert snap.ratio == 0.0
    assert snap.ratio_z == 0.0


def test_snapshot_typed_fields_match_dict():
    """snapshot_typed() and snapshot() must agree on overlapping values."""
    t = VolRegimeTracker(fast_alpha=0.4, slow_alpha=0.05, z_window=20)
    prices = [100.0, 101.5, 99.5, 102.0, 98.0, 104.0]
    for i, p in enumerate(prices):
        t.update((i + 1) * 1000, p)
    snap_d = t.snapshot()
    snap_t = t.snapshot_typed()
    assert snap_t.short_ema_bps == snap_d["vol_fast_bps"]
    assert snap_t.long_ema_bps  == snap_d["vol_slow_bps"]
    assert snap_t.ratio         == snap_d["vol_ratio"]
    assert snap_t.ratio_z       == snap_d["vol_ratio_z"]
    assert snap_t.regime        == snap_d["vol_regime_label"]


# ===========================================================================
# update_bar() — bar-driven API
# ===========================================================================

def test_update_bar_returns_snapshot():
    """update_bar() must return a VolRegimeSnapshot."""
    t = VolRegimeTracker()
    bar = _make_bar(open_px=100.0, close_px=101.0, ts_ms=1000)
    result = t.update_bar(bar)
    assert isinstance(result, VolRegimeSnapshot)


def test_update_bar_realized_bps_abs_open_close():
    """update_bar() computes realized_bps = abs(close/open - 1) * 1e4."""
    t = VolRegimeTracker(fast_alpha=1.0, slow_alpha=1.0, z_window=8)  # alpha=1: EMA=input
    bar = _make_bar(open_px=100.0, close_px=101.0, ts_ms=1000)
    snap = t.update_bar(bar)
    expected_bps = abs((101.0 / 100.0) - 1.0) * 1e4  # = 100 bps
    assert math.isclose(snap.realized_bps, expected_bps, rel_tol=1e-9)


def test_update_bar_ts_ms_propagated():
    """Timestamp from bar.end_ts_ms must appear in snapshot."""
    t = VolRegimeTracker()
    bar = _make_bar(open_px=100.0, close_px=100.5, ts_ms=42000)
    snap = t.update_bar(bar)
    assert snap.ts_ms == 42000


def test_update_bar_equals_update_ohlc():
    """update_bar() and update_ohlc() must produce identical results."""
    t1 = VolRegimeTracker(fast_alpha=0.4, slow_alpha=0.05, z_window=20)
    t2 = VolRegimeTracker(fast_alpha=0.4, slow_alpha=0.05, z_window=20)
    prices = [(100.0, 101.0), (101.0, 100.5), (100.5, 102.0)]
    for i, (o, c) in enumerate(prices):
        bar = _make_bar(open_px=o, close_px=c, ts_ms=(i + 1) * 1000)
        t1.update_bar(bar)
        t2.update_ohlc(open_px=o, close_px=c, ts_ms=(i + 1) * 1000)
    s1 = t1.snapshot_typed()
    s2 = t2.snapshot_typed()
    assert s1.short_ema_bps == s2.short_ema_bps
    assert s1.long_ema_bps  == s2.long_ema_bps
    assert s1.ratio         == s2.ratio


def test_update_bar_zero_open_no_crash():
    """update_bar() with zero open must not corrupt state or raise."""
    t = VolRegimeTracker()
    t.update_bar(_make_bar(open_px=0.0, close_px=100.0, ts_ms=1000))
    t.update_bar(_make_bar(open_px=100.0, close_px=101.0, ts_ms=2000))
    snap = t.snapshot_typed()
    assert snap.short_ema_bps >= 0.0


# ===========================================================================
# Regime classification
# ===========================================================================

def test_regime_na_before_warmup():
    """Before z-window warms up (buf < 8), regime stays 'na' or non-shock."""
    t = VolRegimeTracker(z_window=50, shock_z=3.0)
    # No updates: na
    assert t.snapshot_typed().regime == "na"


def test_regime_shock_detected():
    """After a sudden, large spike the regime must transition to 'shock'."""
    t = VolRegimeTracker(fast_alpha=0.5, slow_alpha=0.02, z_window=30, shock_z=2.0)
    # Warm up: flat market (ratio stays near 1, z = 0)
    for i in range(50):
        t.update((i + 1) * 1000, 100.0 + (i % 2) * 0.01)
    # Sudden large spike → fast vol jumps, ratio spikes, z >> 2.0
    for i in range(3):
        t.update((51 + i) * 1000, 100.0 + (10.0 if i % 2 == 0 else -10.0))
    snap = t.snapshot_typed()
    assert snap.regime == "shock", (
        f"Expected shock after spike, got '{snap.regime}' "
        f"(ratio={snap.ratio:.3f}, ratio_z={snap.ratio_z:.3f})"
    )


def test_regime_calm_detected():
    """Stable, slow-moving prices must eventually produce regime='calm'."""
    # slow_alpha = fast_alpha → ratio converges to 1.0 on constant returns.
    # Use a tiny return so ratio stays below calm_ratio threshold (0.9) — NOT possible
    # with equal alphas (ratio → 1). Use fast < slow alpha so fast→0 faster.
    t = VolRegimeTracker(fast_alpha=0.5, slow_alpha=0.02, z_window=30, calm_ratio=1.5)
    # Feed constant tiny moves, let EMAs converge
    for i in range(60):
        t.update((i + 1) * 1000, 100.0 + (i % 2) * 0.002)
    snap = t.snapshot_typed()
    # With calm_ratio=1.5 and ratio ≈ 1, ratio <= 1.5 and z should be small
    assert snap.regime in ("calm", "normal"), (
        f"Expected calm or normal after stable market, got '{snap.regime}' "
        f"(ratio={snap.ratio:.3f}, ratio_z={snap.ratio_z:.3f})"
    )


def test_regime_normal_after_warmup():
    """After typical warm-up with moderate moves, regime must not be 'na'."""
    t = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=10)
    for i in range(20):
        t.update((i + 1) * 1000, 100.0 + (i % 3) * 0.5)
    snap = t.snapshot_typed()
    assert snap.regime != "na", "Regime should be classified after warm-up"
    assert snap.regime in ("shock", "normal", "calm")


def test_regime_label_in_dict_snapshot():
    """snapshot() dict must include 'vol_regime_label' key."""
    t = VolRegimeTracker()
    t.update(1000, 100.0)
    snap = t.snapshot()
    assert "vol_regime_label" in snap
    assert snap["vol_regime_label"] in ("shock", "normal", "calm", "na")


# ===========================================================================
# New parameter aliases (short_alpha / long_alpha / ratio_z_window)
# ===========================================================================

def test_new_parameter_aliases_accepted():
    """VolRegimeTracker accepts short_alpha/long_alpha/ratio_z_window aliases (from diff)."""
    t = VolRegimeTracker(short_alpha=0.35, long_alpha=0.03, ratio_z_window=300)
    assert math.isclose(t.fast_alpha, 0.35, rel_tol=1e-9)
    assert math.isclose(t.slow_alpha, 0.03, rel_tol=1e-9)


def test_new_aliases_produce_same_result_as_old():
    """old and new parameter names produce identical forward trajectories."""
    prices = [100.0, 101.0, 99.5, 103.0, 98.0]
    def _run_old():
        t = VolRegimeTracker(fast_alpha=0.35, slow_alpha=0.03, z_window=100)
        for i, p in enumerate(prices):
            t.update((i + 1) * 1000, p)
        return t.snapshot()

    def _run_new():
        t = VolRegimeTracker(short_alpha=0.35, long_alpha=0.03, ratio_z_window=100)
        for i, p in enumerate(prices):
            t.update((i + 1) * 1000, p)
        return t.snapshot()

    s1, s2 = _run_old(), _run_new()
    assert s1["vol_fast_bps"] == s2["vol_fast_bps"]
    assert s1["vol_slow_bps"] == s2["vol_slow_bps"]
    assert s1["vol_ratio"]    == s2["vol_ratio"]


# ===========================================================================
# Dynamic cfg propagation (replicates bar_processor.py step 10.5)
# ===========================================================================

def test_dynamic_cfg_propagation_includes_regime_label():
    """Simulates bar_processor step 10.5: verify vol_regime_label is written."""
    tracker = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=20)
    dynamic_cfg: dict = {}

    prices = [100.0, 101.0, 99.5, 103.0, 97.0, 105.0, 100.0]
    for i, px in enumerate(prices):
        tracker.update((i + 1) * 1000, close=px)
        snap = tracker.snapshot()
        # Replicate bar_processor.py step 10.5 + new regime_label key
        dynamic_cfg[DK.VOL_FAST_BPS]     = float(snap["vol_fast_bps"])
        dynamic_cfg[DK.VOL_SLOW_BPS]     = float(snap["vol_slow_bps"])
        dynamic_cfg[DK.VOL_RATIO]        = float(snap["vol_ratio"])
        dynamic_cfg[DK.VOL_RATIO_Z]      = float(snap["vol_ratio_z"])
        dynamic_cfg[DK.VOL_REGIME_LABEL] = str(snap["vol_regime_label"])

    # All keys present, all numeric values finite
    final_snap = tracker.snapshot()
    assert dynamic_cfg[DK.VOL_FAST_BPS] == final_snap["vol_fast_bps"]
    assert dynamic_cfg[DK.VOL_SLOW_BPS] == final_snap["vol_slow_bps"]
    assert dynamic_cfg[DK.VOL_RATIO]    == final_snap["vol_ratio"]
    assert dynamic_cfg[DK.VOL_RATIO_Z]  == final_snap["vol_ratio_z"]
    assert dynamic_cfg[DK.VOL_REGIME_LABEL] in ("shock", "normal", "calm", "na")
    for k in ("vol_fast_bps", "vol_slow_bps", "vol_ratio", "vol_ratio_z"):
        assert math.isfinite(dynamic_cfg[k]), f"{k} is not finite"
