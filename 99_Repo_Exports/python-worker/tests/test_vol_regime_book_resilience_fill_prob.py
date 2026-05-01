# tests/test_vol_regime_book_resilience_fill_prob.py
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Integration tests for:
  - VolRegimeTracker (tick_flow_full/core/vol_regime_tracker.py)
  - BookResilienceTracker (tick_flow_full/core/book_resilience.py)
  - compute_fill_prob_proxy (tick_flow_full/core/fill_prob_proxy.py)

Tests verify correctness, determinism, edge-case safety, and monotone properties
as specified by the diff integration (Stage 4, commit 3-style recommendations).
"""

import math
import sys
import os
import pytest

# The adapted tracker modules live in tick_flow_full/core, not python-worker/core.
# Add tick_flow_full/ to sys.path so `from core.xxx import` resolves correctly here.
_TFF_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tick_flow_full")
if _TFF_ROOT not in sys.path:
    sys.path.insert(0, _TFF_ROOT)

from core.vol_regime_tracker import VolRegimeTracker       # noqa: E402
from core.book_resilience import BookResilienceTracker     # noqa: E402
from core.fill_prob_proxy import compute_fill_prob_proxy   # noqa: E402
from core.dyn_cfg_keys import DynCfgKeys as DK


# ---------------------------------------------------------------------------
# VolRegimeTracker
# ---------------------------------------------------------------------------

class TestVolRegimeTracker:
    """Tests for VolRegimeTracker (realized-vol ratio + robust z-score)."""

    def test_snapshot_keys_present(self):
        """Snapshot must always return mandatory keys."""
        t = VolRegimeTracker()
        t.update(1000, 100.0)
        snap = t.snapshot()
        for key in ("vol_fast_bps", "vol_slow_bps", "vol_ratio", "vol_ratio_z", "vol_ts_ms"):
            assert key in snap, f"Missing key: {key}"

    def test_initial_snapshot_defaults(self):
        """Before any update the snapshot must be all-zero (no spurious state)."""
        t = VolRegimeTracker()
        snap = t.snapshot()
        assert snap["vol_fast_bps"] == 0.0
        assert snap["vol_slow_bps"] == 0.0
        assert snap["vol_ratio"] == 0.0
        assert snap["vol_ratio_z"] == 0.0

    def test_vol_always_nonnegative(self):
        """Realized-vol measures must stay ≥ 0 for any price sequence."""
        t = VolRegimeTracker(fast_alpha=0.4, slow_alpha=0.05, z_window=30)
        prices = [100.0, 102.5, 99.0, 105.0, 95.0, 101.0, 103.0]
        for i, p in enumerate(prices):
            t.update((i + 1) * 1000, p)
        snap = t.snapshot()
        assert snap["vol_fast_bps"] >= 0.0
        assert snap["vol_slow_bps"] >= 0.0
        assert snap["vol_ratio"] >= 0.0

    def test_determinism(self):
        """Same price sequence → identical snapshot (no hidden mutable/random state)."""
        prices = [100.0, 101.5, 99.5, 102.0, 98.0, 104.0]

        def run():
            t = VolRegimeTracker(fast_alpha=0.4, slow_alpha=0.05, z_window=20)
            for i, p in enumerate(prices):
                t.update((i + 1) * 1000, p)
            return t.snapshot()

        s1, s2 = run(), run()
        for k in ("vol_fast_bps", "vol_slow_bps", "vol_ratio", "vol_ratio_z"):
            assert s1[k] == s2[k], f"Non-deterministic for key={k}"

    def test_spike_raises_vol_ratio(self):
        """Sharp price spike must push vol_fast >> vol_slow → ratio > 1."""
        t = VolRegimeTracker(fast_alpha=0.5, slow_alpha=0.02, z_window=30)
        # Warm up: small, stable prices → vol_slow builds up slowly
        for i, px in enumerate([100.0] * 30):
            t.update((i + 1) * 1000, px + (i % 3) * 0.01)
        # Spike
        t.update(31_000, 115.0)
        t.update(32_000, 100.0)
        snap = t.snapshot()
        assert snap["vol_ratio"] > 1.0, (
            f"Expected vol_ratio > 1 after spike, got {snap['vol_ratio']:.4f}"
        )

    def test_stable_prices_ratio_near_one(self):
        """After warm-up on stable price, fast and slow vol converge → ratio ≈ 1."""
        t = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.1, z_window=50)
        # Feed same return bps repeatedly (constant moves)
        for i in range(100):
            # Alternating tiny moves so return is constant ~1 bps
            px = 1000.0 + (i % 2) * 0.1
            t.update((i + 1) * 1000, px)
        snap = t.snapshot()
        # With equal fast/slow vol, ratio should be close to 1
        assert 0.5 <= snap["vol_ratio"] <= 2.0, (
            f"vol_ratio far from 1 under stable prices: {snap['vol_ratio']:.4f}"
        )

    def test_zero_price_ignored(self):
        """Zero or negative price must not corrupt state."""
        t = VolRegimeTracker()
        t.update(1000, 100.0)  # valid seed
        snap_before = t.snapshot().copy()
        t.update(2000, 0.0)    # bad: ignored
        t.update(3000, -5.0)   # bad: ignored
        snap_after = t.snapshot()
        # vol values must remain unchanged
        assert snap_after["vol_fast_bps"] == snap_before["vol_fast_bps"]
        assert snap_after["vol_slow_bps"] == snap_before["vol_slow_bps"]

    def test_accepts_keyword_close(self):
        """Supports update(ts, close=px) in addition to positional update(ts, px)."""
        t1 = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=10)
        t2 = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=10)
        prices = [100.0, 101.0, 99.5, 103.0]
        for i, px in enumerate(prices):
            t1.update((i + 1) * 1000, px)
            t2.update((i + 1) * 1000, close=px)
        s1, s2 = t1.snapshot(), t2.snapshot()
        assert s1["vol_ratio"] == s2["vol_ratio"]
        assert s1["vol_fast_bps"] == s2["vol_fast_bps"]
        assert s1["vol_slow_bps"] == s2["vol_slow_bps"]

    def test_z_score_is_finite_after_warmup(self):
        """vol_ratio_z must be a finite float after enough updates."""
        t = VolRegimeTracker(fast_alpha=0.4, slow_alpha=0.05, z_window=10)
        for i in range(20):
            t.update((i + 1) * 1000, 100.0 + i * 0.5)
        snap = t.snapshot()
        assert math.isfinite(snap["vol_ratio_z"]), (
            f"vol_ratio_z is not finite: {snap['vol_ratio_z']}"
        )

    def test_high_volatility_period_reflected(self):
        """Two periods: calm then stormy → vol_fast_bps higher after storm."""
        t = VolRegimeTracker(fast_alpha=0.5, slow_alpha=0.05, z_window=20)
        # Calm period
        for i in range(20):
            t.update((i + 1) * 1000, 100.0 + (i % 2) * 0.01)
        snap_calm = t.snapshot()
        # Storm period: 5% swings every bar
        for i in range(10):
            px = 100.0 + (5.0 if i % 2 == 0 else -5.0)
            t.update((i + 21) * 1000, px)
        snap_storm = t.snapshot()
        assert snap_storm["vol_fast_bps"] > snap_calm["vol_fast_bps"], (
            "vol_fast_bps should rise in storm period"
        )


# ---------------------------------------------------------------------------
# BookResilienceTracker
# ---------------------------------------------------------------------------

class TestBookResilienceTracker:
    """Tests for BookResilienceTracker (post-sweep depth replenishment)."""

    def test_initial_state_inactive(self):
        """Before any sweep, tracker must be inactive."""
        t = BookResilienceTracker()
        snap = t.snapshot()
        assert snap["res_active"] == 0
        assert snap["res_recovered"] == 0

    def test_sweep_activates_tracking(self):
        """on_sweep with valid depth activates tracker."""
        t = BookResilienceTracker(target_recovery_ratio=0.85, max_window_ms=5000)
        t.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
        snap = t.snapshot()
        assert snap["res_active"] == 1
        assert snap["res_min_ratio"] == pytest.approx(1.0, abs=1e-6)

    def test_standard_recovery_lifecycle(self):
        """Sweep → depth drop → recovery: standard lifecycle."""
        t = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=5000)
        t.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)

        # Depth drops to 50% on bid side
        t.on_book(1500, bid_depth_usd=5000.0, ask_depth_usd=10_000.0)
        snap = t.snapshot()
        assert snap["res_min_ratio"] == pytest.approx(0.5, abs=1e-6)
        assert snap["res_recovered"] == 0

        # Depth recovers to 90% (> 80% target)
        t.on_book(2000, bid_depth_usd=9000.0, ask_depth_usd=10_000.0)
        snap = t.snapshot()
        assert snap["res_curr_ratio"] == pytest.approx(0.9, abs=1e-6)
        assert snap["res_recovered"] == 1

        # After grace period: deactivated
        t.on_book(2300, bid_depth_usd=9000.0, ask_depth_usd=10_000.0)
        snap = t.snapshot()
        assert snap["res_active"] == 0

    def test_no_sweep_no_active(self):
        """Without a sweep, book updates must have no effect on active state."""
        t = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=5000)
        for ts in range(1000, 6000, 500):
            t.on_book(ts, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
        snap = t.snapshot()
        assert snap["res_active"] == 0
        assert snap["res_recovered"] == 0

    def test_slow_recovery_stays_unrecovered(self):
        """If depth never recovers to target within window → res_recovered=0."""
        t = BookResilienceTracker(target_recovery_ratio=0.85, max_window_ms=3000)
        t.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
        # Depth stays at 40%: never reaches 85% target
        for ts in range(1500, 5000, 500):
            t.on_book(ts, bid_depth_usd=4000.0, ask_depth_usd=4000.0)
        t.on_book(4100, bid_depth_usd=4000.0, ask_depth_usd=4000.0)
        snap = t.snapshot()
        assert snap["res_active"] == 0   # expired
        assert snap["res_recovered"] == 0

    def test_window_expiry_deactivates(self):
        """res_active must flip to 0 once max_window_ms elapsed."""
        t = BookResilienceTracker(target_recovery_ratio=0.9, max_window_ms=2000)
        t.on_sweep(1, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
        # Within window
        t.on_book(1501, bid_depth_usd=5000.0, ask_depth_usd=5000.0)
        assert t.snapshot()["res_active"] == 1
        # Past window
        t.on_book(2002, bid_depth_usd=5000.0, ask_depth_usd=5000.0)
        assert t.snapshot()["res_active"] == 0

    def test_invalid_sweep_ignored(self):
        """on_sweep with zero or negative bid/ask depth must be silently ignored."""
        t = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=5000)
        t.on_sweep(1000, bid_depth_usd=0.0, ask_depth_usd=5000.0)  # zero bid
        assert t.snapshot()["res_active"] == 0
        t.on_sweep(1000, bid_depth_usd=-100.0, ask_depth_usd=5000.0)  # negative bid
        assert t.snapshot()["res_active"] == 0

    def test_min_ratio_never_increases(self):
        """res_min_ratio is the minimum depth ratio observed → monotone non-increasing."""
        t = BookResilienceTracker(target_recovery_ratio=0.9, max_window_ms=10_000)
        t.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
        prev_min = 1.0
        for ts, depth in [(1500, 8000), (2000, 6000), (2500, 4000), (3000, 5000), (3500, 7000)]:
            t.on_book(ts, bid_depth_usd=float(depth), ask_depth_usd=float(depth))
            snap = t.snapshot()
            assert snap["res_min_ratio"] <= prev_min + 1e-9, (
                f"res_min_ratio increased from {prev_min} to {snap['res_min_ratio']}"
            )
            if snap["res_active"] == 1:
                prev_min = snap["res_min_ratio"]

    def test_snapshot_keys_present(self):
        """snapshot() must always return all expected keys."""
        t = BookResilienceTracker()
        snap = t.snapshot()
        for key in ("res_active", "res_recovered", "res_recovery_ms",
                    "res_min_ratio", "res_curr_ratio", "res_speed_per_s",
                    "res_baseline_min_usd", "res_min_min_usd", "res_last_min_usd",
                    "res_sweep_ts_ms"):
            assert key in snap, f"Missing snapshot key: {key}"

    def test_recovery_ms_correct(self):
        """res_recovery_ms must equal elapsed time from sweep to first recovery."""
        t = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=10_000)
        t.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)
        t.on_book(1500, bid_depth_usd=5000.0, ask_depth_usd=5000.0)    # not recovered
        t.on_book(3500, bid_depth_usd=9000.0, ask_depth_usd=9000.0)    # recovered at t=3500
        snap = t.snapshot()
        assert snap["res_recovered"] == 1
        assert snap["res_recovery_ms"] == 2500  # 3500 - 1000


# ---------------------------------------------------------------------------
# compute_fill_prob_proxy
# ---------------------------------------------------------------------------

class TestFillProbProxy:
    """Tests for compute_fill_prob_proxy (L3-lite fill probability estimate)."""

    def test_output_keys_present(self):
        """Result dict must contain all documented keys."""
        res = compute_fill_prob_proxy(
            direction="LONG",
            cancel_to_trade_bid=0.2,
            cancel_to_trade_ask=0.5,
            eta_fill_bid_sec=0.5,
            eta_fill_ask_sec=1.2,
            max_wait_s=2.0,
        )
        for key in ("fill_prob_proxy", "fill_prob", "p_fill", "eta_fill_sec",
                    "cancel_to_trade_side", "p_base", "p_wait"):
            assert key in res, f"Missing key: {key}"

    def test_prob_bounds(self):
        """fill_prob_proxy must be in [0, 1] for any inputs."""
        for c2t in [0.0, 0.5, 1.0, 5.0, 100.0]:
            for eta in [0.0, 0.1, 2.0, 10.0]:
                res = compute_fill_prob_proxy(
                    direction="LONG",
                    cancel_to_trade_bid=c2t,
                    cancel_to_trade_ask=0.1,
                    eta_fill_bid_sec=eta,
                    max_wait_s=2.0,
                )
                p = res["fill_prob_proxy"]
                assert 0.0 <= p <= 1.0, f"fill_prob_proxy={p} out of [0,1] for c2t={c2t} eta={eta}"

    def test_long_uses_bid_side(self):
        """LONG direction must use bid cancel_to_trade and bid eta."""
        res = compute_fill_prob_proxy(
            direction="LONG",
            cancel_to_trade_bid=0.2,
            cancel_to_trade_ask=0.9,  # very high: should NOT affect LONG
            eta_fill_bid_sec=0.5,
            eta_fill_ask_sec=5.0,     # very high: should NOT affect LONG
            max_wait_s=2.0,
        )
        assert res["eta_fill_sec"] == pytest.approx(0.5, abs=1e-9)
        assert res["cancel_to_trade_side"] == pytest.approx(0.2, abs=1e-9)

    def test_short_uses_ask_side(self):
        """SHORT direction must use ask cancel_to_trade and ask eta."""
        res = compute_fill_prob_proxy(
            direction="SHORT",
            cancel_to_trade_bid=0.9,  # very high: should NOT affect SHORT
            cancel_to_trade_ask=0.1,
            eta_fill_bid_sec=5.0,     # very high: should NOT affect SHORT
            eta_fill_ask_sec=0.1,
            max_wait_s=2.0,
        )
        assert res["eta_fill_sec"] == pytest.approx(0.1, abs=1e-9)
        assert res["cancel_to_trade_side"] == pytest.approx(0.1, abs=1e-9)

    def test_high_cancel_reduces_prob(self):
        """Higher cancel_to_trade → lower fill probability (monotone inverse)."""
        def prob_for_c2t(c2t):
            return compute_fill_prob_proxy(
                direction="LONG",
                cancel_to_trade_bid=c2t,
                cancel_to_trade_ask=0.0,
                eta_fill_bid_sec=0.5,
                max_wait_s=2.0,
            )["fill_prob"]

        p0 = prob_for_c2t(0.0)
        p1 = prob_for_c2t(1.0)
        p5 = prob_for_c2t(5.0)
        assert p0 > p1 > p5, "fill_prob must decrease as cancel_to_trade increases"

    def test_fast_eta_higher_prob(self):
        """Lower eta_fill_sec (faster fill) → higher fill probability."""
        def prob_for_eta(eta):
            return compute_fill_prob_proxy(
                direction="LONG",
                cancel_to_trade_bid=0.2,
                cancel_to_trade_ask=0.0,
                eta_fill_bid_sec=eta,
                max_wait_s=2.0,
            )["fill_prob"]

        p_fast = prob_for_eta(0.1)
        p_slow = prob_for_eta(10.0)
        assert p_fast > p_slow, "Faster fill (lower eta) must give higher fill_prob"

    def test_zero_eta_no_penalty(self):
        """If eta == 0 (no ETA data), p_wait must be 1.0 (no penalty applied)."""
        res = compute_fill_prob_proxy(
            direction="LONG",
            cancel_to_trade_bid=0.0,
            cancel_to_trade_ask=0.0,
            eta_fill_bid_sec=0.0,
            max_wait_s=2.0,
        )
        assert res["p_wait"] == pytest.approx(1.0, abs=1e-9)
        assert res["p_base"] == pytest.approx(1.0, abs=1e-9)
        assert res["fill_prob"] == pytest.approx(1.0, abs=1e-9)

    def test_aliases_consistent(self):
        """fill_prob, p_fill, fill_prob_proxy must all be the same value."""
        res = compute_fill_prob_proxy(
            direction="LONG",
            cancel_to_trade_bid=0.3,
            cancel_to_trade_ask=0.1,
            eta_fill_bid_sec=1.0,
            max_wait_s=2.0,
        )
        assert res["fill_prob"] == res["fill_prob_proxy"]
        assert res["fill_prob"] == res["p_fill"]

    def test_direction_case_insensitive(self):
        """direction should be case-insensitive (LONG/long/Long)."""
        kwargs = dict(
            cancel_to_trade_bid=0.2,
            cancel_to_trade_ask=0.5,
            eta_fill_bid_sec=0.5,
            eta_fill_ask_sec=1.2,
            max_wait_s=2.0,
        )
        r1 = compute_fill_prob_proxy(direction="LONG", **kwargs)
        r2 = compute_fill_prob_proxy(direction="long", **kwargs)
        r3 = compute_fill_prob_proxy(direction="Long", **kwargs)
        assert r1["fill_prob"] == r2["fill_prob"] == r3["fill_prob"]


# ---------------------------------------------------------------------------
# Integration: bar_processor vol_regime → dynamic_cfg propagation simulation
# ---------------------------------------------------------------------------

class TestVolRegimeDynamicCfgIntegration:
    """
    Simulates the bar_processor.py step 10.5 integration:
    runtime.vol_regime.update() → runtime.dynamic_cfg.
    Verifies that keys written to dynamic_cfg are consistent with snapshot.
    """

    def test_dynamic_cfg_propagation(self):
        """Simulates dynamic_cfg population as done in bar_processor step 10.5."""
        tracker = VolRegimeTracker(fast_alpha=0.3, slow_alpha=0.05, z_window=20)
        dynamic_cfg: dict = {}

        prices = [100.0, 101.0, 99.5, 103.0, 97.0, 105.0, 100.0]
        for i, px in enumerate(prices):
            tracker.update((i + 1) * 1000, close=px)
            snap = tracker.snapshot()
            # Replicate bar_processor logic
            dynamic_cfg[DK.VOL_FAST_BPS] = float(snap["vol_fast_bps"])
            dynamic_cfg[DK.VOL_SLOW_BPS] = float(snap["vol_slow_bps"])
            dynamic_cfg[DK.VOL_RATIO]    = float(snap["vol_ratio"])
            dynamic_cfg[DK.VOL_RATIO_Z]  = float(snap["vol_ratio_z"])

        # All keys must be present and match snapshot
        final_snap = tracker.snapshot()
        assert dynamic_cfg[DK.VOL_FAST_BPS] == final_snap["vol_fast_bps"]
        assert dynamic_cfg[DK.VOL_SLOW_BPS] == final_snap["vol_slow_bps"]
        assert dynamic_cfg[DK.VOL_RATIO]    == final_snap["vol_ratio"]
        assert dynamic_cfg[DK.VOL_RATIO_Z]  == final_snap["vol_ratio_z"]
        # Everything must be finite
        for k in ("vol_fast_bps", "vol_slow_bps", "vol_ratio", "vol_ratio_z"):
            assert math.isfinite(dynamic_cfg[k]), f"{k} is not finite"


# ---------------------------------------------------------------------------
# Integration: book_resilience → dynamic_cfg propagation simulation
# ---------------------------------------------------------------------------

class TestBookResilienceDynamicCfgIntegration:
    """
    Simulates the book_processor.py integration:
    runtime.book_resilience.on_book() → runtime.dynamic_cfg.
    """

    def test_dynamic_cfg_resilience_propagation(self):
        """All resilience keys are correctly merged into dynamic_cfg after recovery."""
        tracker = BookResilienceTracker(target_recovery_ratio=0.8, max_window_ms=10_000)
        dynamic_cfg: dict = {}

        # Sweep event detected (as called from bar_processor/sweep detector)
        tracker.on_sweep(1000, bid_depth_usd=10_000.0, ask_depth_usd=10_000.0)

        # Simulate book updates merging snapshot into dynamic_cfg (as in book_processor.py)
        for ts, depth in [(1200, 6000), (1500, 7000), (2000, 9000), (2300, 9000)]:
            tracker.on_book(ts, bid_depth_usd=float(depth), ask_depth_usd=float(depth))
            snap = tracker.snapshot()
            for k, v in snap.items():
                dynamic_cfg[k] = v  # mirrors book_processor.py: dynamic_cfg[_k] = _v

        # Final state: recovered
        assert dynamic_cfg.get(DK.RES_RECOVERED) == 1
        assert dynamic_cfg.get(DK.RES_ACTIVE) == 0  # deactivated after grace
        assert "res_recovery_ms" in dynamic_cfg
        assert "res_min_ratio" in dynamic_cfg
        # Speed proxy must be ≥ 0
        assert dynamic_cfg.get(DK.RES_SPEED_PER_S, -1.0) >= 0.0
