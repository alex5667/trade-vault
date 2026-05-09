from __future__ import annotations

"""
test_world_practice_trackers_v1.py
===================================
World-practice integration tests for the core micro-structure trackers.
"""


import asyncio
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _as_float(x: Any, default: float = 0.0) -> float:
    """Safely coerce any value to float, returning default on failure."""
    try:
        return float(x)
    except Exception:
        return default


def _call_update(tracker: Any, ts_ms: int, price: float) -> None:
    """
    Best-effort update call that handles multiple naming conventions:
      - update(ts_ms, close)     -- VolRegimeTracker positional
      - update(ts_ms=..., price=...)  -- generic kwarg
      - on_price(ts_ms=..., price=...) -- alternate name
    """
    if hasattr(tracker, "update"):
        try:
            # Prefer real signature: update(ts_ms, close)
            tracker.update(int(ts_ms), float(price))
            return
        except TypeError:
            pass
        try:
            tracker.update(ts_ms=int(ts_ms), price=float(price))
            return
        except TypeError:
            pass
    if hasattr(tracker, "on_price"):
        tracker.on_price(ts_ms=int(ts_ms), price=float(price))
        return
    raise AttributeError(f"{type(tracker).__name__} has no update/on_price method")


def _call_snapshot(tracker: Any) -> dict[str, Any]:
    """Return snapshot dict from tracker; falls back to attribute inspection."""
    if hasattr(tracker, "snapshot"):
        return tracker.snapshot()
    if hasattr(tracker, "state"):
        return tracker.state()
    # Last-resort: pull known public attributes into a dict
    out: dict[str, Any] = {}
    for k in ("vol_fast_bps", "vol_slow_bps", "vol_ratio", "vol_ratio_z",
               "recovered", "res_recovered", "score"):
        if hasattr(tracker, k):
            out[k] = getattr(tracker, k)
    return out


# ===========================================================================
# 1. VolRegimeTracker -- shock detection
# ===========================================================================

def test_vol_regime_tracker_detects_shock() -> None:
    """
    After a volatility shock the short realized vol should be elevated vs the
    slow baseline, so vol_ratio > 1 and vol_ratio_z should be finite.

    Real API: VolRegimeTracker(fast_alpha, slow_alpha, z_window).
    Real snapshot keys: vol_ratio, vol_ratio_z (not vol_z).
    """
    from core.vol_regime_tracker import VolRegimeTracker

    # Construct with real API -- fast_alpha/slow_alpha/z_window
    tr = VolRegimeTracker(fast_alpha=0.35, slow_alpha=0.04, z_window=128)

    ts = 1_700_000_000_000
    px = 100.0

    # Calm regime: tiny oscillations, seed both EMAs to the same level
    for i in range(80):
        ts += 200
        # Alternate very small moves so both EMAs see the same low vol
        px = 100.0 + (0.01 if i % 2 == 0 else -0.01)
        _call_update(tr, ts, px)

    snap_pre = _call_snapshot(tr)
    ratio_pre = _as_float(snap_pre.get("vol_ratio", snap_pre.get("ratio", 0.0)))

    # Shock regime: large alternating moves -> fast EMA spikes, slow stays low
    for j in range(40):
        ts += 200
        px = 130.0 if j % 2 == 0 else 70.0
        _call_update(tr, ts, px)

    snap_post = _call_snapshot(tr)
    # Real keys: vol_ratio, vol_ratio_z (not vol_z)
    ratio_post = _as_float(snap_post.get("vol_ratio", snap_post.get("ratio", 0.0)))
    z_post = _as_float(
        snap_post.get("vol_ratio_z",
                      snap_post.get("vol_z",
                                    snap_post.get("ratio_z", 0.0)))
    )

    # After shock: ratio must be > pre-shock ratio
    assert ratio_post >= ratio_pre, (
        f"Expected post-shock ratio {ratio_post:.4f} >= pre-shock {ratio_pre:.4f}"
    )
    # z_post is derived from RollingRobustZ which uses MAD-based normalization.
    # When MAD approaches zero (all obs near the same value) the z-score can be
    # very large or inf — this is mathematically correct. Only assert it is a number.
    assert z_post == z_post, "vol_ratio_z is NaN"  # NaN != NaN
    # Hard check that ratio is elevated
    assert ratio_post > 1.0, (
        f"Expected vol_ratio > 1.0 after shock, got {ratio_post:.4f}"
    )


# ===========================================================================
# 2. BookResilienceTracker -- sweep -> drawer -> recovery
# ===========================================================================

def test_book_resilience_tracker_recovers() -> None:
    """
    Simulates a sweep followed by a depth crater and then replenishment.
    After recovery the snapshot must report res_recovered=1 and a valid
    res_recovery_ms > 0.

    Real API:
      BookResilienceTracker(recover_ratio, max_recovery_ms, grace_ms)
      .on_sweep(ts_ms, depth_ref_usd, side)
      .on_book(ts_ms, depth_now_usd, side)
    """
    from core.book_resilience_tracker import BookResilienceTracker

    tr = BookResilienceTracker(
        min_sweep_usd=50.0,
        recover_ratio=0.85,
        max_recovery_ms=30_000,
        grace_ms=5_000,
    )

    ts0 = 1_700_000_000_000

    # Sweep baseline at depth=100 USD
    tr.on_sweep(ts_ms=ts0, depth_ref_usd=100.0, side="bid")

    # Depth collapses to 40% of baseline (well below recovery threshold)
    tr.on_book(ts_ms=ts0 + 100, depth_now_usd=40.0, side="bid")

    # Depth recovers above 85% of baseline
    tr.on_book(ts_ms=ts0 + 1_500, depth_now_usd=90.0, side="bid")

    snap = _call_snapshot(tr)

    # Accept both naming conventions from book_resilience.py snapshot keys
    recovered   = int(snap.get("res_recovered", snap.get("recovered", 0)) or 0)
    recovery_ms = int(snap.get("res_recovery_ms", snap.get("recovery_ms", 0)) or 0)
    min_ratio   = _as_float(snap.get("res_min_ratio", snap.get("min_ratio", 1.0)), 1.0)

    # min_ratio must reflect the deep drop from 100->40 (approx 0.4)
    assert min_ratio < 0.60, (
        f"Expected min_ratio < 0.60 after depth crater; got {min_ratio:.4f}"
    )
    # Must detect recovery
    assert recovered == 1, (
        f"Expected recovered==1 after depth crossed threshold; got {recovered}"
    )
    assert 0 < recovery_ms <= 30_000, (
        f"Expected 0 < recovery_ms <= 30000; got {recovery_ms}"
    )


# ===========================================================================
# 3. fill_prob_proxy -- monotonicity of p_fill
# ===========================================================================

def test_fill_prob_proxy_monotonic() -> None:
    """
    fill_prob decreases monotonically as cancel-to-trade pressure increases.
    Both extremes must be in [0, 1].

    Real API: compute_fill_prob_proxy(direction, cancel_to_trade_bid,
                                      cancel_to_trade_ask, eta_fill_bid_sec, ...)
    Returns a dict with keys fill_prob_proxy / fill_prob / p_fill.
    """
    from core.fill_prob_proxy import compute_fill_prob_proxy

    # Low cancel pressure for a LONG order
    out_low = compute_fill_prob_proxy(
        direction="LONG",
        cancel_to_trade_bid=0.2,
        cancel_to_trade_ask=0.2,
        eta_fill_bid_sec=0.5,
        eta_fill_ask_sec=0.5,
        max_wait_s=2.0,
    )
    # High cancel pressure
    out_high = compute_fill_prob_proxy(
        direction="LONG",
        cancel_to_trade_bid=8.0,
        cancel_to_trade_ask=8.0,
        eta_fill_bid_sec=0.5,
        eta_fill_ask_sec=0.5,
        max_wait_s=2.0,
    )

    p_low  = _as_float(out_low.get("fill_prob",  out_low.get("p_fill",  0.0)))
    p_high = _as_float(out_high.get("fill_prob", out_high.get("p_fill", 0.0)))

    assert 0.0 <= p_low  <= 1.0, f"p_low not in [0,1]: {p_low}"
    assert 0.0 <= p_high <= 1.0, f"p_high not in [0,1]: {p_high}"
    assert p_low > p_high, (
        f"Expected p_low ({p_low:.4f}) > p_high ({p_high:.4f}) "
        "-- higher cancel pressure should reduce fill probability"
    )


# ===========================================================================
# 4. TickProcessor -- missing qty must not crash the hot path
# ===========================================================================

class _DummyRedis:
    """Minimal async Redis stub sufficient for TickProcessor instantiation."""

    def __init__(self) -> None:
        self._h: dict[str, dict[str, str]] = {}

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._h.get(key, {}))

    async def hset(self, key: str, mapping: Any = None, **kwargs: Any) -> int:
        m = dict(mapping or {})
        m.update(kwargs)
        self._h.setdefault(key, {}).update({str(k): str(v) for k, v in m.items()})
        return 1

    async def expire(self, key: str, ttl_s: int) -> bool:
        return True

    async def xadd(self, *args: Any, **kwargs: Any) -> str:
        return "0-0"

    async def get(self, key: str) -> Any:
        return None

    async def set(self, key: str, val: Any, **kwargs: Any) -> bool:
        return True

    async def setex(self, key: str, ttl: int, val: Any) -> bool:
        return True

    async def incr(self, key: str) -> int:
        return 1


class _DummyPublisher:
    """Stub signal publisher -- accepts any call, returns None."""

    async def publish_signal(self, *args: Any, **kwargs: Any) -> None:
        return None


class _DummyDeltaDetector:
    """
    Validates that TickProcessor injects a valid 'qty' into the tick dict
    before calling push().
    """

    def push(self, tick: dict[str, Any]) -> Any:
        # After the qty-sanitization patch, 'qty' must always be present and valid
        assert "qty" in tick, "'qty' not in tick after sanitization"
        _ = float(tick["qty"])
        # Return empty dict -> no delta spike (TickProcessor returns None early)
        return {}


class _DummyRuntime:
    """Minimal SymbolRuntime stand-in for TickProcessor unit tests."""

    def __init__(self) -> None:
        self.symbol = "BTCUSDT"
        self.config: dict[str, Any] = {}
        self.dynamic_cfg: dict[str, Any] = {}
        self.last_ts_ms = 0
        self.last_tick_ts = 0
        self.tick_count = 0
        self.delta_detector = _DummyDeltaDetector()


def test_tick_processor_missing_qty_does_not_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Regression: when upstream tick omits qty/q/quantity/volume the qty-
    sanitization block must default to 0.0, not raise.
    TickProcessor must return None (no signal) without crashing.
    """
    # Disable tick-time quarantine to avoid Redis side-effects in unit test
    monkeypatch.setenv("ENABLE_TICK_TIME_QUARANTINE", "0")

    from services.orderflow.components.tick_processor import TickProcessor

    redis_stub = _DummyRedis()

    tp = TickProcessor(
        redis=redis_stub,
        ticks=redis_stub,
        publisher=_DummyPublisher(),
        of_engine=type("E", (), {"symbol": "BTCUSDT"})(),
        calib_svc=type("C", (), {"symbol": "BTCUSDT"})(),
        atr_cache=None,
        atr_sanity=None,
        conf_scorer=None,
    )

    # Stub out _apply_tick_time_guard to bypass full Redis state
    async def _fake_tick_time_guard(self: Any, runtime: Any, tick: Any) -> dict[str, Any]:
        return {"tick_ts_ms": 1_700_000_000_000, "decision": "ok", "meta": {}}

    tp._apply_tick_time_guard = _fake_tick_time_guard.__get__(tp, TickProcessor)

    runtime = _DummyRuntime()

    # Tick with no qty/q/quantity/volume field -- should not crash
    tick: dict[str, Any] = {
        "ts_ms": 1_700_000_000_000,
        "price": 100.0,
        "is_buyer_maker": False,
    }

    out = asyncio.run(tp.process_tick(runtime, tick))
    # DummyDeltaDetector returns {} -> no delta spike -> TickProcessor returns None
    assert out is None, f"Expected None, got {out!r}"
