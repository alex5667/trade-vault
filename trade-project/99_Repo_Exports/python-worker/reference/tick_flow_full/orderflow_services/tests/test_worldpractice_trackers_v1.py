import asyncio
import inspect
import math
import sys
from pathlib import Path


def _ensure_tick_flow_full_on_path() -> None:
    """Make tick_flow_full packages (core/, services/) importable in pytest.

    Repo layout: <root>/tick_flow_full/{core,services,...}
    Many runtime modules expect PYTHONPATH=tick_flow_full.
    These tests keep working even if PYTHONPATH is not set.
    """

    repo_root = Path(__file__).resolve().parents[3]
    tff = repo_root / "tick_flow_full"
    if tff.exists() and str(tff) not in sys.path:
        # Append (not prepend) to avoid shadowing top-level packages.
        sys.path.append(str(tff))


_ensure_tick_flow_full_on_path()


def _construct(cls, preferred_kwargs: dict):
    """Construct class with only supported kwargs (signature-tolerant)."""
    sig = inspect.signature(cls)
    kwargs = {k: v for k, v in preferred_kwargs.items() if k in sig.parameters}
    return cls(**kwargs)


def _call(obj, method_names, preferred_args=(), preferred_kwargs=None):
    """Call first existing method name with a tolerant signature."""
    preferred_kwargs = dict(preferred_kwargs or {})
    for name in method_names:
        if not hasattr(obj, name):
            continue
        m = getattr(obj, name)
        try:
            sig = inspect.signature(m)
            # Try keyword call first
            kw = {k: v for k, v in preferred_kwargs.items() if k in sig.parameters}
            if kw:
                return m(**kw)
        except Exception:
            pass
        # Fallback to positional
        return m(*preferred_args)
    raise AttributeError(f"None of methods exist: {method_names}")


def test_vol_regime_tracker_ratio_and_z_go_up_on_shock():
    from core.vol_regime_tracker import VolRegimeTracker

    tr = _construct(
        VolRegimeTracker,
        {
            "fast_alpha": 0.40,
            "slow_alpha": 0.05,
            "z_window": 128,
            "eps": 1e-12,
        },
    )

    ts0 = 1_700_000_000_000
    px = 100.0

    # Stable regime: tiny oscillations
    for i in range(120):
        px = 100.0 + (0.02 if (i % 2 == 0) else -0.02)
        _call(tr, ("update", "push", "on_price"), preferred_args=(ts0 + i * 1000, px))

    snap1 = _call(tr, ("snapshot", "to_dict"), preferred_args=())
    r1 = float(snap1.get("vol_ratio", snap1.get("ratio", 0.0)) or 0.0)
    z1 = float(snap1.get("vol_ratio_z", snap1.get("ratio_z", 0.0)) or 0.0)
    assert math.isfinite(r1)
    assert math.isfinite(z1)

    # Shock: large oscillations (range expansion / vol shock)
    for j in range(30):
        px = 130.0 if (j % 2 == 0) else 70.0
        _call(tr, ("update", "push", "on_price"), preferred_args=(ts0 + (200 + j) * 1000, px))

    snap2 = _call(tr, ("snapshot", "to_dict"), preferred_args=())
    r2 = float(snap2.get("vol_ratio", snap2.get("ratio", 0.0)) or 0.0)
    z2 = float(snap2.get("vol_ratio_z", snap2.get("ratio_z", 0.0)) or 0.0)
    assert math.isfinite(r2)
    assert math.isfinite(z2)

    # Ratio should increase in shock regime, and z should be non-negative.
    assert r2 > 1.0
    assert r2 > r1
    assert z2 >= 0.0


def test_book_resilience_tracker_detects_recovery_and_measures_time():
    from core.book_resilience_tracker import BookResilienceTracker

    tr = _construct(
        BookResilienceTracker,
        {
            "min_sweep_usd": 50.0,
            "recover_ratio": 0.85,
            "max_recovery_ms": 30_000,
            "grace_ms": 5_000,
        },
    )

    ts0 = 1_700_000_000_000

    # Sweep happens at reference depth
    _call(
        tr,
        ("on_sweep", "start_sweep"),
        preferred_kwargs={"ts_ms": ts0, "depth_ref_usd": 100.0, "side": "bid"},
        preferred_args=(ts0, 100.0),
    )

    # Depth collapses
    _call(
        tr,
        ("on_book", "update", "observe"),
        preferred_kwargs={"ts_ms": ts0 + 100, "depth_now_usd": 40.0, "side": "bid"},
        preferred_args=(ts0 + 100, 40.0),
    )

    # Depth recovers above threshold
    _call(
        tr,
        ("on_book", "update", "observe"),
        preferred_kwargs={"ts_ms": ts0 + 1500, "depth_now_usd": 90.0, "side": "bid"},
        preferred_args=(ts0 + 1500, 90.0),
    )

    snap = _call(tr, ("snapshot", "to_dict"), preferred_args=())

    # Keys are intentionally tolerant to implementation changes.
    recovered = int(snap.get("res_recovered", snap.get("recovered", 0)) or 0)
    recovery_ms = int(snap.get("res_recovery_ms", snap.get("recovery_ms", 0)) or 0)
    min_ratio = float(snap.get("res_min_ratio", snap.get("min_ratio", 1.0)) or 1.0)

    assert recovered in (0, 1)
    assert min_ratio <= 1.0
    assert min_ratio < 0.6  # drop from 100 -> 40 => 0.4
    assert recovered == 1
    assert 0 < recovery_ms <= 30_000


def test_compute_fill_prob_proxy_monotonic_wrt_cancel_pressure():
    from core.fill_prob_proxy import compute_fill_prob_proxy

    # Build args from signature (robust to minor API changes).
    def call_with(cancel_to_trade: float):
        sig = inspect.signature(compute_fill_prob_proxy)
        kw = {}
        for p in sig.parameters:
            if p in ("direction", "side"):
                kw[p] = "LONG"
            elif p in ("spread_bps", "spread"):
                kw[p] = 1.0
            elif p in ("ofi_z", "ofi"):
                kw[p] = 0.0
            elif p in ("cancel_to_trade_bid", "cancel_to_trade") or p == "cancel_to_trade_ask":
                kw[p] = float(cancel_to_trade)
            elif p in ("intensity_tps", "trades_per_sec"):
                kw[p] = 5.0
            elif p in ("depth_ref_usd", "depth_usd"):
                kw[p] = 10_000.0
            elif p in ("order_usd", "notional_usd"):
                kw[p] = 200.0
            elif p in ("max_wait_s", "max_wait_sec"):
                kw[p] = 2.0
        out = compute_fill_prob_proxy(**kw)
        if isinstance(out, dict):
            return float(out.get("fill_prob", out.get("p_fill", 0.0)) or 0.0)
        return float(out or 0.0)

    p_low = call_with(0.2)
    p_high = call_with(5.0)
    assert 0.0 <= p_low <= 1.0
    assert 0.0 <= p_high <= 1.0
    assert p_low > p_high


def test_tick_processor_missing_qty_does_not_crash_and_does_not_call_l3():
    """Sanity/integration test: missing qty must not break TickProcessor.

    This protects the hot path when upstream feeds send minimal trade payloads.
    """

    from services.orderflow.components.tick_processor import TickProcessor

    class DummyRedis:
        async def hgetall(self, key):
            return {}

        async def hset(self, key, mapping=None, **kwargs):
            return 1

        async def expire(self, key, ttl):
            return True

    class DummyDeltaDetector:
        def push(self, tick):
            return {}

    class L3MustNotBeCalled:
        def on_trade(self, *a, **k):
            raise AssertionError("l3_stats.on_trade must not be called when qty is missing")

    class DummyRuntime:
        def __init__(self):
            self.symbol = "BTCUSDT"
            self.config = {}
            self.dynamic_cfg = None
            self.last_tick_ts = 0
            self.tick_count = 0
            self.delta_detector = DummyDeltaDetector()
            # In patched code we may call runtime.l3_stats if qty>0.
            self.l3_stats = L3MustNotBeCalled()

    dummy_redis = DummyRedis()
    tp = TickProcessor(
        redis=dummy_redis,
        ticks=dummy_redis,
        publisher=object(),
        of_engine=type("E", (), {"symbol": "BTCUSDT"})(),
        calib_svc=type("C", (), {"symbol": "BTCUSDT"})(),
        atr_cache=None,
        atr_sanity=None,
        conf_scorer=None,
    )

    async def _fake_apply_tick_time_guard(self, runtime, tick):
        return {"tick_ts_ms": 1_700_000_000_000, "decision": "ok", "meta": {}}

    # Monkeypatch instance method (no external pytest plugin required).
    tp._apply_tick_time_guard = _fake_apply_tick_time_guard.__get__(tp, TickProcessor)

    rt = DummyRuntime()

    # Missing qty field (common for some minimal feeds)
    tick = {"price": 100.0, "m": False, "T": 1_700_000_000_000}

    async def _run():
        return await tp.process_tick(rt, tick)

    out = asyncio.run(_run())
    assert out is None
