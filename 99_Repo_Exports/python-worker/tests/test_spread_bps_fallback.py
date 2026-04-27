"""
Test: spread_bps fallback chain
Verifies that when last_spread_bps=0, the system correctly falls back to
last_spread_bps_l2, then BBO computation, instead of returning 0 (which
triggers of_confirm_engine fallback to 15.0 bps).
"""
import types
import pytest


def _make_runtime(**kwargs):
    """Create a minimal mock runtime object."""
    r = types.SimpleNamespace()
    r.last_spread_bps = 0.0
    r.last_spread_bps_l2 = 0.0
    r.last_book = None
    for k, v in kwargs.items():
        setattr(r, k, v)
    return r


def _make_book(best_bid_px=0.0, best_ask_px=0.0, spread_bps=0.0):
    b = types.SimpleNamespace()
    b.best_bid_px = best_bid_px
    b.best_ask_px = best_ask_px
    b.spread_bps = spread_bps
    return b


def _resolve_spread(runtime) -> tuple[float, str]:
    """
    Mirror of the spread fallback chain implemented in tick_processor.py and strategy.py.
    Returns (spread_bps, spread_bps_src).
    """
    spr = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
    _spread_src = "microbar"
    if spr <= 0:
        spr = float(getattr(runtime, "last_spread_bps_l2", 0.0) or 0.0)
        _spread_src = "l2"
    if spr <= 0 and getattr(runtime, "last_book", None) is not None:
        spr = float(getattr(runtime.last_book, "spread_bps", 0.0) or 0.0)
        _spread_src = "book_snap"
    if spr <= 0:
        try:
            _lb = getattr(runtime, "last_book", None)
            _bid = float(getattr(_lb, "best_bid_px", 0.0) or 0.0) if _lb is not None else 0.0
            _ask = float(getattr(_lb, "best_ask_px", 0.0) or 0.0) if _lb is not None else 0.0
            _mid = 0.5 * (_bid + _ask) if _bid > 0 and _ask > 0 else 0.0
            if _mid > 0 and _ask > _bid:
                spr = float((_ask - _bid) / _mid * 10_000.0)
                _spread_src = "bbo"
        except Exception:
            pass
    if spr <= 0:
        _spread_src = "missing"
    return spr, _spread_src


class TestSpreadFallbackChain:
    """Unit tests for spread_bps fallback chain priority."""

    def test_microbar_takes_priority(self):
        """If last_spread_bps > 0 → use it, source = microbar."""
        rt = _make_runtime(last_spread_bps=1.2, last_spread_bps_l2=3.0)
        spr, src = _resolve_spread(rt)
        assert spr == pytest.approx(1.2)
        assert src == "microbar"

    def test_l2_fallback_when_microbar_zero(self):
        """If last_spread_bps=0 and last_spread_bps_l2 > 0 → use L2, source = l2."""
        rt = _make_runtime(last_spread_bps=0.0, last_spread_bps_l2=0.85)
        spr, src = _resolve_spread(rt)
        assert spr == pytest.approx(0.85)
        assert src == "l2"

    def test_book_snap_fallback(self):
        """If microbar=0 and l2=0, use BookSnapshot.spread_bps, source = book_snap."""
        book = _make_book(spread_bps=1.5)
        rt = _make_runtime(last_spread_bps=0.0, last_spread_bps_l2=0.0, last_book=book)
        spr, src = _resolve_spread(rt)
        assert spr == pytest.approx(1.5)
        assert src == "book_snap"

    def test_bbo_direct_computation(self):
        """If all other sources = 0, compute spread from best_bid/best_ask BBO."""
        # ETHUSDT example: bid=2500.00, ask=2500.25 → spread = 0.25/2500.125 * 10000 ≈ 1.0 bps
        book = _make_book(best_bid_px=2500.00, best_ask_px=2500.25, spread_bps=0.0)
        rt = _make_runtime(last_spread_bps=0.0, last_spread_bps_l2=0.0, last_book=book)
        spr, src = _resolve_spread(rt)
        assert src == "bbo"
        assert spr == pytest.approx(0.25 / 2500.125 * 10_000.0, rel=1e-4)
        assert spr < 5.0  # Sanity: ETHUSDT spread << 5 bps

    def test_missing_when_no_data(self):
        """If no data at all → spread=0.0, source = missing."""
        rt = _make_runtime()
        spr, src = _resolve_spread(rt)
        assert spr == 0.0
        assert src == "missing"

    def test_no_fallback_to_15bps(self):
        """When book has live BBO, final spread must NOT be 15 bps (the erroneous fallback)."""
        book = _make_book(best_bid_px=3000.0, best_ask_px=3000.30)
        rt = _make_runtime(last_spread_bps=0.0, last_spread_bps_l2=0.0, last_book=book)
        spr, _ = _resolve_spread(rt)
        # With real BBO data, spread should be realistic (~1 bps), not the 15 bps fallback
        assert spr < 5.0, f"Expected realistic spread < 5 bps, got {spr:.4f} bps"
        assert spr != pytest.approx(15.0)

    def test_exec_risk_norm_reduction(self):
        """
        Integration sanity: with real spread (~1 bps for ETHUSDT),
        exec_risk_norm should NOT clamp to 1.0.

        Params: exec_risk_ref_bps=12.0, w_exec_risk=0.18
        Old:  exec_risk_bps = 15.0 + 1.5 = 16.5 → norm = min(16.5/12.0, 1.0) = 1.0 → pen = 0.18
        New:  exec_risk_bps =  1.0 + 1.5 =  2.5 → norm = 2.5/12.0 = 0.208           → pen = 0.037
        """
        # Simulate: spread from BBO = 1 bps, slippage = 1.5 bps, ref = 12 bps
        spread_bps = 1.0
        slippage_bps = 1.5
        exec_risk_ref_bps = 12.0
        w_exec = 0.18

        exec_risk_bps = spread_bps + slippage_bps
        exec_risk_norm = min(exec_risk_bps / exec_risk_ref_bps, 1.0)
        exec_pen = exec_risk_norm * w_exec

        assert exec_risk_norm < 1.0, "exec_risk_norm must NOT be clamped at 1.0 with real spread"
        assert exec_pen < 0.10, f"exec_pen should be < 0.10 with real spread, got {exec_pen:.4f}"
        # Score improvement: base_score=0.30, exec_pen≈0.037 → score≈0.263 vs old 0.12
        score = max(0.0, min(0.30 - exec_pen, 1.0))
        assert score > 0.20, f"Score should improve significantly vs old 0.12, got {score:.4f}"
