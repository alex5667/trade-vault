from __future__ import annotations

"""Tests for BookTradeConsistencyGate P6next.

Covers:
  1. Disabled gate → no apply
  2. Stale book detection (book_ts older than event_ts)
  3. Adverse cross detection (trade_px > best_ask + tolerance)
  4. Veto mode (GATE_PROFILE=hard) + stale book → VETO_BOOK_STALE
  5. Fresh book + trade inside BBO → no flags
  6. Missing BBO → missing_bbo flag, no veto
  7. Combined stale + adverse cross in veto mode → VETO_BOOK_STALE_ADVERSE_CROSS
  8. Monitor mode → flags but no veto even when conditions met
"""

import math

import pytest


# ---------------------------------------------------------------------------
# Minimal ctx stub
# ---------------------------------------------------------------------------
class _Ctx:
    """Lightweight context object for testing gate."""

    def __init__(
        self,
        *,
        trade_px: float = 0.0,
        best_bid: float = 0.0,
        best_ask: float = 0.0,
        ts_event_ms: int | None = None,
        book_ts_ms: int | None = None,
        stream_type: str = "tick",
    ) -> None:
        self.trade_px = trade_px
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.stream_type = stream_type
        if ts_event_ms is not None:
            self.ts_event_ms = ts_event_ms
        if book_ts_ms is not None:
            self.book_ts_ms = book_ts_ms


# ---------------------------------------------------------------------------
# Import gate — skip tests cleanly if deps missing in CI
# ---------------------------------------------------------------------------
try:
    from services.orderflow.book_trade_consistency_gate import (
        BookTradeConsistencyDecision,
        BookTradeConsistencyGate,
    )
    _IMPORT_OK = True
except ImportError:
    _IMPORT_OK = False

pytestmark = pytest.mark.skipif(not _IMPORT_OK, reason="BookTradeConsistencyGate not importable")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _gate(
    *,
    enabled: bool = True,
    mode: str = "auto",
    max_book_staleness_ms: float = 1000.0,
    outside_bbo_eps_bps: float = 1.0,
    adverse_cross_bps: float = 1.5,
    veto_on_stale_book: bool = True,
    veto_on_adverse_cross: bool = True,
) -> BookTradeConsistencyGate:
    return BookTradeConsistencyGate(
        enabled=enabled,
        mode=mode,
        max_book_staleness_ms=max_book_staleness_ms,
        outside_bbo_eps_bps=outside_bbo_eps_bps,
        adverse_cross_bps=adverse_cross_bps,
        veto_on_stale_book=veto_on_stale_book,
        veto_on_adverse_cross=veto_on_adverse_cross,
    )


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------
class TestBookTradeConsistencyGate:

    def test_disabled_gate_returns_no_apply(self):
        """Disabled gate must return apply=False, veto=False unconditionally."""
        g = _gate(enabled=False)
        ctx = _Ctx(ts_event_ms=1_700_000_002_000, book_ts_ms=1_699_999_000_000)
        dec = g.evaluate(ctx=ctx, symbol="BTCUSDT")
        assert not dec.apply
        assert not dec.veto
        assert dec.reason_code == "OK"

    def test_stale_book_detected_as_flag(self):
        """When event_ts - book_ts > max_book_staleness_ms → stale_book flag."""
        g = _gate(max_book_staleness_ms=500.0)
        # Book is 2000 ms old relative to event.
        ctx = _Ctx(
            ts_event_ms=1_700_000_002_000,
            book_ts_ms=1_700_000_000_000,
            best_bid=100.0,
            best_ask=100.1,
            trade_px=100.05,
        )
        dec = g.evaluate(ctx=ctx, symbol="BTCUSDT")
        assert "stale_book" in dec.flags
        assert dec.apply
        # Monitor mode → no veto
        assert not dec.veto

    def test_adverse_cross_detected_as_flag(self):
        """Trade above ask by > eps_bps → adverse_cross flag."""
        g = _gate(outside_bbo_eps_bps=0.0, adverse_cross_bps=0.1)
        ctx = _Ctx(
            ts_event_ms=1_700_000_001_000,
            book_ts_ms=1_700_000_001_000,  # fresh book, no staleness
            best_bid=100.0,
            best_ask=100.1,
            trade_px=100.5,  # clearly above ask
        )
        dec = g.evaluate(ctx=ctx, symbol="ETHUSDT")
        assert "adverse_cross" in dec.flags
        assert dec.adverse_cross_bps > 0
        assert not dec.veto  # monitor mode

    def test_veto_stale_book_in_hard_mode(self, monkeypatch):
        """GATE_PROFILE=hard + stale book + veto_on_stale_book → VETO_BOOK_STALE."""
        monkeypatch.setenv("GATE_PROFILE", "hard")
        # mode=auto resolves to 'veto' when GATE_PROFILE=hard
        g = _gate(mode="auto", max_book_staleness_ms=500.0)
        ctx = _Ctx(
            ts_event_ms=1_700_000_002_000,
            book_ts_ms=1_700_000_000_000,  # 2000 ms stale
            best_bid=100.0,
            best_ask=100.1,
            trade_px=100.05,
        )
        dec = g.evaluate(ctx=ctx, symbol="BTCUSDT")
        assert dec.veto
        assert dec.reason_code == "VETO_BOOK_STALE"

    def test_fresh_book_trade_inside_bbo_no_flags(self):
        """Fresh book, trade inside spread → no flags, no veto."""
        g = _gate()
        ctx = _Ctx(
            ts_event_ms=1_700_000_001_000,
            book_ts_ms=1_700_000_001_000,  # exactly same time → staleness=0
            best_bid=100.0,
            best_ask=100.1,
            trade_px=100.05,
        )
        dec = g.evaluate(ctx=ctx, symbol="SOLUSDT")
        assert "stale_book" not in dec.flags
        assert "adverse_cross" not in dec.flags
        assert not dec.veto

    def test_missing_bbo_fail_open(self):
        """Missing bid/ask → missing_bbo flag, never veto, no crash."""
        g = _gate(mode="veto")
        ctx = _Ctx(
            ts_event_ms=1_700_000_001_000,
            book_ts_ms=1_700_000_001_000,
            best_bid=0.0,
            best_ask=0.0,
            trade_px=100.05,
        )
        dec = g.evaluate(ctx=ctx, symbol="BTCUSDT")
        assert "missing_bbo" in dec.flags
        # missing_bbo alone never triggers a veto — it's a monitoring flag only
        assert not dec.veto

    def test_combined_stale_and_adverse_cross_in_veto_mode(self, monkeypatch):
        """Both stale + adverse cross in veto mode → combined VETO_BOOK_STALE_ADVERSE_CROSS."""
        monkeypatch.setenv("GATE_PROFILE", "hard")
        g = _gate(
            mode="auto",
            max_book_staleness_ms=500.0,
            outside_bbo_eps_bps=0.0,
            adverse_cross_bps=0.1,
        )
        ctx = _Ctx(
            ts_event_ms=1_700_000_002_000,
            book_ts_ms=1_700_000_000_000,  # 2000 ms stale
            best_bid=100.0,
            best_ask=100.1,
            trade_px=101.0,  # above ask by >>0.1 bps
        )
        dec = g.evaluate(ctx=ctx, symbol="BTCUSDT")
        assert dec.veto
        assert dec.reason_code == "VETO_BOOK_STALE_ADVERSE_CROSS"

    def test_monitor_mode_no_veto_even_when_stale(self):
        """Explicit mode=monitor → never veto even if stale threshold exceeded."""
        g = _gate(mode="monitor", max_book_staleness_ms=100.0)
        ctx = _Ctx(
            ts_event_ms=1_700_000_002_000,
            book_ts_ms=1_700_000_000_000,  # 2000 ms stale
            best_bid=100.0,
            best_ask=100.1,
            trade_px=100.05,
        )
        dec = g.evaluate(ctx=ctx, symbol="BTCUSDT")
        assert "stale_book" in dec.flags
        assert not dec.veto  # monitor = no veto

    def test_from_env_creates_valid_gate(self, monkeypatch):
        """from_env() should produce a valid gate without raising."""
        monkeypatch.setenv("BOOK_TRADE_CONSISTENCY_ENABLED", "true")
        monkeypatch.setenv("BOOK_TRADE_CONSISTENCY_MODE", "auto")
        monkeypatch.setenv("BOOK_TRADE_CONSISTENCY_MAX_BOOK_STALENESS_MS", "1200")
        g = BookTradeConsistencyGate.from_env()
        assert g.enabled is True
        assert g.max_book_staleness_ms == 1200.0

    def test_decision_dataclass_fields(self):
        """BookTradeConsistencyDecision has expected fields with correct types."""
        g = _gate(enabled=False)
        ctx = _Ctx()
        d = g.evaluate(ctx=ctx, symbol="X")
        assert isinstance(d.apply, bool)
        assert isinstance(d.veto, bool)
        assert isinstance(d.reason_code, str)
        assert isinstance(d.flags, list)
        assert isinstance(d.book_staleness_ms, float)
        assert isinstance(d.adverse_cross_bps, float)
        assert isinstance(d.stream, str)
        assert math.isfinite(d.book_staleness_ms)
        assert math.isfinite(d.adverse_cross_bps)
