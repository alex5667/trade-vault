"""Unit tests for Plan 3.4 cross-asset BTC-drop LONG-block gate.

Covers:
  - master switch OFF (default) → no-op
  - shadow mode → no veto, ctx annotated, soft-flag added
  - enforce mode → veto with reason VETO_BTC_DROP_BLOCK_LONG
  - LONG-only (SHORT passes even on big BTC drop)
  - exempt symbols (BTCUSDT) never blocked
  - threshold respected (drop above threshold passes)
  - ctx.indicators primary source (treats exact 0.0 as missing)
  - reader fallback when ctx indicator missing
  - data-missing → fail-open
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate


def _ctx(*, btc_ret_5m: float | None = None) -> SimpleNamespace:
    ind: dict[str, Any] = {}
    if btc_ret_5m is not None:
        ind["btc_ret_5m"] = btc_ret_5m
    return SimpleNamespace(
        spread_bps=5.0,
        burst_flip_ratio=0.0,
        cancel_to_trade=0.0,
        indicators=ind,
    )


def _patch_reader_returns(monkeypatch, value: float | None) -> None:
    """Patch BOTH the reader module and the symbol re-imported by entry_policy_gate."""
    monkeypatch.setattr(
        "core.btc_drop_reader.get_btc_ret_5m", lambda: value, raising=True,
    )
    monkeypatch.setattr(
        "handlers.crypto_orderflow.utils.entry_policy_gate.get_btc_ret_5m",
        lambda: value, raising=True,
    )


# ────────────────────────────────────────────────────────────────────────────
# Master switch
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_gate_disabled_by_default(monkeypatch):
    """Default OFF — even a -5% BTC drop must NOT trigger anything."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.delenv("BTC_DROP_BLOCK_LONG_ENABLED", raising=False)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=-0.05)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="LONG")

    assert d.veto is False
    assert getattr(ctx, "btc_drop_block_long_alarm", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# Shadow mode
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_shadow_annotates_no_veto(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "shadow")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_PCT_5M", "-0.01")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=-0.012)  # -1.2% drop, exceeds -1% threshold
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="LONG")

    assert d.veto is False
    assert getattr(ctx, "btc_drop_block_long_alarm", 0) == 1
    assert getattr(ctx, "btc_drop_block_long_mode", "") == "shadow"
    assert abs(getattr(ctx, "btc_drop_block_long_btc_ret_5m", 0.0) + 0.012) < 1e-9
    notes = getattr(ctx, "btc_drop_block_long_notes", "")
    assert "btc_ret_5m=" in notes
    assert "src=ctx" in notes
    # Soft flag added for downstream EdgeCostGate tightening
    flags = getattr(ctx, "entry_policy_flags", [])
    assert any("btc_drop_block_long" in f for f in flags)


# ────────────────────────────────────────────────────────────────────────────
# Enforce mode
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_enforce_vetoes_long(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    # Even with default profile (which normally never vetoes), gate-local enforce wins.
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_PCT_5M", "-0.01")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=-0.015)
    d = g.evaluate(ctx=ctx, symbol="SOLUSDT", kind="breakout", side="LONG")

    assert d.veto is True
    assert d.reason_code == "VETO_BTC_DROP_BLOCK_LONG"
    assert "btc_ret_5m=" in d.notes


# ────────────────────────────────────────────────────────────────────────────
# Threshold respected
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_above_threshold_passes(monkeypatch):
    """A -0.5% drop must NOT trigger when threshold is -1%."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_PCT_5M", "-0.01")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=-0.005)  # only -0.5%
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="LONG")

    assert d.veto is False
    assert getattr(ctx, "btc_drop_block_long_alarm", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# LONG-only
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_does_not_block_short(monkeypatch):
    """SHORT signals must pass even on a big BTC drop — gate is LONG-only."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=-0.03)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "btc_drop_block_long_alarm", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# Exempt symbols
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_exempt_btc_itself(monkeypatch):
    """BTCUSDT is exempt by default — trading BTC on a BTC drop is a different
    setup (mean-reversion), not the alt-block scenario this gate addresses."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=-0.03)
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")

    assert d.veto is False
    assert getattr(ctx, "btc_drop_block_long_alarm", 0) == 0


def test_btc_drop_exempt_csv_override(monkeypatch):
    """Custom exempt list via ENV — ETHUSDT can be exempted too if desired."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_EXEMPT", "BTCUSDT,ETHUSDT")

    g = EntryPolicyGate.from_env()
    # ETH exempt
    d_eth = g.evaluate(ctx=_ctx(btc_ret_5m=-0.03), symbol="ETHUSDT",
                       kind="breakout", side="LONG")
    assert d_eth.veto is False
    # SOL not exempt → vetoes
    d_sol = g.evaluate(ctx=_ctx(btc_ret_5m=-0.03), symbol="SOLUSDT",
                       kind="breakout", side="LONG")
    assert d_sol.veto is True


# ────────────────────────────────────────────────────────────────────────────
# Data sources: ctx indicator primary, reader fallback, missing → fail-open
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_ctx_indicator_takes_priority(monkeypatch):
    """ctx.indicators['btc_ret_5m'] is used when present — reader is NOT called."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")

    # Reader would return None — but ctx has -3%
    _patch_reader_returns(monkeypatch, None)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=-0.03)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="LONG")

    assert d.veto is True
    assert "src=ctx" in d.notes


def test_btc_drop_reader_fallback_when_ctx_missing(monkeypatch):
    """No btc_ret_5m in ctx → fall back to live reader."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")

    _patch_reader_returns(monkeypatch, -0.02)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=None)  # ctx has no indicator
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="LONG")

    assert d.veto is True
    assert "src=reader" in d.notes


def test_btc_drop_ctx_zero_treated_as_missing(monkeypatch):
    """Exact 0.0 in ctx is warm-up artefact → fall through to reader."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")

    _patch_reader_returns(monkeypatch, -0.02)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.0)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="LONG")

    assert d.veto is True
    assert "src=reader" in d.notes


def test_btc_drop_no_data_fail_open(monkeypatch):
    """Neither ctx nor reader yields data → pass-through (fail-open)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "1")
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_MODE", "enforce")

    _patch_reader_returns(monkeypatch, None)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=None)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", side="LONG")

    assert d.veto is False
    assert getattr(ctx, "btc_drop_block_long_alarm", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# Reader unit tests
# ────────────────────────────────────────────────────────────────────────────

def test_btc_drop_reader_insufficient_data():
    """Empty ticks → None (caller fails open)."""
    from core.btc_drop_reader import _BtcDropReader

    r = _BtcDropReader()
    # Force redis factory to return None — no data available
    r._redis_factory = lambda: None
    assert r.btc_ret_5m() is None


def test_btc_drop_reader_computes_drop():
    """Synthetic tick history: 5m ago px=100, now px=98 → ret = -0.02."""
    from core.btc_drop_reader import _BtcDropReader

    r = _BtcDropReader()
    now_ms = 1_700_000_000_000
    # 6 minutes of "ticks" spaced 1s apart, with a smooth -2% drift.
    span_ms = 6 * 60 * 1000
    n = 360
    # Synthesise ticks linear from 100 → 98 over the full span
    ticks: list[tuple[int, float]] = []
    for i in range(n):
        ts = now_ms - span_ms + int(i * span_ms / (n - 1))
        px = 100.0 - 2.0 * (i / (n - 1))
        ticks.append((ts, px))
    r._ticks = ticks
    r._last_refresh = 9_999_999_999.0  # bypass refresh
    # Bypass the time.time() cutoff inside refresh by setting cached value path
    # — call directly:
    ret = r.btc_ret_5m()
    assert ret is not None
    # Drop over last 5m of a 6m linear drift = -2% × (5/6) ≈ -1.667%
    assert -0.018 < ret < -0.015


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
