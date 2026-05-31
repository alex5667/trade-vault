"""Unit tests for BLOCKER-2: BTC-pump SHORT-block gate.

Covers:
  - master switch OFF → no-op (SHORT passes even on big pump)
  - shadow mode → no veto, ctx annotated, soft-flag added
  - enforce mode → veto with reason VETO_BTC_PUMP_BLOCK_SHORT
  - SHORT-only (LONG passes even on big BTC pump)
  - exempt symbols (BTCUSDT) never blocked
  - threshold respected (+0.5% pump below +0.7% threshold passes)
  - ctx.indicators primary source (exact 0.0 treated as missing)
  - reader fallback when ctx indicator missing
  - data-missing → fail-open (no veto)
  - annotates ctx fields when hit in shadow
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


def _patch_reader(monkeypatch, value: float | None) -> None:
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

def test_pump_block_disabled_by_default(monkeypatch):
    """Default OFF — even +5% BTC pump must NOT block anything."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.delenv("BTC_PUMP_BLOCK_SHORT_ENABLED", raising=False)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.05)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="iceberg", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "btc_pump_block_short_alarm", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# Shadow mode
# ────────────────────────────────────────────────────────────────────────────

def test_pump_block_shadow_annotates_no_veto(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "shadow")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.01)  # +1.0% pump, exceeds +0.7% threshold
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="iceberg", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "btc_pump_block_short_alarm", 0) == 1
    assert getattr(ctx, "btc_pump_block_short_mode", "") == "shadow"
    assert getattr(ctx, "btc_pump_block_short_btc_ret_5m", None) == pytest.approx(0.01)


# ────────────────────────────────────────────────────────────────────────────
# Enforce mode
# ────────────────────────────────────────────────────────────────────────────

def test_pump_block_enforce_vetoes_short(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.0104)  # +1.04% — matches the BLOCKER-2 incident pattern
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="iceberg", side="SHORT")

    assert d.veto is True
    assert d.reason_code == "VETO_BTC_PUMP_BLOCK_SHORT"


def test_pump_block_btcusdt_enforce_vetoes_btc_short(monkeypatch):
    """BTCUSDT in default exempt list → no veto even in enforce mode."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.02)
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="iceberg", side="SHORT")

    assert d.veto is False


# ────────────────────────────────────────────────────────────────────────────
# Direction filter — LONG must NEVER be affected
# ────────────────────────────────────────────────────────────────────────────

def test_pump_block_does_not_affect_long(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")
    # Also disable BTC_DROP_BLOCK so we isolate to pump-block only
    monkeypatch.setenv("BTC_DROP_BLOCK_LONG_ENABLED", "0")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.02)  # +2% pump
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="iceberg", side="LONG")

    assert d.veto is False
    assert getattr(ctx, "btc_pump_block_short_alarm", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# Threshold boundaries
# ────────────────────────────────────────────────────────────────────────────

def test_pump_below_threshold_passes(monkeypatch):
    """+0.5% pump is below +0.7% threshold → SHORT passes."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.005)  # +0.5%, below +0.7% threshold
    d = g.evaluate(ctx=ctx, symbol="SOLUSDT", kind="delta_spike", side="SHORT")

    assert d.veto is False


def test_pump_at_threshold_blocked(monkeypatch):
    """Exactly at threshold → blocked."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.007)  # exactly threshold
    d = g.evaluate(ctx=ctx, symbol="SOLUSDT", kind="delta_spike", side="SHORT")

    assert d.veto is True


# ────────────────────────────────────────────────────────────────────────────
# Data sources
# ────────────────────────────────────────────────────────────────────────────

def test_pump_block_uses_reader_fallback(monkeypatch):
    """When ctx indicator missing, falls back to btc_drop_reader (same ticker stream)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")
    _patch_reader(monkeypatch, 0.01)  # reader returns +1.0%

    g = EntryPolicyGate.from_env()
    ctx = _ctx()  # no btc_ret_5m in ctx
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="iceberg", side="SHORT")

    assert d.veto is True


def test_pump_block_missing_data_fail_open(monkeypatch):
    """No data in ctx and reader returns None → fail-open, no veto."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")
    _patch_reader(monkeypatch, None)

    g = EntryPolicyGate.from_env()
    ctx = _ctx()  # no indicator, reader returns None
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="iceberg", side="SHORT")

    assert d.veto is False


def test_pump_block_zero_indicator_treated_as_missing(monkeypatch):
    """Exact 0.0 in ctx treated as missing → falls back to reader."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_ENABLED", "1")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_MODE", "enforce")
    monkeypatch.setenv("BTC_PUMP_BLOCK_SHORT_PCT_5M", "0.007")
    _patch_reader(monkeypatch, 0.01)  # reader says +1%

    g = EntryPolicyGate.from_env()
    ctx = _ctx(btc_ret_5m=0.0)  # exact 0.0 → treated as missing
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="iceberg", side="SHORT")

    assert d.veto is True
