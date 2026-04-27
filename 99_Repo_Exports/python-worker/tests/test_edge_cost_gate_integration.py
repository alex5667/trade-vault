from __future__ import annotations

from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    """
    Set up deterministic ENV for tests.
    Derive fees from CRYPTO_COMMISSION_RATE (0.0004 -> 8 bps round trip).
    Disable EMA / Redis deps so tests are self-contained.
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "4.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    # Derive fees from commission rate: 0.0004 -> 8 bps round trip
    monkeypatch.setenv("CRYPTO_COMMISSION_RATE", "0.0004")
    # Unset EDGE_FEES_BPS_DEFAULT so it derives from commission rate
    monkeypatch.delenv("EDGE_FEES_BPS_DEFAULT", raising=False)
    # Disable EMA/drift so tests don't depend on Redis
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")


def test_cost_gate_derives_fees_from_commission_rate():
    """
    Verify that EDGE_FEES_BPS_DEFAULT is correctly derived from CRYPTO_COMMISSION_RATE.
    0.0004 commission => 4 bps one-way => 8 bps round-trip
    """
    gate = EdgeCostGate.from_env()
    assert gate.fees_bps_default == 8.0


def test_cost_gate_veto_when_tp1_too_close():
    """
    Verify gate vetoes when expected_move_bps < K * (fees_bps + slippage_bps).

    Setup:
      - K = 4.0
      - fees = 8 bps (round-trip)
      - slippage = 4 bps (no spread, default only)
      - threshold = 4 * (8 + 4) = 48 bps
      - expected = |100.20 - 100.00| / 100.00 * 10000 = 20 bps

    Expected: veto (20 < 48)
    """
    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace(symbol="BTCUSDT", entry_price=100.0, tp1_price=100.30)

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert dec.apply is True
    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_BELOW_K
    assert dec.expected_move_bps == pytest.approx(30.0, rel=1e-6)
    assert dec.threshold_bps == pytest.approx(48.0, rel=1e-6)


def test_cost_gate_pass_when_tp1_sufficient():
    """
    Verify gate passes when expected_move_bps >= K * (fees_bps + slippage_bps).

    Setup:
      - K = 4.0
      - fees = 8 bps
      - slippage = 4 bps (default, no spread)
      - threshold = 4 * (8 + 4) = 48 bps
      - expected = |100.60 - 100.00| / 100.00 * 10000 = 60 bps

    Expected: pass (60 >= 48)
    """
    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace(symbol="BTCUSDT", entry_price=100.0, tp1_price=100.60)

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert dec.apply is True
    assert dec.veto is False
    assert dec.reason_code == EdgeCostGate.REASON_OK
    assert dec.expected_move_bps == pytest.approx(60.0, rel=1e-6)
    assert dec.threshold_bps == pytest.approx(48.0, rel=1e-6)


def test_cost_gate_uses_ctx_spread_bps(monkeypatch):
    """
    Verify gate correctly reads spread_bps from ctx directly.
    spread_bps=20 -> slippage = max(4, 20/2) = 10 bps.
    """
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")
    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace(symbol="BTCUSDT", entry_price=100.0, tp1_price=100.50, spread_bps=20.0)

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    # slippage = max(4, 20/2) = 10 bps
    # threshold = 4 * (8 + 10) = 72 bps
    # expected = 30 bps => veto
    assert dec.slippage_bps == pytest.approx(10.0, abs=1e-9)
    assert dec.threshold_bps == pytest.approx(72.0, abs=1e-9)
    assert dec.veto is True


def test_cost_gate_uses_ctx_of_spread_bps(monkeypatch):
    """
    Verify gate reads spread_bps from ctx.of when not set on ctx directly.
    ctx.of.spread_bps=20 -> slippage = max(4, 20/2) = 10 bps.
    """
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")
    gate = EdgeCostGate.from_env()

    of = SimpleNamespace(price=100.0, atr=2.0, spread_bps=20.0)
    # No spread_bps on ctx itself — must fall through to ctx.of
    ctx = SimpleNamespace(of=of, symbol="BTCUSDT", entry_price=100.0, tp1_price=100.50)

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert dec.slippage_bps == pytest.approx(10.0, abs=1e-9)
    assert dec.threshold_bps == pytest.approx(72.0, abs=1e-9)
    assert dec.veto is True


def test_cost_gate_missing_levels_fail_open(monkeypatch):
    """
    Verify gate fails open when levels are missing and EDGE_COST_STRICT_MISSING_LEVELS=0.
    """
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "0")
    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace(symbol="BTCUSDT")

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert dec.apply is True
    assert dec.veto is False
    assert dec.reason_code == EdgeCostGate.REASON_OK


def test_cost_gate_missing_levels_fail_closed(monkeypatch):
    """
    Verify gate fails closed when levels are missing and EDGE_COST_STRICT_MISSING_LEVELS=1.
    """
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")
    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace(symbol="BTCUSDT")

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert dec.apply is True
    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_MISSING_LEVELS
