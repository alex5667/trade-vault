"""
Test suite for EdgeCostGate EV-mode.

Covers:
  - Canonical reason_code values (regression-proofed against REASON_* constants)
  - ENV contracts: EDGE_EV_P_MIN, EDGE_EV_P_MIN_{KIND}, EDGE_EV_MIN_TRADES
  - Cold-start: fail-open and fail-closed paths
  - Missing inputs: fail-open and fail-closed paths
  - EV formula numerics with exact bps arithmetic
  - EDGE_EV_STRICT_MISSING_STATS=0/1

All tests use _base_env() to isolate from Redis/EMA/drift, and do NOT
reload the module (avoids Prometheus duplicate metric registration errors).
Gate is reconstructed per-test via EdgeCostGate.from_env() after monkeypatching.
"""
from types import SimpleNamespace

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_env(monkeypatch):
    """Deterministic baseline: no Redis, no EMA, no drift tighten."""
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_EXEC_HEALTH_MODE", "off")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")


def _ev_env(monkeypatch, *, p_min="0.55", min_trades="40", strict="0",
            k="1.0", fees="8.0", slip="4.0", kinds="breakout"):
    """EV-mode base ENV."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "ev")
    monkeypatch.setenv("EDGE_EV_P_MIN", p_min)
    monkeypatch.setenv("EDGE_EV_MIN_TRADES", min_trades)
    monkeypatch.setenv("EDGE_EV_STRICT_MISSING_STATS", strict)
    monkeypatch.setenv("EDGE_COST_K", k)
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", fees)
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", slip)
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", kinds)


def _mk_ctx(*, entry=100.0, tp1=100.6, sl=99.7, p=0.60, n=100):
    """
    Minimal ctx with all EV inputs present.
    Defaults: tp1_bps = 60bps, stop_bps = 30bps (entry=100).
    EV(p=0.60) = 0.60*60 - 0.40*30 = 36 - 12 = 24 bps.
    """
    ctx = SimpleNamespace()
    ctx.entry_price = entry
    ctx.tp1_price = tp1
    ctx.sl_price = sl
    ctx.tp1_hit_prob = float(p)
    ctx.tp1_hit_n = int(n)
    ctx.tp1_hit_src = "ema"
    ctx.spread_bps = 0.0
    return ctx


# ---------------------------------------------------------------------------
# Test 1 (fix): EV < K×costs → REASON_EV_BELOW_K
# ---------------------------------------------------------------------------
def test_ev_veto_by_ev_below_k(monkeypatch):
    """
    EV_bps=18 < threshold=24 → veto with REASON_EV_BELOW_K.

    Arithmetic:
      entry=100, tp1=100.5, sl=99.7
      tp1_bps = 0.5/100*10000 = 50 bps
      stop_bps = 0.3/100*10000 = 30 bps
      p=0.60 → EV = 0.60*50 - 0.40*30 = 30 - 12 = 18 bps
      K=2, fees=8, slip=4 → threshold = 2*(8+4) = 24 bps
      18 < 24 → veto
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, p_min="0.55", min_trades="40", strict="0",
            k="2.0", fees="8.0", slip="4.0")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(entry=100.0, tp1=100.5, sl=99.7, p=0.60, n=100)
    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.apply is True
    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_EV_BELOW_K, (
        f"Got {dec.reason_code!r}, expected {EdgeCostGate.REASON_EV_BELOW_K!r}"
    )
    assert dec.mode == "ev"
    # Numeric sanity
    assert abs(dec.ev_bps - 18.0) < 0.5, f"ev_bps={dec.ev_bps}"
    assert abs(dec.threshold_bps - 24.0) < 0.5, f"threshold_bps={dec.threshold_bps}"
    assert abs(dec.tp1_bps - 50.0) < 1.0, f"tp1_bps={dec.tp1_bps}"
    assert abs(dec.stop_bps - 30.0) < 1.0, f"stop_bps={dec.stop_bps}"
    assert abs(dec.p_hit_tp1 - 0.60) < 1e-6, f"p_hit_tp1={dec.p_hit_tp1}"
    assert dec.passed is False  # compat property


# ---------------------------------------------------------------------------
# Test 2 (fix): p < p_min → REASON_EV_PROB
# ---------------------------------------------------------------------------
def test_ev_veto_by_prob(monkeypatch):
    """
    p=0.69 < p_min=0.70 → veto with REASON_EV_PROB.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, p_min="0.70", min_trades="10", strict="0")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(p=0.69, n=999)
    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_EV_PROB, (
        f"Got {dec.reason_code!r}, expected {EdgeCostGate.REASON_EV_PROB!r}"
    )
    assert abs(dec.p_hit_tp1 - 0.69) < 1e-6
    assert abs(dec.p_min - 0.70) < 1e-6


# ---------------------------------------------------------------------------
# Test 3 (fix): cold-start fail-open → REASON_OK
# ---------------------------------------------------------------------------
def test_ev_fail_open_insufficient_stats(monkeypatch):
    """
    n=10 < EDGE_EV_MIN_TRADES=50, strict=0 → fail-open, reason=REASON_OK.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, min_trades="50", strict="0")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(n=10)
    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.veto is False
    assert dec.reason_code == EdgeCostGate.REASON_OK, (
        f"Got {dec.reason_code!r}, expected {EdgeCostGate.REASON_OK!r}"
    )
    assert "insufficient_stats_fail_open" in dec.notes, f"notes={dec.notes!r}"
    assert dec.stats_n == 10


# ---------------------------------------------------------------------------
# Test 4 (new): p = 0.549 < 0.55 (default boundary, just below)
# ---------------------------------------------------------------------------
def test_ev_p_min_default_boundary_below(monkeypatch):
    """
    p=0.549 is ONE tick below the default p_min=0.55 → must veto with REASON_EV_PROB.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, p_min="0.55", min_trades="10", strict="0")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(p=0.549, n=999)
    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_EV_PROB


# ---------------------------------------------------------------------------
# Test 5 (new): p = 0.550 >= 0.55 (at boundary, must pass prob check)
# ---------------------------------------------------------------------------
def test_ev_p_min_default_boundary_at(monkeypatch):
    """
    p=0.550 meets or exceeds default p_min=0.55 → must NOT veto due to prob.
    (May still veto if EV < K×costs, but reason_code must not be REASON_EV_PROB.)
    Use K=0 / fees=0 / slip=0 so threshold=0 and EV check trivially passes.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, p_min="0.55", min_trades="10", strict="0",
            k="0.0", fees="0.0", slip="0.0")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(p=0.550, n=999)
    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.reason_code != EdgeCostGate.REASON_EV_PROB, (
        "p=0.55 should meet p_min=0.55 but got REASON_EV_PROB"
    )


# ---------------------------------------------------------------------------
# Test 6 (new): per-kind p_min override – breakout uses higher threshold
# ---------------------------------------------------------------------------
def test_ev_per_kind_p_min_breakout(monkeypatch):
    """
    EDGE_EV_P_MIN_BREAKOUT=0.70, global p_min=0.55.
    kind=breakout, p=0.65 < 0.70 → REASON_EV_PROB.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, p_min="0.55", min_trades="10", strict="0",
            kinds="breakout,absorption")
    monkeypatch.setenv("EDGE_EV_P_MIN_BREAKOUT", "0.70")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(p=0.65, n=999)
    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_EV_PROB
    assert abs(dec.p_min - 0.70) < 1e-6, f"Expected p_min=0.70. Got {dec.p_min}"


# ---------------------------------------------------------------------------
# Test 7 (new): per-kind p_min override – OTHER kind uses global threshold
# ---------------------------------------------------------------------------
def test_ev_per_kind_p_min_other_kind(monkeypatch):
    """
    EDGE_EV_P_MIN_BREAKOUT=0.70, global p_min=0.55.
    kind=absorption, p=0.65 → uses global 0.55 → no prob veto.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, p_min="0.55", min_trades="10", strict="0",
            k="0.0", fees="0.0", slip="0.0",
            kinds="breakout,absorption")
    monkeypatch.setenv("EDGE_EV_P_MIN_BREAKOUT", "0.70")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(p=0.65, n=999)
    dec = gate.evaluate(ctx=ctx, kind="absorption", symbol="BTCUSDT")

    assert dec.reason_code != EdgeCostGate.REASON_EV_PROB, (
        "absorption should use global p_min=0.55, not breakout override=0.70"
    )
    assert abs(dec.p_min - 0.55) < 1e-6, f"Expected global p_min=0.55. Got {dec.p_min}"


# ---------------------------------------------------------------------------
# Test 8 (new): cold-start fail-CLOSED (strict=1) → REASON_EV_INSUFFICIENT_STATS
# ---------------------------------------------------------------------------
def test_ev_cold_start_fail_closed(monkeypatch):
    """
    n=5 < EDGE_EV_MIN_TRADES=40, EDGE_EV_STRICT_MISSING_STATS=1 →
    veto with REASON_EV_INSUFFICIENT_STATS, stats_n=5 surfaced in decision.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, min_trades="40", strict="1")

    gate = EdgeCostGate.from_env()
    ctx = _mk_ctx(p=0.80, n=5)  # high p but too few samples
    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_EV_INSUFFICIENT_STATS, (
        f"Got {dec.reason_code!r}"
    )
    assert dec.stats_n == 5


# ---------------------------------------------------------------------------
# Test 9 (new): missing tp1/sl – fail-open
# ---------------------------------------------------------------------------
def test_ev_missing_inputs_fail_open(monkeypatch):
    """
    No tp1_price / sl_price on ctx, strict=0 → fail-open, REASON_OK.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, strict="0")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace()
    ctx.entry_price = 100.0
    ctx.tp1_hit_prob = 0.70
    ctx.tp1_hit_n = 100
    ctx.spread_bps = 0.0
    # tp1_price and sl_price intentionally omitted

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.veto is False
    assert dec.reason_code == EdgeCostGate.REASON_OK, f"Got {dec.reason_code!r}"
    assert "missing_ev_inputs_fail_open" in dec.notes, f"notes={dec.notes!r}"


# ---------------------------------------------------------------------------
# Test 10 (new): missing tp1/sl – fail-CLOSED (strict=1)
# ---------------------------------------------------------------------------
def test_ev_missing_inputs_fail_closed(monkeypatch):
    """
    No tp1_price / sl_price on ctx, strict=1 →
    veto with REASON_EV_MISSING_INPUTS.
    """
    _base_env(monkeypatch)
    _ev_env(monkeypatch, strict="1")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace()
    ctx.entry_price = 100.0
    ctx.tp1_hit_prob = 0.80
    ctx.tp1_hit_n = 100
    ctx.spread_bps = 0.0
    # tp1_price and sl_price intentionally omitted

    dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert dec.veto is True
    assert dec.reason_code == EdgeCostGate.REASON_EV_MISSING_INPUTS, (
        f"Got {dec.reason_code!r}"
    )
