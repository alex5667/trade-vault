from types import SimpleNamespace

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


def _ctx(entry=None, tp1=None, sl=None, spread_bps=None, p=None, n=None):
    ctx = SimpleNamespace()
    if entry is not None:
        ctx.entry_price = entry
        ctx.entry = entry
        ctx.price = entry
    if tp1 is not None:
        ctx.tp1_price = tp1
        ctx.tp1 = tp1
    if sl is not None:
        ctx.sl_price = sl
        ctx.sl = sl
    if spread_bps is not None:
        ctx.spread_bps = float(spread_bps)
    if p is not None:
        ctx.tp1_hit_prob = float(p)
    if n is not None:
        ctx.tp1_hit_n = int(n)
    return ctx


def _base_env(monkeypatch):
    # Детеминизм: не используем EMA/Redis в тестах
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")


def test_skip_when_disabled(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "0")

    gate = EdgeCostGate.from_env()
    d = gate.evaluate(ctx=_ctx(entry=100, tp1=101), kind="absorption", symbol="BTCUSDT")

    assert d.apply is False
    assert d.veto is False
    assert d.reason_code == EdgeCostGate.REASON_SKIP


def test_pass_tp1_ok(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "1")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")

    gate = EdgeCostGate.from_env()
    d = gate.evaluate(ctx=_ctx(entry=100, tp1=101), kind="absorption", symbol="BTCUSDT")

    assert d.apply is True
    assert d.veto is False
    assert d.reason_code == EdgeCostGate.REASON_OK
    assert gate.passes(ctx=_ctx(entry=100, tp1=101), kind="absorption", symbol="BTCUSDT") is True


def test_veto_below_k_tp1(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "1000")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "10")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "10")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")

    gate = EdgeCostGate.from_env()
    d = gate.evaluate(ctx=_ctx(entry=100, tp1=100.50), kind="absorption", symbol="BTCUSDT")

    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_BELOW_K
    assert gate.passes(ctx=_ctx(entry=100, tp1=100.50), kind="absorption", symbol="BTCUSDT") is False


def test_veto_missing_levels_when_strict(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "10")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "10")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")

    gate = EdgeCostGate.from_env()
    d = gate.evaluate(ctx=_ctx(entry=100, tp1=None), kind="absorption", symbol="BTCUSDT")

    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_MISSING_LEVELS


def test_ev_prob_veto(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "ev")
    monkeypatch.setenv("EDGE_EV_MIN_TRADES", "10")
    monkeypatch.setenv("EDGE_EV_P_MIN", "0.60")
    monkeypatch.setenv("EDGE_COST_K", "1")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")

    gate = EdgeCostGate.from_env()
    # p=0.50 < p_min=0.60 -> veto
    ctx = _ctx(entry=100, tp1=101, sl=99, p=0.50, n=100)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_EV_PROB
