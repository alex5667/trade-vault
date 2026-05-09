import types

from common.ctx_cache import cached_on_ctx


class DummyDecision:
    def __init__(self, *, apply=True, veto=False, reason_code="OK"):
        self.apply = apply
        self.veto = veto
        self.reason_code = reason_code

def test_edge_cost_decision_logic_with_cfg_hash():
    """Test the logic of _edge_cost_decision_once with cfg hash in key"""

    def cfg_hash_stub(cfg: dict) -> str:
        # stable pseudo-hash for test
        return "A" if cfg.get("TP_RR") == 2 else "B"

    class MockHandler:
        def __init__(self):
            self._cost_edge_gate = DummyGate()
            self._edge_cost_gate = None

        def _cfg_hash(self, cfg: dict) -> str:
            return cfg_hash_stub(cfg)

        def _ensure_levels_once(self, ctx, *, side):
            """Mock ensure_levels_once"""
            pass

        def _ensure_trade_levels_once(self, **kwargs):
            """Mock ensure_trade_levels_once"""
            ctx = kwargs["ctx"]
            # Simulate attaching levels
            if not hasattr(ctx, "tp1_price"):
                ctx.tp1_price = 1.0
                ctx.sl_price = 0.5

    class DummyGate:
        def __init__(self):
            self.calls = 0

        def evaluate(self, *, ctx, kind: str, symbol: str):
            self.calls += 1
            return DummyDecision(apply=True, veto=False, reason_code="OK")

    # Simulate the _edge_cost_decision_once logic
    def _edge_cost_decision_once_simulated(h, ctx, symbol, kind, side, side_int=None, risk_cfg=None, regime=None, empirical=None):
        gate = h._cost_edge_gate
        fn = getattr(gate, "evaluate", None)
        if not callable(fn):
            return None

        try:
            cfgd = dict(risk_cfg or {})
        except Exception:
            cfgd = {}

        key = (symbol, str(kind), side, h._cfg_hash(cfgd))

        def _compute():
            # 1) Ensure invariants once
            h._ensure_levels_once(ctx, side=side_int if side_int in (1, -1) else side)

            # 2) Ensure deterministic levels once (only if missing, cached)
            try:
                if getattr(ctx, "tp1_price", None) is None or getattr(ctx, "sl_price", None) is None:
                    h._ensure_trade_levels_once(ctx=ctx, side=side, symbol=symbol, kind=str(kind), cfg=cfgd, regime=regime, empirical=empirical, overwrite=False, logger=None)
            except Exception:
                pass

            # 3) Evaluate gate once
            try:
                return fn(ctx=ctx, kind=str(kind), symbol=symbol)
            except Exception:
                return None

        return cached_on_ctx(ctx, slot="_cache_edge_cost_decision", key=key, compute=_compute)

    h = MockHandler()
    ctx = types.SimpleNamespace()

    # cfg A
    dec1 = _edge_cost_decision_once_simulated(h, ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG", side_int=1, risk_cfg={"TP_RR": 2})
    dec2 = _edge_cost_decision_once_simulated(h, ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG", side_int=1, risk_cfg={"TP_RR": 2})
    assert h._cost_edge_gate.calls == 1
    assert dec1 is dec2

    # cfg B -> should re-evaluate (key changed)
    dec3 = _edge_cost_decision_once_simulated(h, ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG", side_int=1, risk_cfg={"TP_RR": 3})
    assert h._cost_edge_gate.calls == 2
    assert dec3 is not None
