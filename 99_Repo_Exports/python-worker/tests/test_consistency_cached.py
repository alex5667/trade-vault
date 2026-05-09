import types


class DummyGate:
    def __init__(self): self.n = 0
    def evaluate(self, *, ctx, symbol, kind, side):
        self.n += 1
        return types.SimpleNamespace(veto=False, reason_code="OK")

def test_consistency_cached():
    from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler

    h = CryptoOrderFlowHandler.__new__(CryptoOrderFlowHandler)
    h._consistency_gate = DummyGate()
    h.logger = None

    ctx = types.SimpleNamespace()
    d1 = h._consistency_gate_cached(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    d2 = h._consistency_gate_cached(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert d1 is d2
    assert h._consistency_gate.n == 1

    _ = h._consistency_gate_cached(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="SHORT")
    assert h._consistency_gate.n == 2
