import types

from common.ctx_cache import cached_on_ctx


class DummyConsistencyGate:
    def __init__(self):
        self.calls = 0

    def evaluate(self, *, ctx, symbol, kind, side):
        self.calls += 1
        return types.SimpleNamespace(apply=True, veto=False, reason_code="OK", notes="")

class DummyEdgeCostGate:
    def __init__(self, veto=False):
        self.calls = 0
        self.veto = veto

    def evaluate(self, *, ctx, kind, symbol):
        self.calls += 1
        return types.SimpleNamespace(apply=True, veto=self.veto, reason_code="VETO_EDGE_COST" if self.veto else "OK")


def cached_on_ctx(ctx, *, slot: str, key, compute):
    d = getattr(ctx, slot, None)
    if not isinstance(d, dict):
        d = {}
        setattr(ctx, slot, d)
    if key in d:
        return d[key]
    v = compute()
    d[key] = v
    return v


class MiniHandler:
    def __init__(self):
        self.symbol = "BTCUSDT"
        self._consistency_gate = DummyConsistencyGate()
        self._cost_edge_gate = DummyEdgeCostGate(veto=False)

    # minimal cache infra
    def _ctx_cache(self, ctx):
        c = getattr(ctx, "_gate_cache", None)
        if not isinstance(c, dict):
            c = {}
            ctx._gate_cache = c
        return c

    @staticmethod
    def _cfg_hash(cfg: dict) -> str:
        import json
        s = json.dumps(cfg or {}, sort_keys=True, separators=(",", ":"))
        import hashlib
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    def _consistency_cached(self, *, ctx, symbol: str, kind: str, side: str):
        gate = getattr(self, "_consistency_gate", None)
        fn = getattr(gate, "evaluate", None) if gate is not None else None
        if not callable(fn):
            return types.SimpleNamespace(apply=False, veto=False, reason_code="OK", notes="no_gate")

        key = (symbol.upper(), kind.lower(), side.upper())

        def _compute():
            return fn(ctx=ctx, symbol=symbol, kind=kind, side=side)

        return cached_on_ctx(ctx, slot="_cache_consistency_decision", key=key, compute=_compute)

    def _edge_cost_cached(self, *, ctx, kind: str, symbol: str, side: str, cfg: dict | None):
        gate = getattr(self, "_cost_edge_gate", None)
        fn = getattr(gate, "evaluate", None) if gate is not None else None
        if not callable(fn):
            return None

        cfgd = dict(cfg or {})
        ck = ("edge_cost", symbol.upper(), kind.lower(), side.upper(), self._cfg_hash(cfgd))
        cache = self._ctx_cache(ctx)
        if ck in cache:
            return cache[ck]
        d = fn(ctx=ctx, kind=kind, symbol=symbol)
        cache[ck] = d
        return d


def test_consistency_gate_called_once_per_ctx():
    h = MiniHandler()
    ctx = types.SimpleNamespace()

    d1 = h._consistency_cached(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    d2 = h._consistency_cached(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")

    assert d1 is d2
    assert h._consistency_gate.calls == 1

def test_edge_cost_gate_called_once_per_key():
    h = MiniHandler()
    ctx = types.SimpleNamespace()
    cfg = {"TP_RR": 1.5}

    d1 = h._edge_cost_cached(ctx=ctx, kind="breakout", symbol="BTCUSDT", side="LONG", cfg=cfg)
    d2 = h._edge_cost_cached(ctx=ctx, kind="breakout", symbol="BTCUSDT", side="LONG", cfg=cfg)

    assert d1 is d2
    assert h._cost_edge_gate.calls == 1

def test_edge_cost_gate_not_reused_if_cfg_changes():
    h = MiniHandler()
    ctx = types.SimpleNamespace()
    cfg1 = {"TP_RR": 1.5}
    cfg2 = {"TP_RR": 2.0}

    h._edge_cost_cached(ctx=ctx, kind="breakout", symbol="BTCUSDT", side="LONG", cfg=cfg1)
    h._edge_cost_cached(ctx=ctx, kind="breakout", symbol="BTCUSDT", side="LONG", cfg=cfg2)

    assert h._cost_edge_gate.calls == 2
