import types
import pytest

from common.gate_cache import cached_call, cached_call_exc


def test_cached_call_runs_once():
    ctx = types.SimpleNamespace()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return {"ok": True}

    v1 = cached_call(ctx, ("k", 1), compute)
    v2 = cached_call(ctx, ("k", 1), compute)
    v3 = cached_call(ctx, ("k", 1), compute)

    assert calls["n"] == 1
    assert v1 is v2 is v3


def test_cached_call_exc_caches_exception_and_reraises_without_recomputing():
    ctx = types.SimpleNamespace()
    calls = {"n": 0}

    class Boom(RuntimeError):
        pass

    def compute():
        calls["n"] += 1
        raise Boom("x")

    with pytest.raises(Boom):
        cached_call_exc(ctx, ("boom",), compute)
    with pytest.raises(Boom):
        cached_call_exc(ctx, ("boom",), compute)
    with pytest.raises(Boom):
        cached_call_exc(ctx, ("boom",), compute)

    # critical: the heavy function executed exactly once
    assert calls["n"] == 1


def test_cached_call_exc_caches_value():
    ctx = types.SimpleNamespace()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return object()

    v1 = cached_call_exc(ctx, ("v",), compute)
    v2 = cached_call_exc(ctx, ("v",), compute)

    assert calls["n"] == 1
    assert v1 is v2


# Тест для consistency caching (минимальный "gate"):

class DummyGate:
    def __init__(self):
        self.calls = 0
    def evaluate(self, **kwargs):
        self.calls += 1
        return types.SimpleNamespace(veto=False, reason_code="OK", notes="")

def test_consistency_evaluate_called_once_even_if_referenced_3_times():
    ctx = types.SimpleNamespace()
    gate = DummyGate()

    def eval1():
        return gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")

    key = ("consistency", "BTCUSDT", "breakout", "LONG")

    a = cached_call(ctx, key, eval1)
    b = cached_call(ctx, key, eval1)
    c = cached_call(ctx, key, eval1)

    assert gate.calls == 1
    assert a is b is c

# Тест для attach_trade_levels_once (гарантия "compute_levels не дергаем многократно"):

class DummyLevels:
    def __init__(self):
        self.calls = 0
    def attach(self, ctx):
        self.calls += 1
        setattr(ctx, "tp1_price", 123.0)

def test_attach_levels_called_once():
    ctx = types.SimpleNamespace()
    dl = DummyLevels()

    def do_attach():
        dl.attach(ctx)
        return getattr(ctx, "tp1_price", None) is not None

    key = ("attach_levels", "BTCUSDT", "breakout", "LONG")

    assert cached_call(ctx, key, do_attach) is True
    assert cached_call(ctx, key, do_attach) is True
    assert cached_call(ctx, key, do_attach) is True

    assert dl.calls == 1
