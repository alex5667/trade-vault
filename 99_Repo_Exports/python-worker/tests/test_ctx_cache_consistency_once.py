import types

from common.ctx_cache import cached_on_ctx


def test_cached_on_ctx_returns_cached_value():
    ctx = types.SimpleNamespace()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return {"veto": False, "reason_code": "OK"}

    key = ("BTCUSDT", "breakout", "LONG")

    v1 = cached_on_ctx(ctx, slot="_cache_consistency_decision", key=key, compute=compute)
    v2 = cached_on_ctx(ctx, slot="_cache_consistency_decision", key=key, compute=compute)

    assert calls["n"] == 1
    assert v1 == v2


def test_cached_on_ctx_recomputes_on_key_change():
    ctx = types.SimpleNamespace()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return calls["n"]

    v1 = cached_on_ctx(ctx, slot="_cache", key=("A",), compute=compute)
    v2 = cached_on_ctx(ctx, slot="_cache", key=("B",), compute=compute)

    assert v1 == 1
    assert v2 == 2
