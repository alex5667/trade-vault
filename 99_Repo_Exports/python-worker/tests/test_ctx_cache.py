from types import SimpleNamespace

from common.ctx_cache import cached_on_ctx


def test_cached_on_ctx():
    ctx = SimpleNamespace()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return 123

    assert cached_on_ctx(ctx, slot="_x", key=("a",), compute=compute) == 123
    assert cached_on_ctx(ctx, slot="_x", key=("a",), compute=compute) == 123
    assert calls["n"] == 1
