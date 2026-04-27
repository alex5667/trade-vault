from types import SimpleNamespace

import pytest

from common.ctx_cache import cached_on_ctx


def test_cached_on_ctx_compute_once():
    ctx = SimpleNamespace()
    calls = {"n": 0}
    def compute():
        calls["n"] += 1
        return {"ok": True}

    v1 = cached_on_ctx(ctx, slot="_slot", key=("k",), compute=compute)
    v2 = cached_on_ctx(ctx, slot="_slot", key=("k",), compute=compute)
    assert v1 == v2
    assert calls["n"] == 1
