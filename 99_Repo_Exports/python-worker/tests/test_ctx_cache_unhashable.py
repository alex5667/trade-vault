from types import SimpleNamespace
from common.ctx_cache import cached_on_ctx

def test_ctx_cache_unhashable_key():
    ctx = SimpleNamespace()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return 123

    # An unhashable key (dict)
    unhashable_key = {"a": 1}
    
    # First call - should compute without failing
    res1 = cached_on_ctx(ctx, slot="_slot", key=unhashable_key, compute=compute) # type: ignore
    assert res1 == 123
    assert calls["n"] == 1
    
    # Second call - cannot cache because it's unhashable, so it should quietly recompute
    res2 = cached_on_ctx(ctx, slot="_slot", key=unhashable_key, compute=compute) # type: ignore
    assert res2 == 123
    assert calls["n"] == 2
    
    # Ensure it didn't write garbage to ctx
    box = getattr(ctx, "_slot")
    assert isinstance(box, dict)
    assert len(box) == 0  # Nothing was cached
