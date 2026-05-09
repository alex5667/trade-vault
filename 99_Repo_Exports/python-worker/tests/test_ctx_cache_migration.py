from types import SimpleNamespace

from common.ctx_cache import cached_on_ctx


def test_ctx_cache_migration():
    ctx = SimpleNamespace()

    # 1. Setup old legacy format manually
    ctx._my_slot = {"key": ("old", "key"), "val": 42}

    # 2. Add an additional existing key in new format to check it's not destroyed
    ctx._my_slot[("new", "format")] = 100

    # 3. Call cached_on_ctx for the old key, to see if it migrates and hits cache
    calls = {"n": 0}
    def compute():
        calls["n"] += 1
        return 999  # If cache miss, returns 999

    res = cached_on_ctx(ctx, slot="_my_slot", key=("old", "key"), compute=compute)

    # 4. Asserts
    assert calls["n"] == 0, "Should have been a cache hit after migration"
    assert res == 42, "Should return migrated value"

    # Verify migration actually restructured the dict
    box = ctx._my_slot
    assert "key" not in box
    assert "val" not in box
    assert box[("old", "key")] == 42
    assert box[("new", "format")] == 100
