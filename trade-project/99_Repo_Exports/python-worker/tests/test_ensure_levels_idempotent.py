import types


def test_ensure_levels_idempotent(monkeypatch):
    # Adjust import to your real module path
    import handlers.base_orderflow_handler as mod

    calls = {"safe_float": 0}

    orig = mod._safe_float_pos

    def _wrap(x):
        calls["safe_float"] += 1
        return orig(x)

    monkeypatch.setattr(mod, "_safe_float_pos", _wrap)

    ctx = types.SimpleNamespace()
    ctx.price = 100.0
    ctx.side = "LONG"

    mod.ensure_levels(ctx, side=1)
    first = calls["safe_float"]

    # Second call must do near-zero work (guard triggers)
    mod.ensure_levels(ctx, side=1)
    second = calls["safe_float"]

    assert getattr(ctx, "_levels_attached", False) is True
    assert second == first, "ensure_levels() should not redo parsing work on repeated calls"
