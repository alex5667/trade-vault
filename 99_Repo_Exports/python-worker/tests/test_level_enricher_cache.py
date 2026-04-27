import types
import os

def test_attach_levels_caches_floor_skip(monkeypatch):
    # module under test
    from signals import level_enricher as le

    calls = {"n": 0}

    def fake_compute_levels(entry, atr, side, cfg, stop_dist_override=None, tp1_dist_override=None):
        calls["n"] += 1
        # intentionally tiny stop to trigger floor
        return {"sl": entry - 0.01, "tp_levels": [entry + 0.01], "stop_dist": 0.01, "rr": [1.0]}

    monkeypatch.setattr(le, "compute_levels", fake_compute_levels)

    # force floor to be higher than produced stop_bps
    monkeypatch.setenv("EDGE_LEVELS_MIN_STOP_BPS", "50")  # big enough
    monkeypatch.delenv("EDGE_LEVELS_MIN_TP1_BPS", raising=False)

    ctx = types.SimpleNamespace(price=100.0, atr=1.0)

    le.attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg={"X": 1}, kind="breakout", overwrite=False)
    le.attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg={"X": 1}, kind="breakout", overwrite=False)

    # first call computed baseline then skipped; second call must hit cache and do nothing
    assert calls["n"] == 1
    assert getattr(ctx, "tp1_price", None) is None  # because it was skipped by floor

def test_attach_levels_caches_success(monkeypatch):
    from signals import level_enricher as le

    calls = {"n": 0}

    def fake_compute_levels(entry, atr, side, cfg, stop_dist_override=None, tp1_dist_override=None):
        calls["n"] += 1
        return {"sl": entry - 1.0, "tp_levels": [entry + 2.0], "stop_dist": 1.0, "rr": [2.0]}

    monkeypatch.setattr(le, "compute_levels", fake_compute_levels)
    monkeypatch.setenv("EDGE_LEVELS_MIN_STOP_BPS", "0")
    monkeypatch.setenv("EDGE_LEVELS_MIN_TP1_BPS", "0")

    ctx = types.SimpleNamespace(price=100.0, atr=1.0)

    le.attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg={"X": 1}, kind="breakout", overwrite=False)
    le.attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg={"X": 1}, kind="breakout", overwrite=False)

    assert calls["n"] == 1
    assert float(ctx.entry_price) == 100.0
    assert float(ctx.tp1_price) == 102.0

def test_attach_levels_recomputes_on_input_change(monkeypatch):
    from signals import level_enricher as le

    calls = {"n": 0}

    def fake_compute_levels(entry, atr, side, cfg, stop_dist_override=None, tp1_dist_override=None):
        calls["n"] += 1
        return {"sl": entry - 1.0, "tp_levels": [entry + 2.0], "stop_dist": 1.0, "rr": [2.0]}

    monkeypatch.setattr(le, "compute_levels", fake_compute_levels)
    monkeypatch.setenv("EDGE_LEVELS_MIN_STOP_BPS", "0")
    monkeypatch.setenv("EDGE_LEVELS_MIN_TP1_BPS", "0")

    ctx = types.SimpleNamespace(price=100.0, atr=1.0)

    le.attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg={"X": 1}, kind="breakout", overwrite=False)
    # change kind => new key
    le.attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg={"X": 1}, kind="absorption", overwrite=False)

    assert calls["n"] == 2

    # change entry => new key
    ctx2 = types.SimpleNamespace(price=101.0, atr=1.0)
    le.attach_trade_levels_to_ctx(ctx2, side="LONG", symbol="BTCUSDT", cfg={"X": 1}, kind="breakout", overwrite=False)
    assert calls["n"] == 3
