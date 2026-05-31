from __future__ import annotations

from types import SimpleNamespace

import pytest

from signals.level_enricher import attach_trade_levels_to_ctx


def test_attach_trade_levels_to_ctx_sets_required_fields():
    """
    Minimal ctx object (real SignalContext is not required; attach_* works via getattr/setattr).
    """
    ctx = SimpleNamespace()
    ctx.price = 100.0
    ctx.atr = 2.0

    cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 0.5,     # stop_dist = 1.0
        "TP_MODE": "RR",
        "TP_RR": "1",            # TP1 = entry + 1*stop_dist
        "TP_ATR_MULTS": "0.6,1.0,1.5",
    }

    attach_trade_levels_to_ctx(
        ctx,
        side="LONG",
        symbol="BTCUSDT",
        cfg=cfg,
        overwrite=True,
        logger=None,
    )

    assert abs(ctx.entry_price - 100.0) < 1e-9
    # LONG: sl = entry - stop_dist = 99
    assert abs(ctx.sl_price - 99.0) < 1e-9
    # TP1 = entry + stop_dist = 101
    assert abs(ctx.tp1_price - 101.0) < 1e-9
    assert isinstance(ctx.tp_levels, list)
    assert len(ctx.tp_levels) >= 1
    assert abs(ctx.tp_levels[0] - 101.0) < 1e-9
    assert abs(ctx.stop_dist - 1.0) < 1e-9


def test_attach_trade_levels_long(monkeypatch):
    """
    Проверяем что attach_trade_levels_to_ctx корректно заполняет уровни для LONG.
    """
    # deterministic cfg via ENV (BTC_* overrides)
    monkeypatch.setenv("BTC_STOP_MODE", "ATR")
    monkeypatch.setenv("BTC_STOP_ATR_MULT", "1.0")
    monkeypatch.setenv("BTC_TP_MODE", "RR")
    monkeypatch.setenv("BTC_TP_RR", "1,2")

    from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver

    symbol = "BTCUSDT"
    of = SimpleNamespace(price=100.0, atr=2.0, spread_bps=10.0)
    ctx = SimpleNamespace(symbol=symbol, of=of)

    cfg = RiskCfgResolver().resolve(symbol)

    attach_trade_levels_to_ctx(ctx, side="LONG", symbol=symbol, cfg=cfg, overwrite=True)

    assert ctx.entry_price == 100.0
    assert ctx.sl_price < 100.0
    assert isinstance(ctx.tp_levels, list) and len(ctx.tp_levels) >= 1
    assert ctx.tp1_price == ctx.tp_levels[0]
    assert ctx.tp1_price > 100.0
    assert getattr(ctx, "stop_dist", 0.0) > 0.0


def test_attach_trade_levels_short(monkeypatch):
    """
    Проверяем что attach_trade_levels_to_ctx корректно заполняет уровни для SHORT.
    """
    monkeypatch.setenv("BTC_STOP_MODE", "ATR")
    monkeypatch.setenv("BTC_STOP_ATR_MULT", "1.0")
    monkeypatch.setenv("BTC_TP_MODE", "RR")
    monkeypatch.setenv("BTC_TP_RR", "1,2")

    from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver
    from signals.level_enricher import attach_trade_levels_to_ctx

    symbol = "BTCUSDT"
    of = SimpleNamespace(price=100.0, atr=2.0, spread_bps=10.0)
    ctx = SimpleNamespace(symbol=symbol, of=of)

    cfg = RiskCfgResolver().resolve(symbol)

    attach_trade_levels_to_ctx(ctx, side="SHORT", symbol=symbol, cfg=cfg, overwrite=True)

    assert ctx.entry_price == 100.0
    assert ctx.sl_price > 100.0
    assert ctx.tp1_price < 100.0


def test_attach_trade_levels_to_ctx_is_fail_open_on_bad_inputs():
    """
    Verify that attach_trade_levels_to_ctx is fail-open when entry is invalid.
    """
    ctx = SimpleNamespace()
    ctx.price = -1.0  # invalid entry => should no-op
    ctx.atr = 2.0

    cfg = {"STOP_MODE": "ATR", "STOP_ATR_MULT": 0.6, "TP_MODE": "RR", "TP_RR": "1,2,3"}
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg=cfg, overwrite=True, logger=None)

    assert getattr(ctx, "entry_price", None) is None
    assert getattr(ctx, "tp1_price", None) is None


def test_attach_trade_levels_fail_open_on_bad_entry():
    """
    Проверяем что attach_trade_levels_to_ctx fail-open при некорректных данных.
    """
    from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver

    symbol = "BTCUSDT"
    of = SimpleNamespace(price=0.0, atr=2.0)  # invalid entry for bps computations
    ctx = SimpleNamespace(symbol=symbol, of=of)

    cfg = RiskCfgResolver().resolve(symbol)

    # should not crash
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol=symbol, cfg=cfg, overwrite=True)

    # should not set levels
    assert getattr(ctx, "entry_price", None) is None or getattr(ctx, "entry_price", 0.0) == 0.0


def test_attach_trade_levels_idempotent():
    """
    Проверяем что attach_trade_levels_to_ctx не перезаписывает существующие уровни
    когда overwrite=False.
    """
    from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver
    from signals.level_enricher import attach_trade_levels_to_ctx

    symbol = "BTCUSDT"
    of = SimpleNamespace(price=100.0, atr=2.0, spread_bps=10.0)
    # Pre-fill levels
    ctx = SimpleNamespace(
        symbol=symbol,
        of=of,
        entry_price=200.0,  # existing levels
        tp1_price=250.0,
        sl_price=150.0,
    )

    cfg = RiskCfgResolver().resolve(symbol)

    # overwrite=False -> should not modify existing levels
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol=symbol, cfg=cfg, overwrite=False)

    assert ctx.entry_price == 200.0  # unchanged
    assert ctx.tp1_price == 250.0  # unchanged
    assert ctx.sl_price == 150.0  # unchanged


def test_attach_trade_levels_overwrite():
    """
    Проверяем что attach_trade_levels_to_ctx перезаписывает уровни когда overwrite=True.
    
    Note: entry резолвится с приоритетом ctx.entry_price, поэтому мы не ставим его в ctx,
    чтобы он взялся из of.price.
    """
    from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver
    from signals.level_enricher import attach_trade_levels_to_ctx

    symbol = "BTCUSDT"
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("BTC_STOP_MODE", "ATR")
    monkeypatch.setenv("BTC_STOP_ATR_MULT", "1.0")
    monkeypatch.setenv("BTC_TP_MODE", "RR")
    monkeypatch.setenv("BTC_TP_RR", "1,2")

    of = SimpleNamespace(price=100.0, atr=2.0, spread_bps=10.0)
    # Pre-fill with different levels (но без entry_price, чтобы он взялся из of)
    ctx = SimpleNamespace(
        symbol=symbol,
        of=of,
        tp1_price=250.0,
        sl_price=150.0,
    )

    cfg = RiskCfgResolver().resolve(symbol)

    # overwrite=True -> should recalculate based on of.price
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol=symbol, cfg=cfg, overwrite=True)

    assert ctx.entry_price == 100.0  # recalculated from of.price
    assert ctx.tp1_price != 250.0  # changed
    assert ctx.sl_price != 150.0  # changed

    monkeypatch.undo()
