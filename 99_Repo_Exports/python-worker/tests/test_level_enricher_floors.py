from __future__ import annotations

from types import SimpleNamespace

import pytest

from signals.level_enricher import attach_trade_levels_to_ctx


def test_enricher_skips_micro_stop_by_env(monkeypatch: pytest.MonkeyPatch):
    # stop_bps floor = 10 bps
    monkeypatch.setenv("EDGE_LEVELS_MIN_STOP_BPS", "10")
    monkeypatch.setenv("EDGE_LEVELS_MIN_TP1_BPS", "0")

    ctx = SimpleNamespace()
    ctx.price = 100.0
    ctx.atr = 1.0

    # stop_dist = 0.05 => stop_bps = 5 bps < 10 => must NOT attach
    cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 0.05,
        "STOP_PCT": 0.2,
        "STOP_POINTS": 1.0,
        "TP_MODE": "RR",
        "TP_RR": "1",
        "TP_ATR_MULTS": "0.6,1.0,1.5",
    }
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg=cfg, overwrite=True, logger=None)
    assert getattr(ctx, "entry_price", None) is None
    assert getattr(ctx, "tp1_price", None) is None


def test_enricher_skips_tiny_tp1_by_env(monkeypatch: pytest.MonkeyPatch):
    # tp1_bps floor = 20 bps
    monkeypatch.setenv("EDGE_LEVELS_MIN_STOP_BPS", "0")
    monkeypatch.setenv("EDGE_LEVELS_MIN_TP1_BPS", "20")

    ctx = SimpleNamespace()
    ctx.price = 100.0
    ctx.atr = 1.0

    # stop_dist = 0.1 => tp1 move = 0.1 => 10 bps < 20 => must NOT attach
    cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 0.1,
        "TP_MODE": "RR",
        "TP_RR": "1",
        "TP_ATR_MULTS": "0.6,1.0,1.5",
    }
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg=cfg, overwrite=True, logger=None)
    assert getattr(ctx, "entry_price", None) is None
    assert getattr(ctx, "tp1_price", None) is None
