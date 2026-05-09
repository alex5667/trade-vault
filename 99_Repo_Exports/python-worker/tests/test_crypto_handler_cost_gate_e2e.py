from __future__ import annotations

from types import SimpleNamespace

import pytest

# ВАЖНО: если у вас путь импорта handler отличается — поправьте строку импорта ниже
from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler
from signals.types import OrderflowContext, SignalContext
from utils.time_utils import get_ny_time_millis


def test_crypto_publish_signal_vetoes_before_super(monkeypatch: pytest.MonkeyPatch):
    # ------------------------------------------------------------
    # ENV must be set BEFORE handler creation (init читает ENV)
    # ------------------------------------------------------------
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")

    # Force enrich to SKIP levels via floors:
    # entry=100, atr=1, stop_atr_mult=0.01 => stop_bps=1 < min_stop_bps=10 -> no attach
    monkeypatch.setenv("BTC_STOP_MODE", "ATR")
    monkeypatch.setenv("BTC_STOP_ATR_MULT", "0.01")
    monkeypatch.setenv("BTC_TP_MODE", "RR")
    monkeypatch.setenv("BTC_TP_RR", "1")
    monkeypatch.setenv("EDGE_LEVELS_MIN_STOP_BPS", "10")
    monkeypatch.setenv("EDGE_LEVELS_MIN_TP1_BPS", "0")

    handler = CryptoOrderFlowHandler(symbol="BTCUSDT")

    # Patch confidence gate to always pass (чтобы тест проверял именно cost gate)
    handler._confidence_threshold_filter.evaluate = lambda **kwargs: SimpleNamespace(  # type: ignore[assignment]
        passed=True,
        veto_reason="",
        confidence_pct=99.0,
        min_conf_threshold=0.0,
        conf_factor=1.0,
        min_conf_factor_threshold=0.0,
    )
    # Patch touch filter to always pass
    handler._touch_filter.check = lambda *a, **k: SimpleNamespace(ok=True, code="OK")  # type: ignore[assignment]

    # Ensure super()._publish_signal is NOT called (мы должны veto раньше)
    def boom(*args, **kwargs):
        raise AssertionError("super()._publish_signal was called, but must be vetoed before it")

    base_cls = None
    for c in handler.__class__.mro()[1:]:
        if hasattr(c, "_publish_signal"):
            base_cls = c
            break
    assert base_cls is not None
    monkeypatch.setattr(base_cls, "_publish_signal", boom, raising=True)  # type: ignore[arg-type]

    # Build minimal ctx
    ts_ms = get_ny_time_millis()
    of = OrderflowContext(ts=ts_ms, price=100.0, symbol="BTCUSDT", atr=1.0, spread_bps=2.0)
    ctx = SignalContext(symbol="BTCUSDT", ts_event_ms=ts_ms, of=of)
    # optional convenience fields used as fallbacks in some code paths
    ctx.price = 100.0
    ctx.confidence_pct = 99.0
    ctx.conf_factor = 1.0

    res = handler._publish_signal(  # type: ignore[attr-defined]
        "LONG",
        ctx,
        "test",
        "🚨",
        signal_kind="breakout",
        level_key="na",
        entry_tag="",
    )

    assert res.sent is False
    assert res.dedup is True
    assert getattr(res, "msg_id", None) is None
    # Veto reason should be cost edge related
    assert "COST_EDGE" in str(getattr(ctx, "veto_reason", ""))
