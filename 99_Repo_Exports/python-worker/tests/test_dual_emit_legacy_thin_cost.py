from __future__ import annotations

from types import SimpleNamespace


def test_emit_veto_metric_dual_off_by_default(monkeypatch):
    from handlers.base_orderflow_handler import emit_veto_metric_dual

    monkeypatch.delenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", raising=False)

    calls = []

    def emit(*, kind, ctx, reason_code):
        calls.append((kind, reason_code))

    ctx = SimpleNamespace()
    emit_veto_metric_dual(emit, kind="absorption", ctx=ctx, reason_code="VETO_EDGE_COST")
    assert calls == [("absorption", "VETO_EDGE_COST")]


def test_emit_veto_metric_dual_on(monkeypatch):
    from handlers.base_orderflow_handler import emit_veto_metric_dual

    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")

    calls = []

    def emit(*, kind, ctx, reason_code):
        calls.append((kind, reason_code))

    ctx = SimpleNamespace()
    emit_veto_metric_dual(emit, kind="breakout", ctx=ctx, reason_code="VETO_EV_COST")
    assert ("breakout", "VETO_EV_COST") in calls
    assert ("breakout", "VETO_EDGE_THIN_COST") in calls
