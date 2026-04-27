import os
from types import SimpleNamespace


def test_dual_emit_off_by_default(monkeypatch):
    from handlers.crypto_orderflow_handler import emit_cost_edge_veto_metrics

    monkeypatch.delenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", raising=False)
    calls = []

    def emit_veto_metric(*, kind, ctx, reason_code):
        calls.append(reason_code)

    emit_cost_edge_veto_metrics(emit_veto_metric, kind="k", ctx=SimpleNamespace(), reason_code="VETO_EDGE_COST")
    assert calls == ["VETO_EDGE_COST"]


def test_dual_emit_on(monkeypatch):
    from handlers.crypto_orderflow_handler import emit_cost_edge_veto_metrics

    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")
    calls = []

    def emit_veto_metric(*, kind, ctx, reason_code):
        calls.append(reason_code)

    emit_cost_edge_veto_metrics(emit_veto_metric, kind="k", ctx=SimpleNamespace(), reason_code="VETO_EDGE_COST")
    assert calls == ["VETO_EDGE_COST", "VETO_EDGE_THIN_COST"]


def test_dual_emit_does_not_duplicate_if_already_legacy(monkeypatch):
    from handlers.crypto_orderflow_handler import emit_cost_edge_veto_metrics

    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")
    calls = []

    def emit_veto_metric(*, kind, ctx, reason_code):
        calls.append(reason_code)

    emit_cost_edge_veto_metrics(emit_veto_metric, kind="k", ctx=SimpleNamespace(), reason_code="VETO_EDGE_THIN_COST")
    assert calls == ["VETO_EDGE_THIN_COST"]
