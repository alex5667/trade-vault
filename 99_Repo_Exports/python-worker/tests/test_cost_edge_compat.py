from __future__ import annotations

from types import SimpleNamespace

from handlers.crypto_orderflow.utils.cost_edge_compat import (
    decision_to_legacy_tuple,
    maybe_dual_emit_legacy_thin_cost,
)


def test_decision_to_legacy_tuple_ok_and_details():
    dec = SimpleNamespace(
        apply=True,
        veto=False,
        expected_move_bps=25.0,
        threshold_bps=20.0,
        fees_bps=8.0,
        slippage_bps=5.0,
        k=2.0,
        mode="tp1",
    )
    ok, details = decision_to_legacy_tuple(dec)
    assert ok is True
    assert details["expected_move_bps"] == 25.0
    assert details["threshold_bps"] == 20.0
    assert details["fees_bps"] == 8.0
    assert details["slippage_bps"] == 5.0
    assert details["k"] == 2.0


def test_decision_to_legacy_tuple_veto_false_ok_false():
    dec = SimpleNamespace(apply=True, veto=True, expected_move_bps=10.0, threshold_bps=20.0, k=2.0)
    ok, _ = decision_to_legacy_tuple(dec)
    assert ok is False


def test_dual_emit_off_by_default(monkeypatch):
    monkeypatch.delenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", raising=False)
    emitted = []

    def emit_veto_metric(*, kind, ctx, reason_code):
        emitted.append(reason_code)

    out = maybe_dual_emit_legacy_thin_cost(
        emit_veto_metric=emit_veto_metric,
        kind="k",
        ctx=object(),
        reason_code="VETO_EDGE_COST",
    )
    assert out == ["VETO_EDGE_COST"]
    assert emitted == ["VETO_EDGE_COST"]


def test_dual_emit_on(monkeypatch):
    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")
    emitted = []

    def emit_veto_metric(*, kind, ctx, reason_code):
        emitted.append(reason_code)

    out = maybe_dual_emit_legacy_thin_cost(
        emit_veto_metric=emit_veto_metric,
        kind="k",
        ctx=object(),
        reason_code="VETO_EV_COST",
    )
    assert out == ["VETO_EV_COST", "VETO_EDGE_THIN_COST"]
    assert emitted == ["VETO_EV_COST", "VETO_EDGE_THIN_COST"]
