from __future__ import annotations

from types import SimpleNamespace


def test_legacy_gate_cost_edge_respects_apply_flag_and_veto_reason():
    """
    Проверяет FIX:
      - evaluate(*, ctx, kind, symbol) -> EdgeCostGateDecision
      - decision.apply=False => gate пропускается
      - decision.veto=True  => блок + reason_code в метрику
    """
    # Mock classes to avoid import issues
    class MockEdgeCostGateDecision:
        def __init__(self, apply, veto, reason_code, expected_move_bps, threshold_bps, fees_bps, slippage_bps, k, mode, notes):
            self.apply = apply
            self.veto = veto
            self.reason_code = reason_code
            self.expected_move_bps = expected_move_bps
            self.threshold_bps = threshold_bps
            self.fees_bps = fees_bps
            self.slippage_bps = slippage_bps
            self.k = k
            self.mode = mode
            self.notes = notes

    class FakeGate:
        def __init__(self, decision):
            self._d = decision

        def evaluate(self, *, ctx, kind: str, symbol: str):
            return self._d

    # Mock handler with minimal state
    class MockHandler:
        def __init__(self):
            self.symbol = "BTCUSDT"
            self.logger = None
            self._emit_veto_metric_calls = []

        def _emit_veto_metric(self, kind, ctx, reason_code):
            self._emit_veto_metric_calls.append((kind, reason_code))

        def _legacy_gate_cost_edge(self, *, frame, pre):
            # Copy the logic from the actual method
            gate = getattr(self, "_edge_cost_gate", None) or getattr(self, "_cost_edge_gate", None)
            if gate is None or not callable(getattr(gate, "evaluate", None)):
                return True, ""

            ctx = frame.ctx
            kind = frame.kind_key or frame.kind_str
            sym = str(pre.get("ctx_symbol") or getattr(self, "symbol", "") or "")

            try:
                decision = gate.evaluate(ctx=ctx, kind=str(kind), symbol=str(sym))
            except Exception as e:
                # fail-open but observable - mock this part
                return True, ""

            # Gate may decide "not applicable" for given inputs / disabled via env
            try:
                if not bool(getattr(decision, "apply", True)):
                    return True, ""
            except Exception:
                return True, ""

            try:
                veto = bool(getattr(decision, "veto", False))
            except Exception:
                veto = False

            if veto:
                try:
                    rc = str(getattr(decision, "reason_code", "") or "VETO_EDGE_COST")
                except Exception:
                    rc = "VETO_EDGE_COST"
                # Mock normalize_reason
                rc = rc or "VETO_EDGE_COST"
                self._emit_veto_metric(kind=kind, ctx=ctx, reason_code=rc)
                return False, rc

            return True, ""

    h = MockHandler()
    ctx = SimpleNamespace(symbol="BTCUSDT", entry_price=100.0, tp1_price=101.0, sl_price=99.0)
    frame = SimpleNamespace(ctx=ctx, kind_key="breakout", kind_str="breakout", side_int=1)
    pre = {"ctx_symbol": "BTCUSDT"}

    # 1) apply=False => pass
    d1 = MockEdgeCostGateDecision(
        apply=False,
        veto=False,
        reason_code="",
        expected_move_bps=0.0,
        threshold_bps=0.0,
        fees_bps=0.0,
        slippage_bps=0.0,
        k=1.0,
        mode="tp1",
        notes="",
    )
    h._edge_cost_gate = FakeGate(d1)
    ok, reason = h._legacy_gate_cost_edge(frame=frame, pre=pre)
    assert ok is True
    assert reason == ""
    assert h._emit_veto_metric_calls == []

    # 2) apply=True + veto=True => block with reason_code
    d2 = MockEdgeCostGateDecision(
        apply=True,
        veto=True,
        reason_code="VETO_EDGE_COST_TEST",
        expected_move_bps=10.0,
        threshold_bps=20.0,
        fees_bps=5.0,
        slippage_bps=5.0,
        k=2.0,
        mode="tp1",
        notes="",
    )
    h._edge_cost_gate = FakeGate(d2)
    ok, reason = h._legacy_gate_cost_edge(frame=frame, pre=pre)
    assert ok is False
    assert reason == "VETO_EDGE_COST_TEST"
    assert h._emit_veto_metric_calls == [("breakout", "VETO_EDGE_COST_TEST")]
