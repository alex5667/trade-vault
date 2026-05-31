from __future__ import annotations

from types import SimpleNamespace

from handlers.crypto_orderflow.utils.trail_conditional import TrailDecision, apply_trailing_policy_to_payload


class FakeEval:
    def __init__(self, enabled: bool, reason: str):
        self._d = TrailDecision(enabled=enabled, reason=reason)
    def evaluate(self, ctx, *, side, symbol, kind, tf, regime):
        return self._d


def test_apply_trailing_policy_writes_payload_and_ctx():
    payload = {"kind": "breakout", "side": "LONG", "symbol": "BTCUSDT", "timeframe": "1m"}
    ctx = SimpleNamespace()
    ev = FakeEval(enabled=False, reason="VETO_TEST")

    ok, rs = apply_trailing_policy_to_payload(
        payload=payload, ctx=ctx, evaluator=ev,
        side="LONG", symbol="BTCUSDT", kind="breakout", tf="1m", regime="na"
    )
    assert ok is False
    assert payload["trail_after_tp1"] is False
    assert payload["trail_after_tp1_reason"] == "VETO_TEST"
    assert ctx.trail_after_tp1 is False
    assert ctx.trail_after_tp1_reason == "VETO_TEST"
