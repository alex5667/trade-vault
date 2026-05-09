from __future__ import annotations

from types import SimpleNamespace

from handlers.confirmations.engine import ConfirmationsEngine
from handlers.confirmations.result import ConfirmResult
from signal_scoring import reason_registry as rr


class _BreakoutStub:
    def confirm(self, *, ctx, l2, level_price: float, side: str):
        # Return veto with reason_code but without u16 -> engine must fill u16.
        return ConfirmResult(
            passed=False,
            veto=True,
            parts={},
            flags={"near_big_wall": True},
            reasons=["near_big_wall"],
            score01=0.0,
            reason_code="VETO_WALL_NEAR",
            reason_u16=0,
        )


class _AbsorptionStub:
    def confirm(self, *, ctx, l2, level_price: float, side: str, require_2ofn: bool = True):
        return ConfirmResult(passed=True, veto=False, reason_code="OK", reason_u16=rr.reason_code_to_u16("OK"))


def test_engine_fills_reason_u16_from_registry_when_missing():
    eng = ConfirmationsEngine(logger=None, breakout=_BreakoutStub(), absorption=_AbsorptionStub(), feature_flags=None)
    ctx = SimpleNamespace(l2_is_stale=False, side="buy")
    l2 = object()  # non-None to bypass fail-closed
    v = eng.validate(kind="breakout", ctx=ctx, l2=l2, l3=None, level_price=100.0)

    assert v.veto is True
    assert v.reason_code == rr.normalize_reason(reason="VETO_WALL_NEAR", reason_code="")[1]
    assert v.reason_u16 == rr.reason_code_to_u16(v.reason_code)
