import pytest


def test_finalize_decision_veto_sets_reason_and_decision():
    from handlers.confirmations.engine import _finalize_decision
    rc, ru16, dc, du16 = _finalize_decision(
        veto=True,
        veto_reason="near_big_wall",   # legacy
        veto_reason_code="",           # let registry map legacy -> structured
    )
    assert rc == "VETO_WALL_NEAR"
    assert ru16 > 0
    assert dc == "VETO_WALL_NEAR"
    assert du16 == ru16


def test_finalize_decision_ok_sets_ok_u16():
    from handlers.confirmations.engine import _finalize_decision
    rc, ru16, dc, du16 = _finalize_decision(
        veto=False,
        veto_reason="",
        veto_reason_code="",
        soft_code="",
    )
    assert rc == ""
    assert ru16 == 0
    assert dc == "OK"
    assert du16 == 1
