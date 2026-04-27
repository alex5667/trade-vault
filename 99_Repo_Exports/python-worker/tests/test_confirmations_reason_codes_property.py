import math
import types
import pytest

from signal_scoring.reason_codes import ReasonCode
from handlers.confirmations.engine import ConfirmationsEngine


def _mk_ctx(**kw):
    # minimal ctx object for ConfirmationsEngine.validate (duck-typing)
    ctx = types.SimpleNamespace()
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


@pytest.mark.parametrize("spread_bps", [50.0, 100.0, 500.0, 1000.0])
def test_spread_veto_always_has_structured_reason_code(spread_bps, monkeypatch) -> None:
    """
    Property: spread veto всегда дает стабильный reason_code.
    """
    monkeypatch.setenv("SPREAD_VETO_BPS", "25")
    eng = ConfirmationsEngine()

    # Set spread veto threshold and create context with high spread
    ctx = _mk_ctx(spread_bps=spread_bps)
    res = eng.validate(kind="breakout", ctx=ctx, l2={"ok": 1}, l3=None, level_price=100.0)

    # Should veto due to high spread
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_SPREAD_WIDE.value
    assert isinstance(res.reason_code, str) and len(res.reason_code) > 0


def test_conf_below_min_always_has_structured_reason_code(monkeypatch) -> None:
    """
    Property: conf below min всегда дает стабильный reason_code.
    """
    monkeypatch.setenv("MIN_CONF_FACTOR01", "0.5")
    eng = ConfirmationsEngine()

    ctx = _mk_ctx(spread_bps=0.0)
    res = eng.validate(kind="extreme", ctx=ctx, l2={"ok": 1}, l3=None, level_price=None)

    # Should veto due to low confidence (extreme with no l2 score boosts)
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_CONF_BELOW_MIN.value
    assert isinstance(res.reason_code, str) and len(res.reason_code) > 0


def test_l2_missing_always_has_structured_reason_code() -> None:
    """
    Property: l2 missing всегда дает стабильный reason_code для breakout.
    """
    eng = ConfirmationsEngine()

    ctx = _mk_ctx(spread_bps=0.0)
    res = eng.validate(kind="breakout", ctx=ctx, l2=None, l3=None, level_price=100.0)

    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_L2_MISSING.value
    assert isinstance(res.reason_code, str) and len(res.reason_code) > 0


@pytest.mark.parametrize("bad_value", [float('nan'), float('inf'), -float('inf'), 1e100, -1e100])
def test_l2_stale_always_has_structured_reason_code(bad_value) -> None:
    """
    Property: l2 stale всегда дает стабильный reason_code для breakout.
    """
    eng = ConfirmationsEngine()

    # Create stale l2 by setting old timestamp
    eng._now_ms = lambda: 2000  # Fixed "now"
    l2 = {"ts_ms": 1000}  # Old timestamp

    ctx = _mk_ctx(spread_bps=0.0)
    res = eng.validate(kind="breakout", ctx=ctx, l2=l2, l3=None, level_price=100.0)

    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_L2_STALE.value
    assert isinstance(res.reason_code, str) and len(res.reason_code) > 0
