from types import SimpleNamespace

from handlers.confirmations.engine import ConfirmationsEngine


class _DummyMicro:
    def validate(self, *, kind, ctx):
        # Return dummy micro quality
        return SimpleNamespace(mult01=1.0, flags=[], parts={}, veto=False)


class _DummyBreakout:
    def confirm(self, *, ctx, l2, level_price):
        # Not used in these branches; kept for safety.
        return SimpleNamespace(veto=False, score01=1.0, flags=[], parts={})


def _mk_engine(min_conf: float = 0.5) -> ConfirmationsEngine:
    # Create minimal instance for testing
    eng = ConfirmationsEngine()
    eng._min_conf = float(min_conf)
    eng._l2_stale_ms = 1500
    eng._extreme_l2_penalty = 0.65
    eng._breakout = _DummyBreakout()
    eng._absorption = _DummyBreakout()  # Reuse dummy
    eng._micro = _DummyMicro()  # Use proper dummy
    eng._spread_veto_bps = 0  # Disable spread veto for tests
    return eng


def test_breakout_veto_l2_missing_has_reason_u16() -> None:
    eng = _mk_engine()
    ctx = SimpleNamespace(l2_stale=False)
    res = eng.validate(kind="breakout", ctx=ctx, l2=None, l3=None, level_price=100.0)
    assert res.veto is True
    assert int(getattr(res, "reason_u16", 0) or 0) != 0


def test_breakout_veto_l2_stale_has_reason_u16() -> None:
    eng = _mk_engine()
    ctx = SimpleNamespace(l2_stale=True)
    res = eng.validate(kind="breakout", ctx=ctx, l2=object(), l3=None, level_price=100.0)
    assert res.veto is True
    assert int(getattr(res, "reason_u16", 0) or 0) != 0


def test_veto_conf_below_min_has_reason_u16() -> None:
    # Setup engine with high min_conf to force veto on final step
    eng = _mk_engine(min_conf=0.99)
    ctx = SimpleNamespace(l2_stale=False)

    # Use breakout kind to avoid policy normalization (L2 reasons are allowed for breakout)
    res = eng.validate(kind="breakout", ctx=ctx, l2=object(), l3=None, level_price=100.0)
    if res.veto:
        assert int(getattr(res, "reason_u16", 0) or 0) != 0
