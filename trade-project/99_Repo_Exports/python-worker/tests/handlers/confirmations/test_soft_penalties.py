from types import SimpleNamespace


def test_soft_penalties_l3_missing_sets_primary_code_and_mul():
    from handlers.confirmations.engine import ConfirmationsEngine

    parts = {}
    ctx = SimpleNamespace(geometry_score=0.5)  # geo ok
    eng = ConfirmationsEngine()
    mul, hits, parts = eng._soft_penalties(kind="extreme", ctx=ctx, l2=object(), l3=None)
    assert ("SOFT_L3_MISSING", 0.10) in hits
    assert parts["l3_missing"] == 1.0


def test_soft_penalties_geo_missing_sets_code_when_l3_ok():
    from handlers.confirmations.engine import ConfirmationsEngine

    parts = {}
    ctx = SimpleNamespace(geometry_score=None, geometry=None)  # geo missing
    eng = ConfirmationsEngine()
    mul, hits, parts = eng._soft_penalties(kind="extreme", ctx=ctx, l2=object(), l3=object())
    assert ("SOFT_HTF_MISSING", 0.05) in hits
    assert parts["missing_htf"] == 1.0


def test_soft_penalties_l2_missing_only_for_non_breakout_absorption():
    from handlers.confirmations.engine import ConfirmationsEngine

    eng = ConfirmationsEngine()

    # breakout should not get L2 soft penalty
    parts1 = {}
    ctx1 = SimpleNamespace(geometry_score=0.5)
    mul1, hits1, parts1 = eng._soft_penalties(kind="breakout", ctx=ctx1, l2=None, l3=object())
    assert "soft_l2_missing_fail_open" not in parts1

    # extreme should get L2 soft penalty
    parts2 = {}
    ctx2 = SimpleNamespace(geometry_score=0.5)
    mul2, hits2, parts2 = eng._soft_penalties(kind="extreme", ctx=ctx2, l2=None, l3=object())
    assert parts2["soft_l2_missing_fail_open"] == 1.0
    assert ("SOFT_L2_STALE_EXTREME", 0.15) in hits2
