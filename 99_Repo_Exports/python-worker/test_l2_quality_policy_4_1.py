from __future__ import annotations

from dataclasses import dataclass, field
import math
import pytest

from handlers.confirmations.l2_quality import L2QualityPolicy, apply_l2_policy_to_ctx


@dataclass
class Ctx:
    ts: int
    data_quality_flags: list[str] | None = None
    l2_score01: float | None = None
    l2_missing_rate: float | None = None


@dataclass
class L2Level:
    price: float
    size: float
    notional: float


@dataclass
class L2:
    ts_ms: int
    bids: list[L2Level] = field(default_factory=lambda: [L2Level(50000.0, 1.0, 50000.0)] * 10)
    asks: list[L2Level] = field(default_factory=lambda: [L2Level(50001.0, 1.0, 50001.0)] * 10)


def test_breakout_missing_is_fail_closed_veto():
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    a = p.assess(kind="breakout", ctx=ctx, l2=None)
    assert a.veto is True
    assert a.l2_score01 == 0.0
    apply_l2_policy_to_ctx(ctx, "breakout", a, p.missing_rate())
    assert "l2_missing" in (ctx.data_quality_flags or [])


def test_breakout_stale_is_fail_closed_veto():
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    l2 = L2(ts_ms=1_000)  # age=9000ms -> stale
    a = p.assess(kind="breakout", ctx=ctx, l2=l2)
    assert a.veto is True
    apply_l2_policy_to_ctx(ctx, "breakout", a, p.missing_rate())
    assert "l2_stale" in (ctx.data_quality_flags or [])


def test_extreme_missing_is_fail_open_penalty_not_veto():
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    a = p.assess(kind="extreme", ctx=ctx, l2=None)
    assert a.veto is False
    assert a.l2_score01 == pytest.approx(0.3)
    apply_l2_policy_to_ctx(ctx, "extreme", a, p.missing_rate())
    assert "l2_missing" in (ctx.data_quality_flags or [])


def test_extreme_stale_is_fail_open_penalty_not_veto():
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    l2 = L2(ts_ms=1_000)
    a = p.assess(kind="extreme", ctx=ctx, l2=l2)
    assert a.veto is False
    assert a.l2_score01 == pytest.approx(0.35)
    apply_l2_policy_to_ctx(ctx, "extreme", a, p.missing_rate())
    assert "l2_stale" in (ctx.data_quality_flags or [])


def test_fresh_l2_sets_score_1_no_flags():
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    l2 = L2(ts_ms=9_600)  # age=400ms -> ok
    a = p.assess(kind="breakout", ctx=ctx, l2=l2)
    assert a.veto is False
    assert a.l2_score01 == pytest.approx(0.99125, rel=1e-3)
    apply_l2_policy_to_ctx(ctx, "breakout", a, p.missing_rate())
    assert (ctx.data_quality_flags or []) == []


def test_bad_ts_is_handled_no_crash():
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    class BadL2:
        ts_ms = float("nan")
    a = p.assess(kind="extreme", ctx=ctx, l2=BadL2())
    assert a.veto is False
    apply_l2_policy_to_ctx(ctx, "extreme", a, p.missing_rate())
    assert "l2_bad_ts" in (ctx.data_quality_flags or [])


def test_property_fuzz_nan_inf_ts():
    """
    Property-style без зависимости от hypothesis: перебор "плохих" значений.
    Требование: не падать и возвращать корректный L2Assessment.
    """
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    for bad in [float("nan"), float("inf"), float("-inf")]:
        class Bad:
            ts_ms = bad
        a = p.assess(kind="breakout", ctx=ctx, l2=Bad())
        # breakout -> fail-closed
        assert a.veto is True
        assert math.isfinite(float(a.l2_score01))
