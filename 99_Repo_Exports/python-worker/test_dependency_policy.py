from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.dependency_policy import (
    ensure_dependency_defaults,
    dependency_decision_for_kind,
    DEFAULT_L3_SCORE_NEUTRAL,
    DEFAULT_GEOMETRY_SCORE_NEUTRAL,
)


def test_defaults_fill_l3_and_geometry_and_flags():
    ctx = SimpleNamespace()
    ensure_dependency_defaults(ctx)

    assert ctx.l3_score == DEFAULT_L3_SCORE_NEUTRAL
    assert ctx.geometry_score == DEFAULT_GEOMETRY_SCORE_NEUTRAL
    assert "l3_missing" in ctx.data_quality_flags
    assert "htf_missing" in ctx.data_quality_flags


def test_hlc_fallback_is_only_flagged_not_vetoed():
    ctx = SimpleNamespace(hlc_fallback_used=True)
    ensure_dependency_defaults(ctx)
    assert "hlc_fallback" in ctx.data_quality_flags

    # Без L2 данных breakout всё равно veto (по L2), но сам hlc_fallback ничего не veto'ит
    dep = dependency_decision_for_kind(kind="custom", ctx=ctx, now_ms=10_000, l2_stale_ms=1500)
    assert dep.veto is False
    assert dep.parts["hlc_fallback"] is True


def test_l2_stale_breakout_fail_closed_veto():
    # L2 отсутствует -> stale -> breakout veto
    ctx = SimpleNamespace(ts=10_000)
    dep = dependency_decision_for_kind(kind="breakout", ctx=ctx, now_ms=10_000, l2_stale_ms=1500)
    assert dep.veto is True
    assert dep.parts["l2"]["stale"] is True
    assert "l2_stale" in ctx.data_quality_flags


def test_l2_stale_by_age_breakout_veto():
    ctx = SimpleNamespace(ts=10_000, l2_ts_ms=7_000)
    dep = dependency_decision_for_kind(kind="breakout", ctx=ctx, now_ms=10_000, l2_stale_ms=1500)
    assert dep.veto is True
    assert dep.parts["l2"]["age_ms"] == 3_000
    assert dep.parts["l2"]["stale"] is True


def test_l2_stale_extreme_fail_open_penalty_not_veto():
    ctx = SimpleNamespace(ts=10_000, l2_ts_ms=7_000)
    dep = dependency_decision_for_kind(kind="extreme", ctx=ctx, now_ms=10_000, l2_stale_ms=1500)
    assert dep.veto is False
    assert dep.conf_multiplier == pytest.approx(0.75)
    assert dep.parts["l2"]["stale"] is True


def test_l2_fresh_no_veto():
    ctx = SimpleNamespace(ts=10_000, l2_ts_ms=9_500)
    dep = dependency_decision_for_kind(kind="breakout", ctx=ctx, now_ms=10_000, l2_stale_ms=1500)
    assert dep.veto is False
    assert dep.conf_multiplier == pytest.approx(1.0)
    assert dep.parts["l2"]["stale"] is False


def test_defaults_do_not_override_explicit_scores():
    ctx = SimpleNamespace(l3_score=0.9, geometry_score=0.8)
    ensure_dependency_defaults(ctx)
    assert ctx.l3_score == pytest.approx(0.9)
    assert ctx.geometry_score == pytest.approx(0.8)
    assert "l3_missing" not in ctx.data_quality_flags
    assert "htf_missing" not in ctx.data_quality_flags
