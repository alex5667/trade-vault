"""
tests/test_regime_squeeze_breakout.py

Unit-тесты для squeeze-expansion breakout gate в ModeValidator.

Покрывает:
  - legacy mode (ENABLED=0): поведение как раньше (z*1.2)
  - new mode (ENABLED=1): multi-condition gate (z+boost, OBI, microprice, spread, stale, touch)
  - squeeze-aliases (squeeze, squeeze_low, ...) корректно нормализуются в range
  - absorption/extreme остаются без изменений
  - non-range режимы не затронуты
"""
from __future__ import annotations

import os
import pytest

from handlers.crypto_orderflow.core.crypto_orderflow_quality import ModeValidator
from handlers.crypto_orderflow.types.crypto_orderflow_pipeline_types import (
    Candidate,
    QualityState,
)


# ── helpers ──────────────────────────────────────────────────────────────────

class _Ctx:
    """Минимальный ctx для тестов (getattr с дефолтом работает корректно)."""
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


def _cand(kind: str = "breakout") -> Candidate:
    return Candidate(kind=kind, direction=1, raw_score=1.0)


def _run(ctx: _Ctx, kind: str = "breakout") -> QualityState:
    v = ModeValidator()
    q = QualityState()
    v.validate(ctx, _cand(kind), q)
    return q


def _ctx_pass(**overrides) -> _Ctx:
    """Контекст, который проходит все условия нового gate при ENABLED=1."""
    defaults = dict(
        market_mode="squeeze",      # squeeze → range
        z_delta=2.0,                # > thr(1.0) + boost(0.5) = 1.5
        _breakout_thr=1.0,
        obi_sustained=True,
        lob_dw_obi_stable=False,
        microprice_shift=0.1,       # > 0 → подтверждает UP (z>0)
        spread_bps=5.0,             # < 10.0
        book_is_stale=False,
        dq_flag_stale=False,
        data_quality_flag=False,
        touch_ask_tag="depletion",
        touch_bid_tag="depletion",
    )
    defaults.update(overrides)
    return _Ctx(**defaults)


# ── legacy mode (ENABLED=0) ───────────────────────────────────────────────────

class TestLegacyMode:
    def test_legacy_veto_when_z_below_1_2x(self, monkeypatch):
        monkeypatch.delenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", raising=False)
        ctx = _Ctx(market_mode="range", z_delta=1.1, _breakout_thr=1.0)
        # z_abs=1.1 < thr*1.2=1.2 → veto
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "breakout_in_range_requires_stronger_z"

    def test_legacy_pass_when_z_meets_1_2x(self, monkeypatch):
        monkeypatch.delenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", raising=False)
        ctx = _Ctx(market_mode="range", z_delta=1.3, _breakout_thr=1.0)
        # z_abs=1.3 >= 1.2 → pass
        q = _run(ctx)
        assert not q.veto

    def test_legacy_no_veto_when_thr_zero(self, monkeypatch):
        monkeypatch.delenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", raising=False)
        ctx = _Ctx(market_mode="range", z_delta=0.1, _breakout_thr=0.0)
        q = _run(ctx)
        assert not q.veto

    def test_legacy_squeeze_treated_as_range(self, monkeypatch):
        monkeypatch.delenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", raising=False)
        ctx = _Ctx(market_mode="squeeze", z_delta=1.1, _breakout_thr=1.0)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "breakout_in_range_requires_stronger_z"

    def test_legacy_trend_not_affected(self, monkeypatch):
        monkeypatch.delenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", raising=False)
        ctx = _Ctx(market_mode="trending_bull", z_delta=0.5, _breakout_thr=1.0)
        q = _run(ctx)
        assert not q.veto


# ── new mode (ENABLED=1): z condition ─────────────────────────────────────────

class TestNewModeZCondition:
    def test_veto_when_z_below_thr_plus_boost(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_Z_BOOST", "0.5")
        # thr=1.0, boost=0.5, z_min=1.5; z=1.4 < 1.5 → veto
        ctx = _ctx_pass(z_delta=1.4, _breakout_thr=1.0)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_z_weak"

    def test_pass_when_z_meets_thr_plus_boost(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_Z_BOOST", "0.5")
        # thr=1.0, boost=0.5, z_min=1.5; z=1.6 >= 1.5 → pass z-check
        ctx = _ctx_pass(z_delta=1.6, _breakout_thr=1.0)
        q = _run(ctx)
        assert not q.veto

    def test_z_boost_default_0_5(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.delenv("REGIME_SQUEEZE_BREAKOUT_Z_BOOST", raising=False)
        ctx = _ctx_pass(z_delta=1.49, _breakout_thr=1.0)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_z_weak"

    def test_no_thr_uses_boost_alone(self, monkeypatch):
        """Если _breakout_thr=0, z_min = boost."""
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_Z_BOOST", "0.5")
        ctx = _ctx_pass(z_delta=0.4, _breakout_thr=0.0)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_z_weak"

    def test_negative_z_uses_abs(self, monkeypatch):
        """Короткий сигнал (z<0): z_abs должен быть >= порога."""
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_Z_BOOST", "0.5")
        ctx = _ctx_pass(z_delta=-1.6, _breakout_thr=1.0, microprice_shift=-0.1)
        q = _run(ctx)
        assert not q.veto


# ── new mode: OBI condition ───────────────────────────────────────────────────

class TestNewModeOBI:
    def test_veto_when_no_obi(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(obi_sustained=False, lob_dw_obi_stable=False)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_obi_not_stable"

    def test_pass_with_obi_sustained(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(obi_sustained=True, lob_dw_obi_stable=False)
        q = _run(ctx)
        assert not q.veto

    def test_pass_with_lob_dw_obi_stable(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(obi_sustained=False, lob_dw_obi_stable=True)
        q = _run(ctx)
        assert not q.veto

    def test_flag_squeeze_obi_stable_recorded(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(obi_sustained=True)
        q = _run(ctx)
        assert q.quality_flags.get("squeeze_obi_stable") is True


# ── new mode: microprice_shift condition ──────────────────────────────────────

class TestNewModeMicroprice:
    def test_veto_when_microprice_zero(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(z_delta=2.0, microprice_shift=0.0)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_microprice_no_confirm"

    def test_veto_when_microprice_opposite_direction(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        # z>0 (UP), но microprice_shift < 0 → не подтверждает
        ctx = _ctx_pass(z_delta=2.0, microprice_shift=-0.05)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_microprice_no_confirm"

    def test_pass_microprice_confirms_up(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(z_delta=2.0, microprice_shift=0.05)
        q = _run(ctx)
        assert not q.veto

    def test_pass_microprice_confirms_down(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(z_delta=-2.0, microprice_shift=-0.05)
        q = _run(ctx)
        assert not q.veto

    def test_flag_recorded(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(microprice_shift=0.07)
        q = _run(ctx)
        assert q.quality_flags.get("squeeze_microprice_shift") == pytest.approx(0.07)


# ── new mode: spread condition ────────────────────────────────────────────────

class TestNewModeSpread:
    def test_veto_when_spread_wide(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_MAX_SPREAD_BPS", "10.0")
        ctx = _ctx_pass(spread_bps=10.1)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_spread_wide"

    def test_pass_when_spread_at_limit(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_MAX_SPREAD_BPS", "10.0")
        ctx = _ctx_pass(spread_bps=10.0)
        q = _run(ctx)
        assert not q.veto

    def test_default_max_spread_10(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.delenv("REGIME_SQUEEZE_BREAKOUT_MAX_SPREAD_BPS", raising=False)
        ctx = _ctx_pass(spread_bps=10.1)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_spread_wide"


# ── new mode: stale book / DQ ─────────────────────────────────────────────────

class TestNewModeStale:
    def test_veto_when_book_is_stale(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(book_is_stale=True)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_stale_book"

    def test_veto_when_dq_flag_stale(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(dq_flag_stale=True)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_dq_flag"

    def test_veto_when_data_quality_flag(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(data_quality_flag=True)
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_dq_flag"

    def test_pass_no_stale_flags(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(book_is_stale=False, dq_flag_stale=False, data_quality_flag=False)
        q = _run(ctx)
        assert not q.veto


# ── new mode: touch_tag (optional) ───────────────────────────────────────────

class TestNewModeTouchTag:
    def test_not_checked_when_disabled(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_REQUIRE_TOUCH_DEPLETION", "0")
        ctx = _ctx_pass(touch_ask_tag="refill")
        q = _run(ctx)
        assert not q.veto

    def test_veto_when_tag_is_refill(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_REQUIRE_TOUCH_DEPLETION", "1")
        ctx = _ctx_pass(z_delta=2.0, touch_ask_tag="refill")
        q = _run(ctx)
        assert q.veto
        assert q.veto_reason == "squeeze_breakout_touch_not_depletion"

    @pytest.mark.parametrize("tag", ["depletion", "strong_ofi", "absorption", "depletion_ofi"])
    def test_pass_good_tags_up(self, monkeypatch, tag):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_REQUIRE_TOUCH_DEPLETION", "1")
        ctx = _ctx_pass(z_delta=2.0, touch_ask_tag=tag)
        q = _run(ctx)
        assert not q.veto

    def test_uses_bid_tag_for_short(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_REQUIRE_TOUCH_DEPLETION", "1")
        # z<0 (short) → должен читать touch_bid_tag
        ctx = _ctx_pass(z_delta=-2.0, microprice_shift=-0.1, touch_bid_tag="depletion", touch_ask_tag="refill")
        q = _run(ctx)
        assert not q.veto

    def test_touch_tag_flag_recorded(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_REQUIRE_TOUCH_DEPLETION", "1")
        ctx = _ctx_pass(z_delta=2.0, touch_ask_tag="depletion")
        q = _run(ctx)
        assert q.quality_flags.get("squeeze_touch_tag") == "depletion"


# ── squeeze_expansion_breakout flag ──────────────────────────────────────────

class TestSqueezeExpansionFlag:
    def test_flag_set_on_full_pass(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass()
        q = _run(ctx)
        assert not q.veto
        assert q.quality_flags.get("squeeze_expansion_breakout") is True

    def test_flag_not_set_on_veto(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(obi_sustained=False, lob_dw_obi_stable=False)
        q = _run(ctx)
        assert q.veto
        assert "squeeze_expansion_breakout" not in q.quality_flags


# ── regime label normalization ────────────────────────────────────────────────

class TestRegimeLabelNormalization:
    @pytest.mark.parametrize("mode", [
        "range", "ranging", "chop", "sideways", "meanrev",
        "squeeze", "squeeze_low", "squeeze_high",
    ])
    def test_range_aliases_trigger_gate(self, monkeypatch, mode):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(market_mode=mode)
        q = _run(ctx)
        # gate должен применяться (флаг market_mode установлен)
        assert "market_mode" in q.quality_flags
        # если всё ок — не veto; проверяем что gate сработал через флаг
        if not q.veto:
            assert q.quality_flags.get("squeeze_obi_stable") is True

    @pytest.mark.parametrize("mode", [
        "trending_bull", "trending_bear", "momentum", "trend", "expansion_bull",
    ])
    def test_trend_modes_not_gated(self, monkeypatch, mode):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        # breakout в trending → gate не применяется вообще
        ctx = _ctx_pass(market_mode=mode, obi_sustained=False, microprice_shift=0.0)
        q = _run(ctx)
        assert not q.veto
        assert "squeeze_obi_stable" not in q.quality_flags

    def test_mixed_mode_not_gated(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(market_mode="mixed", obi_sustained=False)
        q = _run(ctx)
        assert not q.veto


# ── absorption and extreme unaffected ────────────────────────────────────────

class TestOtherKindsUnaffected:
    def test_absorption_in_momentum_still_vetoed(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _Ctx(market_mode="momentum", z_delta=1.0, _extreme_thr=1.0)
        q = _run(ctx, kind="absorption")
        assert q.veto
        assert q.veto_reason == "absorption_in_momentum"

    def test_absorption_in_range_no_squeeze_gate(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(market_mode="range")
        q = _run(ctx, kind="absorption")
        assert not q.veto

    def test_extreme_in_range_legacy_z_guard(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _Ctx(market_mode="range", z_delta=1.1, _extreme_thr=1.0, _breakout_thr=0.0)
        q = _run(ctx, kind="extreme")
        # z_abs=1.1 < thr*1.15=1.15 → veto
        assert q.veto
        assert q.veto_reason == "extreme_in_range_requires_stronger_z"

    def test_sweep_never_gated(self, monkeypatch):
        monkeypatch.setenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "1")
        ctx = _ctx_pass(market_mode="range")
        q = _run(ctx, kind="sweep")
        assert not q.veto
