from __future__ import annotations

from dataclasses import dataclass

import pytest

from signal_scoring.kind_rules import apply_kind_rules


@dataclass
class Ctx:
    # generic
    spread_bps: float | None = None
    # breakout
    microprice_shift_bps: float | None = None
    taker_rate_ema: float | None = None
    cancel_to_trade: float | None = None
    level_price: float | None = 100.0
    # absorption
    refill: bool | None = None
    mp_contra: bool | None = None
    micro_proxy: bool | None = None
    # extreme
    micro_quality_ok: bool | None = None
    book_quality_ok: bool | None = None
    micro_quality: float | None = None
    book_quality: float | None = None
    # obi
    obi_sustained: bool | None = None
    l3_cancel_to_trade: float | None = None


def test_breakout_fake_breakout_downscales_when_shift_but_no_taker_continuation():
    ctx = Ctx(microprice_shift_bps=1.2, taker_rate_ema=0.10, cancel_to_trade=1.0, spread_bps=0.5)
    r = apply_kind_rules("breakout", ctx, {})
    assert r.veto is False
    assert 0.0 <= r.conf_mult01 <= 1.0
    assert r.conf_mult01 < 1.0
    assert r.flags.get("no_taker_continuation") is True


def test_breakout_veto_on_high_cancel_to_trade_and_low_taker_rate():
    ctx = Ctx(microprice_shift_bps=1.2, taker_rate_ema=0.10, cancel_to_trade=3.0)
    r = apply_kind_rules("breakout", ctx, {})
    assert r.veto is True
    assert r.flags.get("veto_spoof_like") is True


def test_breakout_emits_post_acceptance_probe_label():
    ctx = Ctx(microprice_shift_bps=1.0, taker_rate_ema=0.5, cancel_to_trade=1.0, level_price=123.45)
    r = apply_kind_rules("breakout", ctx, {})
    assert "post_acceptance_probe" in r.labels
    assert r.labels["post_acceptance_probe"]["level_price"] == 123.45


def test_absorption_requires_two_independent_sources_and_min_taker_rate():
    # only wall/refill, no mp_contra/micro_proxy => veto
    ctx = Ctx(taker_rate_ema=0.30)
    r = apply_kind_rules("absorption", ctx, {"wall_here": True, "mp_contra": False, "micro_proxy": False})
    assert r.veto is True
    assert r.flags.get("veto_no_micro_contra_or_proxy") is True

    # both sides present + taker ok => pass
    ctx2 = Ctx(taker_rate_ema=0.30)
    r2 = apply_kind_rules("absorption", ctx2, {"wall_here": True, "micro_proxy": True})
    assert r2.veto is False

    # low taker rate => veto
    ctx3 = Ctx(taker_rate_ema=0.05)
    r3 = apply_kind_rules("absorption", ctx3, {"wall_here": True, "micro_proxy": True})
    assert r3.veto is True
    assert r3.flags.get("veto_low_taker_rate") is True


def test_extreme_veto_when_quality_is_bad_when_present():
    ctx = Ctx(micro_quality_ok=False, book_quality_ok=True)
    r = apply_kind_rules("extreme", ctx, {})
    assert r.veto is True

    ctx2 = Ctx(micro_quality=0.4, book_quality=0.8)
    r2 = apply_kind_rules("extreme", ctx2, {})
    assert r2.veto is True
    assert "veto_micro_quality" in r2.flags


def test_obi_spike_anti_spoof_veto_when_not_sustained_and_l3_c2t_high():
    ctx = Ctx(obi_sustained=False, l3_cancel_to_trade=4.0)
    r = apply_kind_rules("obi_spike", ctx, {})
    assert r.veto is True
    assert r.flags.get("veto_obi_spoof") is True


def test_spread_scaling_always_in_0_1_and_applied_softly():
    ctx = Ctx(spread_bps=20.0, microprice_shift_bps=0.0, taker_rate_ema=1.0, cancel_to_trade=0.0)
    r = apply_kind_rules("breakout", ctx, {})
    assert r.veto is False
    assert 0.0 <= r.conf_mult01 <= 1.0
    assert r.flags.get("spread_scale") is not None


def test_optional_hypothesis_fuzz_conf_mult_bounds_and_no_crash():
    hyp = pytest.importorskip("hypothesis")
    st = pytest.importorskip("hypothesis.strategies")

    float_any = st.floats(allow_nan=True, allow_infinity=True, width=64)
    bool_any = st.booleans()

    @hyp.given(
        spread=float_any,
        mps=float_any,
        taker=float_any,
        c2t=float_any,
        l3=float_any,
        sustained=bool_any,
    )
    def _prop(spread, mps, taker, c2t, l3, sustained):
        ctx = Ctx(
            spread_bps=spread,
            microprice_shift_bps=mps,
            taker_rate_ema=taker,
            cancel_to_trade=c2t,
            obi_sustained=sustained,
            l3_cancel_to_trade=l3,
        )
        r = apply_kind_rules("breakout", ctx, {})
        assert 0.0 <= r.conf_mult01 <= 1.0
        r2 = apply_kind_rules("obi_spike", ctx, {})
        assert 0.0 <= r2.conf_mult01 <= 1.0

    _prop()
