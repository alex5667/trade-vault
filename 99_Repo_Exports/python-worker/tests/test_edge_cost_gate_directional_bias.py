from __future__ import annotations

"""Tests for directional p_min bias in EdgeCostGate._p_min_for_kind.

Counter-trend source of truth = SMT bundle leader (ctx.smt_leader_dir,
ctx.smt_leader_confirm, ctx.smt_state_stale) — same definition as the
SMT coherence gate uses (see pre_publish_gates._evaluate).

Bias is additive (post-calibrator), default master switch OFF.
"""

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


class _StubReader:
    def __init__(self, returns: float) -> None:
        self.returns = returns

    def p_min_for(
        self,
        *,
        symbol: str,
        regime: str,
        kind: str,
        default: float,
        direction: str = "*",
    ) -> float:
        return self.returns


@pytest.fixture
def reader_055(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "core.p_edge_threshold_reader.get_reader",
        lambda: _StubReader(returns=0.55),
    )


@pytest.fixture
def reader_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "core.p_edge_threshold_reader.get_reader",
        lambda: None,
    )


def _gate(
    *,
    enabled: bool = True,
    long_bias: float = 0.0,
    long_ct: float = 0.06,
    short_bias: float = 0.0,
    short_ct: float = 0.0,
    ev_p_min: float = 0.55,
) -> EdgeCostGate:
    return EdgeCostGate(
        enabled=True,
        mode="ev",
        strict_missing_levels=False,
        apply_kinds=set(),
        k_default=4.0,
        k_by_symbol={},
        fees_bps_default=4.0,
        slippage_bps_default=4.0,
        slippage_use_spread_half=True,
        min_expected_move_bps_default=0.0,
        min_expected_move_bps_by_symbol={},
        ev_p_min=ev_p_min,
        ev_p_min_by_kind={},
        directional_bias_enabled=enabled,
        directional_bias_long=long_bias,
        directional_bias_long_countertrend=long_ct,
        directional_bias_short=short_bias,
        directional_bias_short_countertrend=short_ct,
    )


class _Ctx:
    """Minimal stub mimicking pipeline ctx attributes used by the bias logic."""

    def __init__(
        self,
        *,
        leader_dir: str = "",
        leader_confirm: int = 0,
        stale: bool = True,
    ) -> None:
        self.smt_leader_dir = leader_dir
        self.smt_leader_confirm = leader_confirm
        self.smt_state_stale = stale


# ---------------------------------------------------------------------------
# behaviour
# ---------------------------------------------------------------------------


def test_master_switch_disabled_no_bias(reader_055: None) -> None:
    """When directional_bias_enabled=False, no bias regardless of other ENVs/ctx."""
    gate = _gate(enabled=False, long_ct=0.06)
    ctx = _Ctx(leader_dir="DOWN", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx)
    assert val == pytest.approx(0.55)


def test_long_trend_aligned_no_bias(reader_055: None) -> None:
    """LONG signal in line with confirmed UP leader → no bias (long_bias default 0)."""
    gate = _gate(enabled=True, long_ct=0.06)
    ctx = _Ctx(leader_dir="UP", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx)
    assert val == pytest.approx(0.55)


def test_long_countertrend_applies_bias(reader_055: None) -> None:
    """LONG against confirmed DOWN leader → +0.06 → 0.61."""
    gate = _gate(enabled=True, long_ct=0.06)
    ctx = _Ctx(leader_dir="DOWN", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx)
    assert val == pytest.approx(0.61)


def test_short_countertrend_default_no_bias(reader_055: None) -> None:
    """SHORT against UP leader: default short_ct=0 → no bias (asymmetric ship)."""
    gate = _gate(enabled=True, short_ct=0.0)
    ctx = _Ctx(leader_dir="UP", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="short", ctx=ctx)
    assert val == pytest.approx(0.55)


def test_stale_smt_state_no_bias(reader_055: None) -> None:
    """When SMT state is stale, counter-trend detection fails open → no bias."""
    gate = _gate(enabled=True, long_ct=0.06)
    ctx = _Ctx(leader_dir="DOWN", leader_confirm=1, stale=True)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx)
    assert val == pytest.approx(0.55)


def test_leader_unconfirmed_no_bias(reader_055: None) -> None:
    """leader_confirm=0 → leader not trusted → no counter-trend bias."""
    gate = _gate(enabled=True, long_ct=0.06)
    ctx = _Ctx(leader_dir="DOWN", leader_confirm=0, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx)
    assert val == pytest.approx(0.55)


def test_side_na_no_bias(reader_055: None) -> None:
    """side='' (NA) → bias not applied."""
    gate = _gate(enabled=True, long_ct=0.06)
    ctx = _Ctx(leader_dir="DOWN", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="", ctx=ctx)
    assert val == pytest.approx(0.55)


def test_no_ctx_no_bias(reader_055: None) -> None:
    """ctx=None → cannot evaluate counter-trend → no bias."""
    gate = _gate(enabled=True, long_ct=0.06)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=None)
    assert val == pytest.approx(0.55)


def test_bias_clipped_to_tau_ceil(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bias is clipped to TAU_CEIL=0.80 so we never demand p_min>0.80."""
    monkeypatch.setattr(
        "core.p_edge_threshold_reader.get_reader",
        lambda: _StubReader(returns=0.78),
    )
    gate = _gate(enabled=True, long_ct=0.10)  # would push 0.78+0.10=0.88
    ctx = _Ctx(leader_dir="DOWN", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx)
    assert val == pytest.approx(0.80)


def test_short_countertrend_explicit_bias(reader_055: None) -> None:
    """SHORT counter-trend with non-default short_ct=0.04 → 0.55+0.04=0.59."""
    gate = _gate(enabled=True, short_ct=0.04)
    ctx = _Ctx(leader_dir="UP", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="short", ctx=ctx)
    assert val == pytest.approx(0.59)


def test_long_bias_alpha_applies_even_when_aligned(reader_055: None) -> None:
    """Non-zero long_bias should apply for any LONG, trend-aligned or not.

    This documents the API: directional_bias_long is a "tax on all LONGs",
    directional_bias_long_countertrend is the conditional tax. Operators can
    use both — counter-trend penalty stacks via *_countertrend taking
    precedence over the unconditional one.
    """
    gate = _gate(enabled=True, long_bias=0.03, long_ct=0.06)
    # Trend-aligned LONG → unconditional long_bias=0.03 → 0.58
    ctx_aligned = _Ctx(leader_dir="UP", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx_aligned)
    assert val == pytest.approx(0.58)
    # Counter-trend LONG → long_countertrend=0.06 (NOT 0.03+0.06)
    ctx_ct = _Ctx(leader_dir="DOWN", leader_confirm=1, stale=False)
    val_ct = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                                  side="long", ctx=ctx_ct)
    assert val_ct == pytest.approx(0.61)


def test_works_without_reader(reader_disabled: None) -> None:
    """Bias is added to static_floor when reader is unavailable."""
    gate = _gate(enabled=True, long_ct=0.06, ev_p_min=0.55)
    ctx = _Ctx(leader_dir="DOWN", leader_confirm=1, stale=False)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend",
                               side="long", ctx=ctx)
    assert val == pytest.approx(0.61)
