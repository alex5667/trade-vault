"""Phase E regression tests: risk_overlay_v1.

Покрытие:
  - portfolio_heat суммирует только negative-R позиции;
  - correlated_group_notional группирует по symbol → group;
  - consecutive_losses: считает подряд в одном bucket в окне;
  - evaluate_risk_overlay: SHADOW не блокирует, ENFORCE блокирует;
  - все три veto-cases (heat / correlation / consec_loss);
  - disabled → no-op.
"""

from __future__ import annotations

import pytest

from services.risk_overlay_v1 import (
    OpenPositionInfo,
    RecentTradeOutcome,
    RiskLimits,
    compute_portfolio_heat_r,
    consecutive_losses,
    correlated_group_notional,
    correlation_group_for,
    evaluate_risk_overlay,
)


# ───────────────────────────── pure helpers ────────────────────────────────────
def test_correlation_group_for_known_symbols():
    assert correlation_group_for("BTCUSDT") == "BTC_CLUSTER"
    assert correlation_group_for("ETHUSDT") == "ETH_CLUSTER"
    assert correlation_group_for("SOLUSDT") == "ALTS_HIGH_BETA"
    assert correlation_group_for("PEPEUSDT") == "MEME"
    assert correlation_group_for("UNKNOWNUSDT") == "OTHER"


def test_portfolio_heat_only_negative_R():
    positions = [
        OpenPositionInfo("BTCUSDT", 100, unrealized_r=-0.5),
        OpenPositionInfo("ETHUSDT", 100, unrealized_r=0.3),    # winner — не считаем
        OpenPositionInfo("SOLUSDT", 100, unrealized_r=-1.0),
        OpenPositionInfo("PEPEUSDT", 100, unrealized_r=0.0),   # breakeven — не считаем
    ]
    assert compute_portfolio_heat_r(positions) == pytest.approx(1.5)


def test_group_notional_aggregates_alts():
    positions = [
        OpenPositionInfo("SOLUSDT", 200, unrealized_r=0),
        OpenPositionInfo("AVAXUSDT", 150, unrealized_r=0),
        OpenPositionInfo("BTCUSDT", 500, unrealized_r=0),   # другая группа
    ]
    group, total = correlated_group_notional(positions, symbol="DOTUSDT")
    assert group == "ALTS_HIGH_BETA"
    assert total == 350.0


def test_consecutive_losses_stops_at_first_winner():
    """4 убытка подряд, потом winner → счётчик от последней сделки = 0."""
    recent = [
        RecentTradeOutcome("BTCUSDT", "b1", -0.5, 100),
        RecentTradeOutcome("BTCUSDT", "b1", -0.3, 200),
        RecentTradeOutcome("BTCUSDT", "b1", -0.2, 300),
        RecentTradeOutcome("BTCUSDT", "b1", -0.1, 400),
        RecentTradeOutcome("BTCUSDT", "b1", +0.7, 500),  # winner — обрывает
        RecentTradeOutcome("BTCUSDT", "b1", -0.5, 600),  # самая свежая, но одна
    ]
    n = consecutive_losses(recent, bucket="b1", now_ms=1000, lookback_ms=10_000)
    # Самая свежая (600) — loss; предыдущая (500) — winner → прерывает.
    assert n == 1


def test_consecutive_losses_ignores_other_bucket():
    recent = [
        RecentTradeOutcome("BTCUSDT", "b1", -0.5, 100),
        RecentTradeOutcome("BTCUSDT", "b2", -0.5, 200),  # другой bucket
        RecentTradeOutcome("BTCUSDT", "b1", -0.3, 300),
    ]
    n = consecutive_losses(recent, bucket="b1", now_ms=1000, lookback_ms=10_000)
    assert n == 2


def test_consecutive_losses_respects_lookback():
    recent = [
        RecentTradeOutcome("X", "b", -0.5, 100),
        RecentTradeOutcome("X", "b", -0.5, 90_000),  # вне окна (lookback=5000)
    ]
    n = consecutive_losses(recent, bucket="b", now_ms=100_000, lookback_ms=5_000)
    assert n == 0


# ─────────────────────────── evaluate (SHADOW) ──────────────────────────────────
def _limits(**kw):
    base = dict(
        max_portfolio_heat_r=5.0,
        max_correlation_group_usd=1000.0,
        max_consecutive_losses=4,
        cooldown_lookback_ms=10_000,
        enabled=True,
        enforce=False,
    )
    base.update(kw)
    return RiskLimits(**base)


def test_shadow_does_not_veto_even_on_breach():
    positions = [OpenPositionInfo("BTCUSDT", 800, unrealized_r=-3.0)]
    decision = evaluate_risk_overlay(
        symbol="BTCUSDT", bucket="x",
        open_positions=positions,
        new_position_notional_usd=500,  # 800+500=1300 > 1000
        recent_outcomes=[],
        now_ms=1000,
        limits=_limits(),  # enforce=False (default)
    )
    # SHADOW: reason_code заполнен, но veto=False.
    assert decision.veto is False
    assert decision.shadow is True
    assert decision.reason_code is not None
    assert "CORRELATION" in decision.reason_code


def test_enforce_blocks_on_portfolio_heat():
    positions = [
        OpenPositionInfo("BTCUSDT", 100, unrealized_r=-3.0),
        OpenPositionInfo("ETHUSDT", 100, unrealized_r=-2.5),
    ]
    d = evaluate_risk_overlay(
        symbol="SOLUSDT", bucket="b",
        open_positions=positions,
        new_position_notional_usd=10,
        recent_outcomes=[],
        now_ms=1000,
        limits=_limits(enforce=True, max_portfolio_heat_r=5.0),
    )
    assert d.veto is True
    assert d.reason_code is not None
    assert "VETO_PORTFOLIO_HEAT" in d.reason_code
    assert d.portfolio_heat_r == pytest.approx(5.5)


def test_enforce_blocks_on_correlation():
    positions = [OpenPositionInfo("SOLUSDT", 900, unrealized_r=0)]
    d = evaluate_risk_overlay(
        symbol="AVAXUSDT", bucket="b",
        open_positions=positions,
        new_position_notional_usd=200,
        recent_outcomes=[],
        now_ms=1000,
        limits=_limits(enforce=True, max_correlation_group_usd=1000),
    )
    assert d.veto is True
    assert "VETO_CORRELATION" in (d.reason_code or "")
    assert d.correlation_group == "ALTS_HIGH_BETA"


def test_enforce_blocks_on_consec_losses():
    recent = [
        RecentTradeOutcome("X", "b1", -0.5, 1),
        RecentTradeOutcome("X", "b1", -0.3, 2),
        RecentTradeOutcome("X", "b1", -0.2, 3),
        RecentTradeOutcome("X", "b1", -0.1, 4),
    ]
    d = evaluate_risk_overlay(
        symbol="X", bucket="b1",
        open_positions=[],
        new_position_notional_usd=100,
        recent_outcomes=recent,
        now_ms=100,
        limits=_limits(enforce=True, max_consecutive_losses=4),
    )
    assert d.veto is True
    assert "VETO_CONSEC_LOSS" in (d.reason_code or "")


def test_disabled_overlay_passes_through():
    positions = [OpenPositionInfo("BTCUSDT", 10_000, unrealized_r=-100)]
    d = evaluate_risk_overlay(
        symbol="BTCUSDT", bucket="b",
        open_positions=positions,
        new_position_notional_usd=5_000,
        recent_outcomes=[],
        now_ms=1,
        limits=_limits(enforce=True, enabled=False),
    )
    assert d.veto is False
    assert d.reason_code is None
    assert d.details.get("enabled") is False


def test_no_breach_returns_no_reason():
    positions = [OpenPositionInfo("BTCUSDT", 100, unrealized_r=0.1)]
    d = evaluate_risk_overlay(
        symbol="BTCUSDT", bucket="b",
        open_positions=positions,
        new_position_notional_usd=50,
        recent_outcomes=[],
        now_ms=1,
        limits=_limits(enforce=True),
    )
    assert d.veto is False
    assert d.reason_code is None
    assert d.portfolio_heat_r == 0.0


def test_limits_from_env(monkeypatch):
    monkeypatch.setenv("RISK_OVERLAY_MAX_HEAT_R", "10.0")
    monkeypatch.setenv("RISK_OVERLAY_MAX_GROUP_USD", "2000")
    monkeypatch.setenv("RISK_OVERLAY_MAX_CONSEC_LOSSES", "6")
    monkeypatch.setenv("RISK_OVERLAY_ENFORCE", "1")
    monkeypatch.setenv("RISK_OVERLAY_ENABLED", "true")
    lim = RiskLimits.from_env()
    assert lim.max_portfolio_heat_r == 10.0
    assert lim.max_correlation_group_usd == 2000
    assert lim.max_consecutive_losses == 6
    assert lim.enforce is True
    assert lim.enabled is True
