"""
Тесты для signals/ev_gate.py

EV Gate: Expected Value Gating для оценки профитности сделок.
"""
import os
import pytest
from signals.ev_gate import (
    EvGateConfig,
    EvGateResult,
    evaluate_ev_gate,
    estimate_costs_bps,
    _bps_move,
)


# ─────────────────────── _bps_move ───────────────────────

def test_bps_move_long():
    # entry=1000, tp=1010 → 100 bps
    result = _bps_move(1000.0, 1010.0)
    assert result == pytest.approx(100.0, rel=1e-4)


def test_bps_move_short():
    result = _bps_move(1000.0, 990.0)
    assert result == pytest.approx(100.0, rel=1e-4)


def test_bps_move_zero_entry():
    assert _bps_move(0.0, 100.0) == 0.0


# ─────────────────────── EvGateConfig ───────────────────────

def test_ev_gate_config_from_env_defaults(monkeypatch):
    """Дефолтный конфиг: disabled=False, p_min=0.55, k_cost=1.0."""
    monkeypatch.delenv("EV_GATE_ENABLED", raising=False)
    monkeypatch.delenv("EV_GATE_P_MIN", raising=False)
    cfg = EvGateConfig.from_env()
    assert cfg.enabled is False
    assert cfg.p_min == pytest.approx(0.55)
    assert cfg.k_cost == pytest.approx(1.0)
    assert cfg.default_costs_bps == pytest.approx(8.0)


def test_ev_gate_config_enabled_from_env(monkeypatch):
    monkeypatch.setenv("EV_GATE_ENABLED", "true")
    monkeypatch.setenv("EV_GATE_P_MIN", "0.65")
    cfg = EvGateConfig.from_env()
    assert cfg.enabled is True
    assert cfg.p_min == pytest.approx(0.65)


def test_ev_gate_config_p_min_clamped(monkeypatch):
    monkeypatch.setenv("EV_GATE_P_MIN", "1.5")  # > 1.0 → clamped to 1.0
    cfg = EvGateConfig.from_env()
    assert cfg.p_min == pytest.approx(1.0)


# ─────────────────────── evaluate_ev_gate ───────────────────────

def _make_cfg(enabled=True, p_min=0.55, k_cost=1.0, costs_bps=8.0) -> EvGateConfig:
    return EvGateConfig(
        enabled=enabled,
        p_min=p_min,
        k_cost=k_cost,
        default_costs_bps=costs_bps,
        log_veto=False
    )


def test_ev_gate_passes_high_edge_trade():
    """Высокое p_hit и хороший RR → пропускает."""
    cfg = _make_cfg(p_min=0.55)
    result = evaluate_ev_gate(
        cfg=cfg,
        entry=1000.0,
        tp1=1020.0,  # 200 bps
        sl=990.0,    # 100 bps
        p_hit_tp1=0.70,
        costs_bps=8.0
    )
    assert result.passed is True
    assert result.veto_reason == ""
    # ev = 0.70 * 200 - 0.30 * 100 = 140 - 30 = 110 bps
    assert result.ev_bps == pytest.approx(110.0, rel=1e-3)


def test_ev_gate_rejected_low_p_hit():
    """p_hit < p_min → veto."""
    cfg = _make_cfg(p_min=0.55)
    result = evaluate_ev_gate(
        cfg=cfg,
        entry=1000.0,
        tp1=1020.0,
        sl=990.0,
        p_hit_tp1=0.40,  # < 0.55
        costs_bps=8.0
    )
    assert result.passed is False
    assert "p_hit_tp1" in result.veto_reason


def test_ev_gate_rejected_low_ev():
    """EV слишком маленький по сравнению с costs."""
    cfg = _make_cfg(p_min=0.55, k_cost=2.0)
    result = evaluate_ev_gate(
        cfg=cfg,
        entry=1000.0,
        tp1=1001.0,   # 10 bps
        sl=999.5,     # 5 bps
        p_hit_tp1=0.60,
        costs_bps=10.0  # required = 2.0 * 10 = 20 bps
    )
    # ev = 0.60*10 - 0.40*5 = 6 - 2 = 4 bps < 20 → rejected
    assert result.passed is False
    assert "ev" in result.veto_reason


def test_ev_gate_fields_are_always_populated():
    cfg = _make_cfg(p_min=0.55)
    result = evaluate_ev_gate(
        cfg=cfg,
        entry=2000.0,
        tp1=2040.0,
        sl=1980.0,
        p_hit_tp1=0.60,
        costs_bps=5.0
    )
    # Все поля заполнены
    assert isinstance(result.passed, bool)
    assert result.p_hit_tp1 == pytest.approx(0.60)
    assert result.tp1_bps > 0
    assert result.stop_bps > 0
    assert isinstance(result.ev_bps, float)


def test_ev_gate_p_clamped_to_01():
    """p_hit_tp1 > 1.0 → clamp to 1.0."""
    cfg = _make_cfg(p_min=0.55)
    result = evaluate_ev_gate(
        cfg=cfg,
        entry=1000.0,
        tp1=1010.0,
        sl=990.0,
        p_hit_tp1=2.0,  # invalid
        costs_bps=5.0
    )
    assert result.p_hit_tp1 == pytest.approx(1.0)


# ─────────────────────── estimate_costs_bps ───────────────────────

def test_estimate_costs_bps_from_global_env(monkeypatch):
    monkeypatch.setenv("EV_GATE_COSTS_BPS", "12.0")
    monkeypatch.delenv("BTCUSDT_EV_COSTS_BPS", raising=False)
    import types
    ctx = types.SimpleNamespace()
    result = estimate_costs_bps(ctx, symbol="BTCUSDT")
    assert result == pytest.approx(12.0)


def test_estimate_costs_bps_from_ctx_attribute():
    import types
    ctx = types.SimpleNamespace(total_costs_bps=7.5)
    result = estimate_costs_bps(ctx, symbol="BTCUSDT")
    assert result == pytest.approx(7.5)
